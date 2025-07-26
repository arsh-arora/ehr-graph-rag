#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Index episode-scoped notes into a DEV Qdrant collection.

Defaults:
- Source view: coh.episode_notes_dev (falls back to coh.episode_notes)
- Collection:  notes_chunks_dev
- Model:       intfloat/e5-base-v2
- Device:      cuda > mps > cpu (auto-detect)
"""

import os, time, math, argparse
import psycopg2
from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer

try:
    import torch
    HAS_TORCH = True
except Exception:
    HAS_TORCH = False

# ---------- defaults ----------
DEFAULT_DSN = os.environ.get(
    "PG_DSN",
    "host=localhost dbname=synthea user=mimic password=strong_password"
)
DEFAULT_QURL = os.environ.get("QDRANT_URL", "http://localhost:6333")

# ---------- helpers ----------
def pick_device():
    if HAS_TORCH:
        if torch.cuda.is_available():
            return "cuda"
        if getattr(torch.backends, "mps", None) and torch.backends.mps.is_available():
            return "mps"
    return "cpu"

def total_rows(dsn, source_view, ep_file=None):
    q = f"SELECT COUNT(*) FROM {source_view} WHERE text IS NOT NULL"
    if ep_file:
        q = f"""
        SELECT COUNT(*) FROM {source_view}
        WHERE text IS NOT NULL AND ep_id = ANY(%s)
        """
    with psycopg2.connect(dsn) as c, c.cursor() as cur:
        if ep_file:
            eps = [l.strip() for l in open(ep_file) if l.strip()]
            cur.execute(q, (eps,))
        else:
            cur.execute(q)
        return cur.fetchone()[0]

def row_iter(dsn, source_view, limit=None, ep_file=None):
    base_sql = f"""
      SELECT note_id, ep_id, patient, encounter, ts, section, text
      FROM {source_view}
      WHERE text IS NOT NULL
    """
    params = None
    if ep_file:
        eps = [l.strip() for l in open(ep_file) if l.strip()]
        base_sql += " AND ep_id = ANY(%s) "
        params = (eps,)
    base_sql += " ORDER BY ep_id, ts "
    if limit:
        base_sql += f" LIMIT {int(limit)} "

    with psycopg2.connect(dsn) as c, c.cursor(name="dev_note_cur") as cur:
        cur.itersize = 5000
        cur.execute(base_sql, params)
        for r in cur:
            nid, ep, pat, enc, ts, sec, txt = r
            yield {
                "id": nid,
                "ep_id": ep,
                "patient": pat,
                "encounter": enc,
                "ts": str(ts),
                "section": sec,
                "text": txt,
            }

def ensure_collection(client: QdrantClient, name: str, dim: int, recreate: bool):
    # Avoid deprecated get_collection kwargs; use collection_exists
    if recreate and client.collection_exists(name):
        client.delete_collection(name)
    if not client.collection_exists(name):
        client.create_collection(
            collection_name=name,
            vectors_config=VectorParams(size=dim, distance=Distance.COSINE),
        )

# ---------- main ----------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default=DEFAULT_DSN, help="Postgres DSN")
    ap.add_argument("--qdrant-url", default=DEFAULT_QURL, help="Qdrant URL")
    ap.add_argument("--collection", default="notes_chunks_dev", help="Qdrant collection")
    ap.add_argument("--source-view", default="coh.episode_notes_dev",
                   help="Source table/view. Falls back to coh.episode_notes if missing.")
    ap.add_argument("--episodes-file", default=None,
                   help="Optional file with ep_id (one per line) to filter rows")
    ap.add_argument("--model", default="intfloat/e5-base-v2",
                   help="SentenceTransformer model (e.g., intfloat/e5-small-v2 for speed)")
    ap.add_argument("--encode-batch", type=int, default=64, help="Encode batch size")
    ap.add_argument("--upsert-batch", type=int, default=512, help="Qdrant upsert batch size")
    ap.add_argument("--limit", type=int, default=None, help="Limit number of rows")
    ap.add_argument("--append", action="store_true",
                   help="Append to existing collection (do not delete/recreate)")
    args = ap.parse_args()

    device = pick_device()
    dim = 768
    if "small" in args.model:
        dim = 384

    # Detect source view existence; fallback if needed
    source_view = args.source_view
    with psycopg2.connect(args.dsn) as c, c.cursor() as cur:
        cur.execute("""
            SELECT to_regclass(%s) IS NOT NULL
        """, (args.source_view,))
        exists = cur.fetchone()[0]
    if not exists:
        source_view = "coh.episode_notes"
        print(f"[i] Source view {args.source_view} not found; using {source_view}")

    n_total = total_rows(args.dsn, source_view, args.episodes_file)
    if args.limit:
        n_total = min(n_total, args.limit)

    print(f"[i] Source={source_view} rows={n_total} | device={device} | model={args.model} "
          f"| encode_batch={args.encode_batch} | upsert_batch={args.upsert_batch} "
          f"| collection={args.collection}")

    # Qdrant client
    client = QdrantClient(args.qdrant_url, check_compatibility=False, timeout=60)
    ensure_collection(client, args.collection, dim, recreate=(not args.append))

    # Encoder
    model = SentenceTransformer(args.model, device=device)

    t0 = time.time()
    buf = []
    done = 0

    def flush():
        nonlocal buf, done
        if not buf:
            return
        texts = [b["text"] for b in buf]
        vecs = model.encode(
            texts,
            batch_size=args.encode_batch,
            normalize_embeddings=True,
            convert_to_numpy=True,
        )
        points = [
            PointStruct(id=buf[i]["id"], vector=vecs[i].tolist(), payload=buf[i])
            for i in range(len(buf))
        ]
        client.upsert(args.collection, points=points)
        done += len(points)
        elapsed = time.time() - t0
        rate = done / max(elapsed, 1e-6)
        remaining = max(n_total - done, 0)
        eta_min = (remaining / rate) / 60 if rate > 0 else float("inf")
        print(f"[{done}/{n_total}] {rate:.1f} notes/s  ETA ~{eta_min:.1f} min")
        buf.clear()

    pulled = 0
    try:
        for rec in row_iter(args.dsn, source_view, args.limit, args.episodes_file):
            pulled += 1
            buf.append(rec)
            if len(buf) >= args.upsert_batch:
                flush()
        flush()
    except KeyboardInterrupt:
        print("\n[!] Interrupted — flushing remaining batch …")
        flush()

    total_s = time.time() - t0
    print(f"[done] upserted {done}/{n_total} notes in {total_s/60:.1f} min")

if __name__ == "__main__":
    main()
