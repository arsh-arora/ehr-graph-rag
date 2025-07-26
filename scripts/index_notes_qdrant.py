from qdrant_client import QdrantClient
from qdrant_client.http.models import VectorParams, Distance, PointStruct
from sentence_transformers import SentenceTransformer
import psycopg2, time, torch

COL="notes_chunks"; QURL="http://localhost:6333"
DSN="host=localhost dbname=synthea user=mimic password=strong_password"

ENCODE_BATCH = 64
UPSERT_BATCH = 512
MODEL_NAME   = "intfloat/e5-base-v2"
DEVICE = "cuda" if torch.cuda.is_available() else ("mps" if torch.backends.mps.is_available() else "cpu")

def total_rows():
    with psycopg2.connect(DSN) as c, c.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM coh.episode_notes WHERE text IS NOT NULL")
        return cur.fetchone()[0]

def row_iter(limit=None):
    sql = """
      SELECT note_id, ep_id, patient, encounter, ts, section, text
      FROM coh.episode_notes
      WHERE text IS NOT NULL
      ORDER BY ep_id, ts
    """
    if limit: sql += f" LIMIT {int(limit)}"
    with psycopg2.connect(DSN) as c, c.cursor(name="note_cur") as cur:
        cur.itersize = 5000
        cur.execute(sql)
        for r in cur:
            nid, ep, pat, enc, ts, sec, txt = r
            yield {"id":nid,"ep_id":ep,"patient":pat,"encounter":enc,"ts":str(ts),"section":sec,"text":txt}

def main(limit=None):
    n_total = limit or total_rows()
    print(f"[i] Target ~{n_total} notes | device={DEVICE} | model={MODEL_NAME}")

    # Qdrant client; skip compatibility check
    client = QdrantClient(QURL, timeout=60, check_compatibility=False)

    # (re)create collection with explicit path
    dim = 768 if "base" in MODEL_NAME else (384 if "small" in MODEL_NAME else 768)
    if client.get_collection(COL) is not None:
        client.delete_collection(COL)
    client.create_collection(COL, vectors_config=VectorParams(size=dim, distance=Distance.COSINE))

    model = SentenceTransformer(MODEL_NAME, device=DEVICE)

    t0=time.time()
    buf=[]; done=0; seen_any=False

    def flush():
        nonlocal buf, done, seen_any
        if not buf: return
        texts=[p["text"] for p in buf]
        vecs=model.encode(texts, batch_size=ENCODE_BATCH, normalize_embeddings=True, convert_to_numpy=True)
        points=[PointStruct(id=buf[i]["id"], vector=vecs[i].tolist(), payload=buf[i]) for i in range(len(buf))]
        client.upsert(COL, points=points)
        done += len(points); seen_any=True
        rate = done / max(time.time()-t0, 1e-6)
        print(f"[{done}/{n_total}] {rate:.1f} notes/s")
        buf=[]

    pulled=0
    for rec in row_iter(limit):
        pulled += 1
        buf.append(rec)
        if len(buf) >= UPSERT_BATCH: flush()
    flush()

    if not seen_any:
        print("[!] Pulled 0 rows from coh.episode_notes. Check the view and REFRESH it.")
    else:
        print(f"[done] {done} notes indexed in {time.time()-t0:.1f}s")

if __name__=="__main__":
    import sys
    lim = int(sys.argv[1]) if len(sys.argv)>1 else None
    main(lim)
