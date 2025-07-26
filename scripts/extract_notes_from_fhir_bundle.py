#!/usr/bin/env python3
import os, re, json, gzip, html, base64, argparse
import psycopg2
from psycopg2.extras import execute_batch

# ---------- DB helpers ----------
def ensure_table(conn):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE SCHEMA IF NOT EXISTS coh;
        CREATE TABLE IF NOT EXISTS coh.notes(
          id BIGSERIAL PRIMARY KEY,
          patient   TEXT,
          encounter TEXT,
          ts        TIMESTAMPTZ NULL,
          section   TEXT,
          text      TEXT
        );
        CREATE INDEX IF NOT EXISTS notes_idx ON coh.notes(patient, ts);
        """)
    conn.commit()

def flush(conn, batch):
    if not batch: return
    with conn.cursor() as cur:
        execute_batch(cur, """
            INSERT INTO coh.notes(patient, encounter, ts, section, text)
            VALUES (%s,%s, NULLIF(%s,'')::timestamptz, %s, %s)
        """, batch, page_size=1000)
    conn.commit()

# ---------- reference resolution ----------
def build_resolver(bundle):
    """
    Build a resolver that maps:
      - fullUrl (e.g., 'urn:uuid:abc') -> (resourceType, id)
      - logical 'ResourceType/id'      -> (resourceType, id)
    """
    by_full = {}
    for ent in bundle.get("entry", []) or []:
        full = ent.get("fullUrl")
        res  = ent.get("resource") or {}
        rtype = res.get("resourceType")
        rid   = res.get("id")
        if full and rtype and rid:
            by_full[full] = (rtype, rid)
        if rtype and rid:
            by_full[f"{rtype}/{rid}"] = (rtype, rid)
    def resolve(ref: str):
        if not ref: return (None, None)
        # Direct hit
        if ref in by_full: return by_full[ref]
        # urn:uuid:<guid>
        if ref.startswith("urn:uuid:"):
            return by_full.get(ref, (None, ref.split(":")[-1]))
        # ResourceType/id
        if "/" in ref:
            rtype, rid = ref.split("/", 1)
            return (rtype, rid)
        # local fragment '#id' (contained) -> leave unresolved
        if ref.startswith("#"):
            return (None, ref[1:])
        return (None, ref)
    return resolve

# ---------- field helpers ----------
def strip_html(x: str) -> str:
    return re.sub(r"<[^>]+>", " ", x or "").strip()

def first_nonempty(*vals):
    for v in vals:
        if v: return v
    return None

# ---------- resource extractors ----------
def from_Composition(res, resolve):
    out=[]
    subj_t, subj_id = resolve((res.get("subject") or {}).get("reference") or "")
    enc_t,  enc_id  = resolve((res.get("encounter") or {}).get("reference") or "")
    ts = res.get("date")
    for sec in (res.get("section") or []):
        title = sec.get("title") or "section"
        div = ((sec.get("text") or {}).get("div")) or ""
        txt = strip_html(div)
        if txt:
            out.append((subj_id, enc_id, ts, f"Composition:{title}", html.unescape(txt)))
    return out

def from_DiagnosticReport(res, resolve):
    out=[]
    subj_t, subj_id = resolve((res.get("subject") or {}).get("reference") or "")
    enc_t,  enc_id  = resolve((res.get("encounter") or {}).get("reference") or "")
    ts = first_nonempty(res.get("effectiveDateTime"), res.get("issued"), res.get("date"))
    # presentedForm (base64 text/*)
    for pf in (res.get("presentedForm") or []):
        data = pf.get("data"); ctype = (pf.get("contentType") or "")
        if data and ctype.startswith("text/"):
            try:
                txt = base64.b64decode(data).decode("utf-8","ignore").strip()
                if txt: out.append((subj_id, enc_id, ts, "DiagnosticReport", txt))
            except Exception: pass
    # generic note[]
    for note in (res.get("note") or []):
        txt = (note or {}).get("text")
        if txt: out.append((subj_id, enc_id, ts, "DiagnosticReport:note", txt.strip()))
    return out

def from_DocumentReference(res, resolve):
    out=[]
    subj_t, subj_id = resolve((res.get("subject") or {}).get("reference") or "")
    encs = ((res.get("context") or {}).get("encounter")) or []
    enc_ref = (encs[0] or {}).get("reference") if encs else None
    _, enc_id = resolve(enc_ref or "")
    ts = first_nonempty(res.get("date"), res.get("created"))
    for c in (res.get("content") or []):
        att = (c.get("attachment") or {})
        ctype = att.get("contentType") or ""
        data  = att.get("data")
        if data and ctype.startswith("text/"):
            try:
                txt = base64.b64decode(data).decode("utf-8","ignore").strip()
                if txt: out.append((subj_id, enc_id, ts, "DocumentReference", txt))
            except Exception: pass
    return out

def from_generic_with_notes(res, resolve):
    out=[]
    subj_t, subj_id = resolve(
        ((res.get("subject") or {}).get("reference")) or
        ((res.get("patient") or {}).get("reference")) or ""
    )
    enc_t, enc_id = resolve(((res.get("encounter") or {}).get("reference")) or "")
    ts = first_nonempty(res.get("effectiveDateTime"), res.get("issued"),
                        res.get("performedDateTime"), res.get("authoredOn"))
    for n in (res.get("note") or []):
        txt = (n or {}).get("text")
        if txt: out.append((subj_id, enc_id, ts, f"{res.get('resourceType','Resource')}:note", txt.strip()))
    return out

# ---------- bundle walker ----------
def process_bundle(path):
    open_fn = gzip.open if path.endswith(".gz") else open
    with open_fn(path, "rt", encoding="utf-8") as f:
        bundle = json.load(f)
    if not (isinstance(bundle, dict) and bundle.get("resourceType")=="Bundle"):
        return []

    resolve = build_resolver(bundle)
    out=[]
    for ent in bundle.get("entry") or []:
        res = ent.get("resource") or {}
        rt = res.get("resourceType")
        if   rt=="Composition":        out += from_Composition(res, resolve)
        elif rt=="DiagnosticReport":   out += from_DiagnosticReport(res, resolve)
        elif rt=="DocumentReference":  out += from_DocumentReference(res, resolve)
        else:                          out += from_generic_with_notes(res, resolve)
    return out

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--fhir", default="coherent/fhir", help="Directory with FHIR *.json / *.json.gz Bundles")
    ap.add_argument("--dsn",  default="host=localhost dbname=synthea user=mimic password=strong_password")
    ap.add_argument("--limit", type=int, default=0)
    args = ap.parse_args()

    files=[]
    for root, _, fns in os.walk(args.fhir):
        for fn in fns:
            if fn.lower().endswith((".json", ".json.gz")):
                files.append(os.path.join(root, fn))
    files.sort()
    if args.limit: files = files[:args.limit]
    if not files:
        raise SystemExit(f"No .json/.json.gz files found under {args.fhir}")

    conn = psycopg2.connect(args.dsn)
    ensure_table(conn)

    total=0
    for i, path in enumerate(files, 1):
        rows = process_bundle(path)
        flush(conn, rows)
        total += len(rows)
        if i % 50 == 0:
            print(f"... processed {i} files, inserted ~{total} notes")
    print(f"Done. Files: {len(files)}; notes inserted: ~{total}")
    conn.close()

if __name__ == "__main__":
    main()
