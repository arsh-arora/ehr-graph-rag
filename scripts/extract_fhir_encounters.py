#!/usr/bin/env python3
import os, json, gzip, argparse, re, psycopg2
from psycopg2.extras import execute_batch

def rid(ref):
    if not ref: return None
    m=re.search(r"/([^/]+)$", ref); return m.group(1) if m else None

def iter_bundles(root):
    for r,_,fns in os.walk(root):
        for fn in fns:
            if not fn.lower().endswith((".json",".json.gz")): continue
            p=os.path.join(r,fn)
            op=gzip.open if fn.endswith(".gz") else open
            try:
                with op(p,"rt",encoding="utf-8") as f:
                    b=json.load(f)
                if isinstance(b,dict) and b.get("resourceType")=="Bundle":
                    yield b
            except Exception:
                pass

def upsert(conn, rows):
    with conn.cursor() as cur:
        cur.execute("""
        CREATE TABLE IF NOT EXISTS coh.fhir_encounters(
          patient TEXT,
          enc_id_fhir TEXT,
          start TIMESTAMPTZ NULL,
          stop  TIMESTAMPTZ NULL
        );
        CREATE INDEX IF NOT EXISTS fhir_enc_pt_start ON coh.fhir_encounters(patient, start);
        """)
        execute_batch(cur, """
            INSERT INTO coh.fhir_encounters(patient, enc_id_fhir, start, stop)
            VALUES (%s,%s,%s,%s)
        """, rows, page_size=1000)
    conn.commit()

def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--fhir", default="coherent/fhir")
    ap.add_argument("--dsn", default="host=localhost dbname=synthea user=mimic password=strong_password")
    ap.add_argument("--limit", type=int, default=0)
    args=ap.parse_args()

    conn=psycopg2.connect(args.dsn)
    rows=[]; seen=set(); nfiles=0
    for b in iter_bundles(args.fhir):
        nfiles+=1
        for ent in b.get("entry") or []:
            res=ent.get("resource") or {}
            if res.get("resourceType")!="Encounter": continue
            pid = rid((res.get("subject") or {}).get("reference"))
            enc_id = res.get("id")
            per = res.get("period") or {}
            start = per.get("start"); stop = per.get("end")
            key=(pid,enc_id,start,stop)
            if enc_id and key not in seen:
                seen.add(key)
                rows.append((pid,enc_id,start,stop))
            if len(rows)>=5000:
                upsert(conn, rows); rows=[]
        if args.limit and nfiles>=args.limit: break
    if rows: upsert(conn, rows)
    conn.close()
    print("Done.")

if __name__=="__main__": main()
