import json, gzip, os, re, html, base64
import psycopg2
from psycopg2.extras import execute_batch

FHIR_DIR = "coherent/fhir"  # adjust if needed
PG_DSN   = "host=localhost dbname=synthea user=mimic password=strong_password"

def parse_ndjson(path):
    with (gzip.open if path.endswith(".gz") else open)(path, "rt", encoding="utf-8") as f:
        for line in f:
            line=line.strip()
            if line: yield json.loads(line)

def rid(ref):  # "Encounter/123" -> "123"
    if not ref: return None
    m = re.search(r"/([^/]+)$", ref); return m.group(1) if m else None

def from_composition(o):
    out=[]; p=rid(o.get("subject",{}).get("reference")); e=rid(o.get("encounter",{}).get("reference")); ts=o.get("date")
    for sec in o.get("section",[]) or []:
        title=sec.get("title") or "section"
        div=sec.get("text",{}).get("div","")
        txt=re.sub("<[^<]+?>"," ",div).strip()
        if txt: out.append((p,e,ts,title,html.unescape(txt)))
    return out

def from_documentref(o):
    out=[]; p=rid(o.get("subject",{}).get("reference"))
    encs=o.get("context",{}).get("encounter",[])
    e=rid(encs[0].get("reference")) if encs else None
    ts=o.get("date") or o.get("created")
    for c in o.get("content",[]):
        a=c.get("attachment",{})
        if a.get("contentType","").startswith("text/") and a.get("data"):
            try:
                txt=base64.b64decode(a["data"]).decode("utf-8","ignore").strip()
                if txt: out.append((p,e,ts,"DocumentReference",txt))
            except Exception: pass
    return out

def from_generic_with_notes(o, rtype):
    out=[]; p=rid((o.get("subject") or {}).get("reference")) or rid((o.get("patient") or {}).get("reference"))
    e=rid((o.get("encounter") or {}).get("reference"))
    ts= o.get("effectiveDateTime") or o.get("issued") or o.get("performedDateTime") or o.get("authoredOn")
    for n in o.get("note",[]) or []:
        txt=n.get("text"); 
        if txt: out.append((p,e,ts,rtype,txt.strip()))
    return out

def flush(batch):
    conn=psycopg2.connect(PG_DSN); cur=conn.cursor()
    cur.execute("""
      CREATE TABLE IF NOT EXISTS coh.notes(
        id BIGSERIAL PRIMARY KEY,
        patient TEXT, encounter TEXT, ts TIMESTAMP NULL,
        section TEXT, text TEXT
      );
      CREATE INDEX IF NOT EXISTS notes_idx ON coh.notes(patient, ts);
    """)
    execute_batch(cur, """
      INSERT INTO coh.notes(patient, encounter, ts, section, text)
      VALUES (%s,%s,NULLIF(%s,''),%s,%s)
    """, batch, page_size=1000)
    conn.commit(); cur.close(); conn.close()

def main():
    batch=[]
    for fname in os.listdir(FHIR_DIR):
        if not fname.lower().endswith((".ndjson",".ndjson.gz")): continue
        rtype=fname.split(".")[0]
        for obj in parse_ndjson(os.path.join(FHIR_DIR,fname)):
            typ=obj.get("resourceType") or rtype
            if   typ=="Composition":       batch += from_composition(obj)
            elif typ=="DocumentReference": batch += from_documentref(obj)
            else:                          batch += from_generic_with_notes(obj, typ)
            if len(batch)>=5000: flush(batch); batch=[]
    if batch: flush(batch)

if __name__=="__main__": main()
