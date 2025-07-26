# EHR‑Graph‑RAG (Synthea Coherent)

Graph‑RAG pipeline over synthetic EHR:

* **Postgres**: raw CSV/FHIR → relational + materialized episode windows
* **Note extraction**: FHIR JSON Bundles → `coh.notes` (TIMESTAMPTZ)
* **Crosswalk**: map notes to **episodes** (and best CSV encounter where possible)
* **Qdrant**: dense embedding index of episode‑scoped notes
* **Neo4j**: structured clinical events → knowledge graph

> This repo does **data plumbing & retrieval**. Plug your own LLM on top to generate summaries.

---

## 0) Prerequisites

* macOS/Linux with Docker (or Docker Desktop) & Compose
* Python 3.10+ (pyenv/conda OK)
* `psql` (PostgreSQL client)
* (Optional) Homebrew `cypher-shell` OR run it inside the Neo4j container
* \~100–200 GB free disk if you unzip the full Coherent set

Create a virtualenv and install deps you’ll use from Python:

```bash
pyenv virtualenv 3.10.14 kg-ehr
pyenv local kg-ehr
pip install -U "psycopg2-binary" "neo4j>=5.20,<6" qdrant-client sentence-transformers torch
```

---

## 1) Bring up infra

`docker-compose.yml` launches:

* **postgres:15** (DB: `synthea`, user: `mimic`, password: `strong_password`)
* **neo4j:5.20** (auth: `neo4j/neo4j_password`)
* **qdrant:1.12.3** (vector DB on `:6333`)

```bash
docker compose up -d
docker compose ps
```

---

## 2) Get the data

### 2.1 Synthea Coherent (AWS Open Data)

```bash
# List the bucket
aws s3 ls --no-sign-request s3://synthea-open-data/coherent/

# Download the big zip (~9GB compressed)
aws s3 cp --no-sign-request s3://synthea-open-data/coherent/coherent-11-07-2022.zip .

# Unzip (creates ./coherent with csv/, fhir/, dicom/, dna/)
unzip -q coherent-11-07-2022.zip -d .
```

You should see:

```
coherent/
  csv/
    patients.csv, encounters.csv, observations.csv, medications.csv, ...
  fhir/
    <many> *.json (FHIR Bundles) and/or *.json.gz
```

---

## 3) Load Postgres schema + CSV

### 3.1 Create schema/tables

```bash
export PGPASSWORD=strong_password
psql -h localhost -U mimic -d synthea -f sql/schema.sql
```

### 3.2 Copy CSVs into tables

```bash
psql -h localhost -U mimic -d synthea <<'SQL'
\copy coh.patients          FROM 'coherent/csv/patients.csv'            CSV HEADER
\copy coh.encounters        FROM 'coherent/csv/encounters.csv'          CSV HEADER
\copy coh.conditions        FROM 'coherent/csv/conditions.csv'          CSV HEADER
\copy coh.medications       FROM 'coherent/csv/medications.csv'         CSV HEADER
\copy coh.observations      FROM 'coherent/csv/observations.csv'        CSV HEADER
\copy coh.procedures        FROM 'coherent/csv/procedures.csv'          CSV HEADER
\copy coh.allergies         FROM 'coherent/csv/allergies.csv'           CSV HEADER
\copy coh.careplans         FROM 'coherent/csv/careplans.csv'           CSV HEADER
\copy coh.devices           FROM 'coherent/csv/devices.csv'             CSV HEADER
\copy coh.imaging_studies   FROM 'coherent/csv/imaging_studies.csv'     CSV HEADER
\copy coh.immunizations     FROM 'coherent/csv/immunizations.csv'       CSV HEADER
\copy coh.organizations     FROM 'coherent/csv/organizations.csv'       CSV HEADER
\copy coh.payer_transitions FROM 'coherent/csv/payer_transitions.csv'   CSV HEADER
\copy coh.payers            FROM 'coherent/csv/payers.csv'              CSV HEADER
\copy coh.providers         FROM 'coherent/csv/providers.csv'           CSV HEADER
\copy coh.supplies          FROM 'coherent/csv/supplies.csv'            CSV HEADER
SQL
```

### 3.3 Build episode windows

```bash
# schema.sql created the materialized view; now populate it
psql -h localhost -U mimic -d synthea -c "REFRESH MATERIALIZED VIEW coh.episodes;"
psql -h localhost -U mimic -d synthea -c "SELECT COUNT(*) AS episodes FROM coh.episodes;"
```

You should see a large count (e.g., \~261k).

---

## 4) Extract simple notes from FHIR Bundles

Synthea Coherent’s FHIR is **JSON Bundle(s)** (not NDJSON). Use:

```
scripts/extract_notes_from_fhir_bundle.py
```

It pulls text from:

* `Composition.section[].text.div` (HTML stripped)
* `DiagnosticReport.presentedForm[].attachment.data` (base64 text/\*)
* `DocumentReference.content[].attachment.data` (base64 text/\*)
* any `resource.note[].text`

It writes to `coh.notes(patient, encounter, ts TIMESTAMPTZ, section, text)` and resolves `urn:uuid:` references to IDs.

```bash
python scripts/extract_notes_from_fhir_bundle.py --fhir coherent/fhir
psql -h localhost -U mimic -d synthea -c "SELECT COUNT(*) FROM coh.notes;"
psql -h localhost -U mimic -d synthea -c "SELECT id, patient, encounter, ts, section FROM coh.notes ORDER BY ts NULLS LAST LIMIT 5;"
```

> If `patient/encounter` show up blank initially, re‑run—this script now builds a per‑bundle resolver to map `urn:uuid:*` → real IDs.

---

## 5) Map notes → encounters (optional but useful)

Synthea’s FHIR encounter IDs usually **don’t match** CSV encounter IDs. We create a best‑match crosswalk by **time proximity** and **patient** and backfill `notes.encounter_csv`.

```bash
# Add encounter_csv and build crosswalk from note timestamps
psql -h localhost -U mimic -d synthea <<'SQL'
ALTER TABLE coh.notes ADD COLUMN IF NOT EXISTS encounter_csv TEXT;

DROP TABLE IF EXISTS coh._note_enc_candidates;
CREATE TABLE coh._note_enc_candidates AS
SELECT
  n.id AS note_id, n.patient, n.encounter AS enc_id_fhir, n.ts,
  e.id AS csv_id, e.start::timestamptz AS csv_start, e.stop::timestamptz AS csv_stop,
  ABS(EXTRACT(EPOCH FROM (n.ts - e.start::timestamptz))) AS start_delta_sec
FROM coh.notes n
JOIN coh.encounters e ON e.patient = n.patient
WHERE n.encounter IS NOT NULL
  AND n.ts IS NOT NULL
  AND e.start IS NOT NULL
  AND n.ts BETWEEN (e.start::timestamptz - INTERVAL '24 hours')
               AND (COALESCE(e.stop::timestamptz, e.start::timestamptz) + INTERVAL '24 hours');

DROP TABLE IF EXISTS coh.encounter_xwalk;
CREATE TABLE coh.encounter_xwalk AS
SELECT note_id, patient, enc_id_fhir, csv_id
FROM (
  SELECT c.*,
         ROW_NUMBER() OVER (PARTITION BY c.patient, c.enc_id_fhir ORDER BY c.start_delta_sec ASC) AS rn
  FROM coh._note_enc_candidates c
) r WHERE rn=1;

UPDATE coh.notes n
SET encounter_csv = x.csv_id
FROM coh.encounter_xwalk x
WHERE n.id = x.note_id
  AND (n.encounter_csv IS NULL OR n.encounter_csv <> x.csv_id);
SQL

# sanity
psql -h localhost -U mimic -d synthea -c "
SELECT COUNT(*) AS note_ep_matches
FROM coh.notes n JOIN coh.episodes ep
  ON ep.patient=n.patient AND n.ts BETWEEN ep.t0 AND ep.t1;
"
psql -h localhost -U mimic -d synthea -c "
SELECT COUNT(*) AS note_enc_matches_csv
FROM coh.notes n JOIN coh.encounters e ON e.id=n.encounter_csv;
"
```

---

## 6) Episode‑scoped notes materialized view

We pre‑join each note to its episode window for retrieval:

```bash
psql -h localhost -U mimic -d synthea <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS coh.episode_notes;
CREATE MATERIALIZED VIEW coh.episode_notes AS
SELECT ep.ep_id, n.id AS note_id,
       n.patient, n.encounter_csv AS encounter, n.ts, n.section, n.text
FROM coh.notes n
JOIN coh.episodes ep
  ON ep.patient = n.patient
 AND n.ts BETWEEN ep.t0 AND ep.t1;
CREATE INDEX IF NOT EXISTS epn_idx ON coh.episode_notes(ep_id, ts);
SQL

psql -h localhost -U mimic -d synthea -c "SELECT COUNT(*) FROM coh.episode_notes WHERE text IS NOT NULL;"
```

---

## 7) Dev‑slice for fast iteration (recommended)

Work on \~100 episodes first.

```bash
# create a slice
psql -h localhost -U mimic -d synthea -Atc "
  SELECT ep_id FROM (SELECT DISTINCT ep_id FROM coh.episode_notes) s
  ORDER BY random() LIMIT 100
" > episodes_dev.txt

# load slice table + dev view
psql -h localhost -U mimic -d synthea <<'SQL'
DROP TABLE IF EXISTS coh.dev_eps;
CREATE TABLE coh.dev_eps(ep_id TEXT PRIMARY KEY);
SQL
psql -h localhost -U mimic -d synthea -c "\copy coh.dev_eps FROM 'episodes_dev.txt'"

psql -h localhost -U mimic -d synthea <<'SQL'
DROP MATERIALIZED VIEW IF EXISTS coh.episode_notes_dev;
CREATE MATERIALIZED VIEW coh.episode_notes_dev AS
SELECT en.* FROM coh.episode_notes en JOIN coh.dev_eps d USING(ep_id);
CREATE INDEX IF NOT EXISTS epn_dev_idx ON coh.episode_notes_dev(ep_id, ts);
SQL
```

---

## 8) Index notes in Qdrant

### 8.1 Dev collection (fast)

```
scripts/index_notes_qdrant_dev.py
```

* reads from `coh.episode_notes_dev`
* writes to `notes_chunks_dev`
* auto‑detects device (`cuda` > `mps` > `cpu`)
* recreates collection unless `--append`

```bash
python scripts/index_notes_qdrant_dev.py
# Speed up (smaller model):
python scripts/index_notes_qdrant_dev.py --model intfloat/e5-small-v2 --encode-batch 96 --upsert-batch 1024
```

### 8.2 Full collection (overnight)

```
scripts/index_notes_qdrant.py
```

* reads `coh.episode_notes`
* writes to `notes_chunks`

```bash
python scripts/index_notes_qdrant.py          # full run
# or dry‑run on N
python scripts/index_notes_qdrant.py 5000
```

> If you see a client/server warning (client 1.15 vs server 1.12), we use `check_compatibility=False`. To align versions, either upgrade the container to `qdrant/qdrant:1.15.0` or `pip install "qdrant-client==1.12.0"`.

---

## 9) Build the Knowledge Graph in Neo4j

### 9.1 Create constraints/indexes

Either install `cypher-shell` locally or run it in the container:

```bash
# inside container (recommended)
docker compose exec neo4j bash -lc 'cat > /tmp/schema.cypher << "CYPHER"
CREATE CONSTRAINT patient_pk  IF NOT EXISTS FOR (p:Patient)   REQUIRE p.id IS UNIQUE;
CREATE CONSTRAINT episode_pk  IF NOT EXISTS FOR (e:Episode)   REQUIRE e.ep_id IS UNIQUE;
CREATE CONSTRAINT enc_pk      IF NOT EXISTS FOR (c:Encounter) REQUIRE c.id IS UNIQUE;
CREATE INDEX med_idx  IF NOT EXISTS FOR (m:Medication) ON (m.drug);
CREATE INDEX lab_idx  IF NOT EXISTS FOR (l:LabTest)    ON (l.label);
CREATE INDEX proc_idx IF NOT EXISTS FOR (p:Procedure)  ON (p.code);
CYPHER
cypher-shell -u neo4j -p neo4j_password -a bolt://localhost:7687 -f /tmp/schema.cypher'
```

### 9.2 Upsert structured facts

```
scripts/kg_upsert_structured.py
```

* **safe MERGE** patterns (no NULLs in MERGE maps)
* filters by an `ep_id` file if provided

```bash
# dev slice
python scripts/kg_upsert_structured.py episodes_dev.txt

# or full graph
python scripts/kg_upsert_structured.py
```

Sanity:

```bash
cypher-shell -u neo4j -p neo4j_password -a bolt://localhost:7687 "MATCH (e:Episode) RETURN count(e);"
cypher-shell -u neo4j -p neo4j_password -a bolt://localhost:7687 "MATCH (e:Episode)-[r]->() RETURN type(r), count(*) ORDER BY count(*) DESC LIMIT 10;"
```

---

## 10) (Optional) Retrieval smoke tests

**Qdrant (dev episode):**

```bash
EP=$(psql -h localhost -U mimic -d synthea -Atc "SELECT ep_id FROM coh.dev_eps LIMIT 1")
python - <<PY
from qdrant_client import QdrantClient
from sentence_transformers import SentenceTransformer
client=QdrantClient("http://localhost:6333", check_compatibility=False)
model=SentenceTransformer("intfloat/e5-base-v2")
qv=model.encode("[query] "+"Hospital course", normalize_embeddings=True)
hits=client.search(collection_name="notes_chunks_dev", query_vector=qv, limit=50)
hits=[h for h in hits if h.payload.get("ep_id")=="$EP"]
print("hits:", len(hits))
for h in hits[:5]:
    p=h.payload
    print(p["ts"], p["section"], p["text"][:120].replace("\n"," ")+"...")
PY
```

**Neo4j subgraph (dev episode):** open Neo4j Browser →

```
MATCH (e:Episode {ep_id: "<paste EP>"})-[:HAS_ENCOUNTER|HAS_LAB|RECEIVED|UNDERWENT]->(x)
RETURN e,x LIMIT 50;
```

At this point your **Graph‑RAG evidence layer is live**:

* dense retriever (`notes_chunks[_dev]`)
* KG subgraphs in Neo4j

Wire any LLM you like on top of it.

---

## 11) (Optional) LLM hook (one example)

Create `scripts/llm.py` with your provider (OpenAI, Claude, local/Ollama). Example OpenAI:

```python
# scripts/llm.py
import os, time, requests
MODEL=os.getenv("LLM_MODEL","gpt-4o-mini")
KEY=os.environ["OPENAI_API_KEY"]
def generate(prompt, max_tokens=700, temperature=0.2):
    t=time.time()
    r=requests.post("https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {KEY}"},
        json={"model": MODEL, "messages":[
            {"role":"system","content":"You are a careful clinical summarizer."},
            {"role":"user","content":prompt}],
            "temperature":temperature,"max_tokens":max_tokens}, timeout=60)
    r.raise_for_status()
    out=r.json()
    return out["choices"][0]["message"]["content"], {"latency_s": round(time.time()-t,3)}
```

Then wrap your retrieval + KG triples into a prompt and call `generate(...)`.

---

## Repo layout

```
scripts/
  extract_fhir_encounters.py           # optional: FHIR Encounter table (urn:uuid → id, timestamps)
  extract_notes_from_fhir_bundle.py    # FHIR Bundle → coh.notes (TIMESTAMPTZ)
  extract_notes_from_fhir.py           # (legacy NDJSON variant; not used for Coherent JSON)
  index_notes_qdrant_dev.py            # index coh.episode_notes_dev → Qdrant (notes_chunks_dev)
  index_notes_qdrant.py                # index coh.episode_notes → Qdrant (notes_chunks)
  kg_upsert_structured.py              # Postgres structured → Neo4j graph
sql/
  schema.sql                           # tables, indexes, episodes MV
docker-compose.yml                     # postgres, neo4j, qdrant
```

---

## Performance notes

* **Embedding is the bottleneck.** On Apple M‑series with MPS:

  * `e5-base-v2`: \~10–70 notes/s depending on batch → 293k notes ≈ 1–8 h
  * `e5-small-v2`: 2–3× faster with a mild quality hit
* Tune `--encode-batch` and `--upsert-batch` in the indexers; monitor VRAM/RAM.
* Qdrant upserts are fast locally; compatibility warnings are safe if you set `check_compatibility=False`.

---

## Troubleshooting

* **`SELECT 0` after creating episodes** → You refreshed before loading CSVs. Load CSVs then:

  ```
  REFRESH MATERIALIZED VIEW coh.episodes;
  ```
* **`coh.notes` doesn’t exist** → The extractor didn’t flush. Run:

  ```
  CREATE TABLE coh.notes(...);
  ```

  then re‑run `extract_notes_from_fhir_bundle.py`.
* **Timestamps cast error** → We store `ts` as `TIMESTAMPTZ`; script casts with `::timestamptz`.
* **Empty patient/encounter** → The extractor now resolves `urn:uuid:`. If some still empty, those resources truly lack links; that’s fine for episode‑level retrieval.
* **`ORDER BY random()` with DISTINCT** → Wrap DISTINCT in a subquery (see §7).
* **`cypher-shell: command not found`** → Use `docker compose exec neo4j cypher-shell ...`.
* **Neo4j `MERGE` with null** → Never put nullable fields in a MERGE map. This repo’s script already uses `MERGE` on IDs only and `SET` for optionals.

---

## Scaling to full dataset

1. Let `scripts/index_notes_qdrant.py` run overnight (→ `notes_chunks`).
2. `python scripts/kg_upsert_structured.py` (no ep list) for full Neo4j graph.
3. Point your retrieval to `notes_chunks` and batch your generation jobs.

---

## Security & provenance (strongly recommended for papers)

* Keep your pipeline deterministic: low temp (≤0.2), fixed evidence caps (≤150 triples, ≤12 chunks).
* Post‑validate numbers/dates in outputs against the KG and redact mismatches.
* Log provenance: include medication names, lab labels, and procedure codes used by the model.

---

That’s it. You now have a reproducible Graph‑RAG backend on Synthea Coherent: **structured KG + dense retriever** ready for summarization or QA. If you want me to wire a specific LLM and add a tiny evaluation script (dose/date + labs accuracy over N episodes), say the word and I’ll drop it in.
# ehr-graph-rag
# ehr-graph-rag
# ehr-graph-rag
