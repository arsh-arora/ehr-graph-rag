#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
import psycopg2
from neo4j import GraphDatabase, basic_auth

# --- CONFIG ---
PG_DSN  = "host=localhost dbname=synthea user=mimic password=strong_password"
NEO_URI = "bolt://localhost:7687"
NEO_AUTH = basic_auth("neo4j", "neo4j_password")


def q(sql, params=None):
    """Yield dict rows from Postgres."""
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params or ())
            cols = [d[0] for d in cur.description] if cur.description else []
            for r in cur.fetchall():
                yield dict(zip(cols, r))


def read_ep_ids(path):
    with open(path) as f:
        eps = [ln.strip() for ln in f if ln.strip()]
    # psycopg2 adapts list -> SQL array for = ANY(%s)
    return eps


def main(ep_file=None):
    eps = None
    if ep_file:
        eps = read_ep_ids(ep_file)
        print(f"[i] Filtering by {len(eps)} ep_id(s) from {ep_file}")

    drv = GraphDatabase.driver(NEO_URI, auth=NEO_AUTH)
    with drv.session() as s:

        # ---- 1) Patients & Episodes ----
        sql_ep = """
            SELECT e.patient, e.ep_id, e.t0, e.t1
            FROM coh.episodes e
        """
        params = ()
        if eps:
            sql_ep += " WHERE e.ep_id = ANY(%s) "
            params = (eps,)
        for r in q(sql_ep, params):
            # MERGE patient
            s.run("MERGE (p:Patient {id:$pid})", pid=r["patient"])
            # MERGE episode (only identifier in MERGE)
            s.run("""
                MERGE (e:Episode {ep_id:$eid})
                SET e.t0 = $t0, e.t1 = $t1
            """, eid=r["ep_id"], t0=r["t0"], t1=r["t1"])
            # MERGE relationship without props (then SET)
            s.run("""
                MATCH (p:Patient {id:$pid}), (e:Episode {ep_id:$eid})
                MERGE (p)-[rel:HAS_EPISODE]->(e)
            """, pid=r["patient"], eid=r["ep_id"])

        # ---- 2) Encounters ----
        # Link encounters to episodes if the encounter's time window overlaps
        # COALESCE(c.stop, c.start) handles records with null stop times.
        sql_enc = """
            SELECT c.id, c.patient, c.start, c.stop, e.ep_id
            FROM coh.encounters c
            JOIN coh.episodes e
              ON e.patient = c.patient
             AND c.start BETWEEN e.t0 AND e.t1
             AND COALESCE(c.stop, c.start) BETWEEN e.t0 AND e.t1
        """
        params = ()
        if eps:
            sql_enc += " WHERE e.ep_id = ANY(%s) "
            params = (eps,)
        for r in q(sql_enc, params):
            # MERGE node by id
            s.run("""
                MERGE (x:Encounter {id:$id})
                SET x.t0 = $t0, x.t1 = $t1
            """, id=r["id"], t0=r["start"], t1=r["stop"])
            # Link to episode
            s.run("""
                MATCH (e:Episode {ep_id:$eid}), (x:Encounter {id:$id})
                MERGE (e)-[rel:HAS_ENCOUNTER]->(x)
                SET rel.start = $t0, rel.end = $t1
            """, eid=r["ep_id"], id=r["id"], t0=r["start"], t1=r["stop"])

        # ---- 3) Medications ----
        # Link medications to episodes if the medication's time window overlaps
        sql_med = """
            SELECT e.ep_id, m.patient, m.start, m.stop,
                   COALESCE(m.description, m.code) AS drug,
                   m.payer
            FROM coh.medications m
            JOIN coh.episodes e
              ON e.patient = m.patient
             AND m.start BETWEEN e.t0 AND e.t1
             AND COALESCE(m.stop, m.start) BETWEEN e.t0 AND e.t1
            WHERE COALESCE(m.description, m.code) IS NOT NULL
        """
        params = ()
        if eps:
            sql_med += " AND e.ep_id = ANY(%s) "
            params = (eps,)
        for r in q(sql_med, params):
            # drug identifier may still be empty after COALESCE (unlikely), but guard anyway
            drug = r["drug"]
            if not drug:
                continue
            s.run("MERGE (m:Medication {drug:$drug})", drug=drug)
            s.run("""
                MATCH (e:Episode {ep_id:$eid}), (m:Medication {drug:$drug})
                MERGE (e)-[rel:RECEIVED]->(m)
                SET rel.start_ts = $start, rel.end_ts = $stop, rel.payer = $payer
            """, eid=r["ep_id"], drug=drug, start=r["start"], stop=r["stop"], payer=r["payer"])

        # ---- 4) Labs (Observations) ----
        # Skip rows with missing label or ts, and avoid nulls in MERGE
        sql_lab = """
            SELECT e.ep_id,
                   o.patient,
                   o.date                 AS ts,
                   COALESCE(o.description, o.code) AS label,
                   o.value, o.units
            FROM coh.observations o
            JOIN coh.episodes e
              ON e.patient = o.patient
             AND o.date BETWEEN e.t0 AND e.t1
            WHERE COALESCE(o.description, o.code) IS NOT NULL
              AND o.date IS NOT NULL
        """
        params = ()
        if eps:
            sql_lab += " AND e.ep_id = ANY(%s) "
            params = (eps,)
        for r in q(sql_lab, params):
            label = r["label"]
            ts    = r["ts"]
            if not label or not ts:
                continue
            # MERGE LabTest by label
            s.run("MERGE (l:LabTest {label:$label})", label=label)
            # MERGE LabResult by (label, ts) only; then SET optional props
            s.run("""
                MERGE (lr:LabResult {label:$label, ts:$ts})
                SET lr.value = $value, lr.unit = $unit
            """, label=label, ts=ts,
                 value=(None if r["value"] is None else str(r["value"])),
                 unit=r["units"])
            # Link LabTest -> LabResult and Episode -> LabTest
            s.run("""
                MATCH (l:LabTest {label:$label}), (lr:LabResult {label:$label, ts:$ts})
                MERGE (l)-[r:RESULT]->(lr)
                SET r.ts = $ts, r.value = $value, r.unit = $unit
            """, label=label, ts=ts,
                 value=(None if r["value"] is None else str(r["value"])),
                 unit=r["units"])
            s.run("""
                MATCH (e:Episode {ep_id:$eid}), (l:LabTest {label:$label})
                MERGE (e)-[:HAS_LAB]->(l)
            """, eid=r["ep_id"], label=label)

        # ---- 5) Procedures ----
        sql_proc = """
            SELECT e.ep_id, p.patient, p.date AS ts, p.code, p.description
            FROM coh.procedures p
            JOIN coh.episodes e
              ON e.patient = p.patient
             AND p.date BETWEEN e.t0 AND e.t1
            WHERE p.code IS NOT NULL
        """
        params = ()
        if eps:
            sql_proc += " AND e.ep_id = ANY(%s) "
            params = (eps,)
        for r in q(sql_proc, params):
            s.run("""
                MERGE (pr:Procedure {code:$code})
                SET pr.name = $name
            """, code=r["code"], name=r["description"])
            s.run("""
                MATCH (e:Episode {ep_id:$eid}), (pr:Procedure {code:$code})
                MERGE (e)-[rel:UNDERWENT]->(pr)
                SET rel.ts = $ts
            """, eid=r["ep_id"], code=r["code"], ts=r["ts"])

    print("KG upsert complete.")


if __name__ == "__main__":
    # Optional: path to a file containing ep_id (one per line)
    main(sys.argv[1] if len(sys.argv) > 1 else None)
