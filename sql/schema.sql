BEGIN;

-- Clean slate
DROP SCHEMA IF EXISTS coh CASCADE;
CREATE SCHEMA coh;

-- =========================
-- Core tables (exact CSV order)
-- =========================

-- patients.csv: Id, BIRTHDATE, DEATHDATE, SSN, DRIVERS, PASSPORT, PREFIX, FIRST, LAST, ...
CREATE TABLE coh.patients (
  id                   TEXT PRIMARY KEY,
  birthdate            TIMESTAMP NULL,
  deathdate            TIMESTAMP NULL,
  ssn                  TEXT,
  drivers              TEXT,
  passport             TEXT,
  prefix               TEXT,
  first                TEXT,
  last                 TEXT,
  suffix               TEXT,
  maiden               TEXT,
  marital              TEXT,
  race                 TEXT,
  ethnicity            TEXT,
  gender               TEXT,
  birthplace           TEXT,
  address              TEXT,
  city                 TEXT,
  state                TEXT,
  county               TEXT,
  zip                  TEXT,
  lat                  DOUBLE PRECISION,
  lon                  DOUBLE PRECISION,
  healthcare_expenses  NUMERIC,
  healthcare_coverage  NUMERIC
);

-- encounters.csv
-- Id,START,STOP,PATIENT,ORGANIZATION,PROVIDER,PAYER,ENCOUNTERCLASS,CODE,DESCRIPTION,
-- BASE_ENCOUNTER_COST,TOTAL_CLAIM_COST,PAYER_COVERAGE,REASONCODE,REASONDESCRIPTION
CREATE TABLE coh.encounters (
  id                   TEXT PRIMARY KEY,
  start                TIMESTAMP,
  stop                 TIMESTAMP,
  patient              TEXT,
  organization         TEXT,
  provider             TEXT,
  payer                TEXT,
  encounterclass       TEXT,
  code                 TEXT,
  description          TEXT,
  base_encounter_cost  NUMERIC,
  total_claim_cost     NUMERIC,
  payer_coverage       NUMERIC,
  reasoncode           TEXT,
  reasondescription    TEXT
);

-- conditions.csv: START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION
CREATE TABLE coh.conditions (
  start          TIMESTAMP,
  stop           TIMESTAMP,
  patient        TEXT,
  encounter      TEXT,
  code           TEXT,
  description    TEXT
);

-- medications.csv
-- START,STOP,PATIENT,PAYER,ENCOUNTER,CODE,DESCRIPTION,BASE_COST,PAYER_COVERAGE,DISPENSES,TOTALCOST,REASONCODE,REASONDESCRIPTION
CREATE TABLE coh.medications (
  start             TIMESTAMP,
  stop              TIMESTAMP,
  patient           TEXT,
  payer             TEXT,
  encounter         TEXT,
  code              TEXT,
  description       TEXT,
  base_cost         NUMERIC,
  payer_coverage    NUMERIC,
  dispenses         INT,
  totalcost         NUMERIC,
  reasoncode        TEXT,
  reasondescription TEXT
);

-- observations.csv: DATE,PATIENT,ENCOUNTER,CODE,DESCRIPTION,VALUE,UNITS,TYPE
CREATE TABLE coh.observations (
  date         TIMESTAMP,
  patient      TEXT,
  encounter    TEXT,
  code         TEXT,
  description  TEXT,
  value        TEXT,
  units        TEXT,
  type         TEXT
);

-- procedures.csv: DATE, PATIENT, ENCOUNTER, CODE, DESCRIPTION, BASE_COST, REASONCODE, REASONDESCRIPTION
CREATE TABLE coh.procedures (
  date              TIMESTAMP,
  patient           TEXT,
  encounter         TEXT,
  code              TEXT,
  description       TEXT,
  base_cost         NUMERIC,
  reasoncode        TEXT,
  reasondescription TEXT
);

-- allergies.csv: START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION
CREATE TABLE coh.allergies (
  start        TIMESTAMP,
  stop         TIMESTAMP,
  patient      TEXT,
  encounter    TEXT,
  code         TEXT,
  description  TEXT
);

-- careplans.csv: Id, START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, REASONCODE, REASONDESCRIPTION
CREATE TABLE coh.careplans (
  id                TEXT PRIMARY KEY,
  start             TIMESTAMP,
  stop              TIMESTAMP,
  patient           TEXT,
  encounter         TEXT,
  code              TEXT,
  description       TEXT,
  reasoncode        TEXT,
  reasondescription TEXT
);

-- devices.csv: START, STOP, PATIENT, ENCOUNTER, CODE, DESCRIPTION, UDI
CREATE TABLE coh.devices (
  start        TIMESTAMP,
  stop         TIMESTAMP,
  patient      TEXT,
  encounter    TEXT,
  code         TEXT,
  description  TEXT,
  udi          TEXT
);

-- imaging_studies.csv:
-- Id, DATE, PATIENT, ENCOUNTER, BODYSITE_CODE, BODYSITE_DESCRIPTION, MODALITY_CODE, MODALITY_DESCRIPTION, SOP_CODE, SOP_DESCRIPTION
CREATE TABLE coh.imaging_studies (
  id                    TEXT PRIMARY KEY,
  date                  TIMESTAMP,
  patient               TEXT,
  encounter             TEXT,
  bodysite_code         TEXT,
  bodysite_description  TEXT,
  modality_code         TEXT,
  modality_description  TEXT,
  sop_code              TEXT,
  sop_description       TEXT
);

-- immunizations.csv: DATE, PATIENT, ENCOUNTER, CODE, DESCRIPTION, BASE_COST
CREATE TABLE coh.immunizations (
  date         TIMESTAMP,
  patient      TEXT,
  encounter    TEXT,
  code         TEXT,
  description  TEXT,
  base_cost    NUMERIC
);

-- organizations.csv: Id, NAME, ADDRESS, CITY, STATE, ZIP, LAT, LON, PHONE, REVENUE, UTILIZATION
CREATE TABLE coh.organizations (
  id          TEXT PRIMARY KEY,
  name        TEXT,
  address     TEXT,
  city        TEXT,
  state       TEXT,
  zip         TEXT,
  lat         DOUBLE PRECISION,
  lon         DOUBLE PRECISION,
  phone       TEXT,
  revenue     NUMERIC,
  utilization NUMERIC
);

-- payer_transitions.csv: PATIENT, START_YEAR, END_YEAR, PAYER, OWNERSHIP
CREATE TABLE coh.payer_transitions (
  patient     TEXT,
  start_year  INT,
  end_year    INT,
  payer       TEXT,
  ownership   TEXT
);

-- payers.csv:
-- Id, NAME, ADDRESS, CITY, STATE_HEADQUARTERED, ZIP, PHONE, AMOUNT_COVERED, AMOUNT_UNCOVERED, REVENUE,
-- COVERED_ENCOUNTERS, UNCOVERED_ENCOUNTERS, COVERED_MEDICATIONS, UNCOVERED_MEDICATIONS,
-- COVERED_PROCEDURES, UNCOVERED_PROCEDURES, COVERED_IMMUNIZATIONS, UNCOVERED_IMMUNIZATIONS,
-- UNIQUE_CUSTOMERS, QOLS_AVG, MEMBER_MONTHS
CREATE TABLE coh.payers (
  id                    TEXT PRIMARY KEY,
  name                  TEXT,
  address               TEXT,
  city                  TEXT,
  state_headquartered   TEXT,
  zip                   TEXT,
  phone                 TEXT,
  amount_covered        NUMERIC,
  amount_uncovered      NUMERIC,
  revenue               NUMERIC,
  covered_encounters    INT,
  uncovered_encounters  INT,
  covered_medications   INT,
  uncovered_medications INT,
  covered_procedures    INT,
  uncovered_procedures  INT,
  covered_immunizations INT,
  uncovered_immunizations INT,
  unique_customers      INT,
  qols_avg              NUMERIC,
  member_months         INT
);

-- providers.csv: Id, ORGANIZATION, NAME, GENDER, SPECIALITY, ADDRESS, CITY, STATE, ZIP, LAT, LON, UTILIZATION
CREATE TABLE coh.providers (
  id          TEXT PRIMARY KEY,
  organization TEXT,
  name        TEXT,
  gender      TEXT,
  speciality  TEXT,
  address     TEXT,
  city        TEXT,
  state       TEXT,
  zip         TEXT,
  lat         DOUBLE PRECISION,
  lon         DOUBLE PRECISION,
  utilization NUMERIC
);

-- supplies.csv: DATE, PATIENT, ENCOUNTER, CODE, DESCRIPTION, QUANTITY
CREATE TABLE coh.supplies (
  date         TIMESTAMP,
  patient      TEXT,
  encounter    TEXT,
  code         TEXT,
  description  TEXT,
  quantity     INT
);

-- Helpful indexes
CREATE INDEX idx_enc_patient_start ON coh.encounters (patient, start);
CREATE INDEX idx_med_patient_start ON coh.medications (patient, start);
CREATE INDEX idx_obs_patient_date  ON coh.observations (patient, date);

-- =========================
-- Episodes view (long-horizon windows using start/stop)
-- =========================
DROP MATERIALIZED VIEW IF EXISTS coh.episodes;

CREATE MATERIALIZED VIEW coh.episodes AS
WITH e AS (
  SELECT patient, id AS enc_id, start, stop,
         LAG(stop) OVER (PARTITION BY patient ORDER BY start) AS prev_stop
  FROM coh.encounters
),
markers AS (
  SELECT patient, enc_id, start, stop,
         CASE WHEN prev_stop IS NULL OR start > prev_stop + INTERVAL '48 hours'
              THEN 1 ELSE 0 END AS new_ep
  FROM e
),
grp AS (
  SELECT patient, enc_id, start, stop,
         SUM(new_ep) OVER (PARTITION BY patient ORDER BY start) AS ep_idx
  FROM markers
)
SELECT patient,
       MIN(start) AS t0,
       MAX(COALESCE(stop, start)) AS t1,
       COUNT(*) AS n_enc,
       CONCAT(patient,'::',MIN(start)::date,'::',MAX(COALESCE(stop,start))::date,'::',MIN(ep_idx)) AS ep_id
FROM grp
GROUP BY patient, ep_idx;

CREATE INDEX idx_episodes_patient_t0_t1 ON coh.episodes (patient, t0, t1);

COMMIT;
