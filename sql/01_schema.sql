CREATE SCHEMA IF NOT EXISTS coh;

-- patients.csv
CREATE TABLE IF NOT EXISTS coh.patients (
  id TEXT PRIMARY KEY,
  birthdate TIMESTAMP NULL,
  deathdate TIMESTAMP NULL,
  ssn TEXT, drivers TEXT, passport TEXT,
  prefix TEXT, first TEXT, last TEXT, suffix TEXT, maiden TEXT,
  marital TEXT, race TEXT, ethnicity TEXT, gender TEXT,
  birthplace TEXT,
  address TEXT, city TEXT, state TEXT, county TEXT, zip TEXT,
  lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  healthcare_expenses NUMERIC, healthcare_coverage NUMERIC
);

-- encounters.csv  (you didn’t paste headers, but Synthea Coherent usually has at least these)
-- If your encounters.csv differs, paste its header and I’ll adjust.
CREATE TABLE IF NOT EXISTS coh.encounters (
  id TEXT PRIMARY KEY,
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT,
  -- add the rest of your encounter columns here *in CSV order* if present
  class TEXT NULL,
  code TEXT NULL, description TEXT NULL,
  reasoncode TEXT NULL, reasondescription TEXT NULL
);

-- conditions.csv
CREATE TABLE IF NOT EXISTS coh.conditions (
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT
);

-- medications.csv  (header not pasted; this is a typical Coherent layout)
CREATE TABLE IF NOT EXISTS coh.medications (
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT NULL, description TEXT,        -- description often holds drug name
  dose TEXT NULL, route TEXT NULL
);

-- observations.csv  (header not pasted; common Coherent layout)
CREATE TABLE IF NOT EXISTS coh.observations (
  date TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT,
  value TEXT, units TEXT
);

-- procedures.csv
CREATE TABLE IF NOT EXISTS coh.procedures (
  date TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT,
  base_cost NUMERIC,
  reasoncode TEXT, reasondescription TEXT
);

-- allergies.csv
CREATE TABLE IF NOT EXISTS coh.allergies (
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT
);

-- careplans.csv
CREATE TABLE IF NOT EXISTS coh.careplans (
  id TEXT PRIMARY KEY,
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT,
  reasoncode TEXT, reasondescription TEXT
);

-- devices.csv
CREATE TABLE IF NOT EXISTS coh.devices (
  start TIMESTAMP, stop TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT, udi TEXT
);

-- imaging_studies.csv
CREATE TABLE IF NOT EXISTS coh.imaging_studies (
  id TEXT PRIMARY KEY,
  date TIMESTAMP,
  patient TEXT, encounter TEXT,
  bodysite_code TEXT, bodysite_description TEXT,
  modality_code TEXT, modality_description TEXT,
  sop_code TEXT, sop_description TEXT
);

-- immunizations.csv
CREATE TABLE IF NOT EXISTS coh.immunizations (
  date TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT,
  base_cost NUMERIC
);

-- organizations.csv
CREATE TABLE IF NOT EXISTS coh.organizations (
  id TEXT PRIMARY KEY,
  name TEXT, address TEXT, city TEXT, state TEXT, zip TEXT,
  lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  phone TEXT, revenue NUMERIC, utilization NUMERIC
);

-- payer_transitions.csv
CREATE TABLE IF NOT EXISTS coh.payer_transitions (
  patient TEXT,
  start_year INT, end_year INT,
  payer TEXT, ownership TEXT
);

-- payers.csv
CREATE TABLE IF NOT EXISTS coh.payers (
  id TEXT PRIMARY KEY,
  name TEXT, address TEXT, city TEXT, state_headquartered TEXT, zip TEXT, phone TEXT,
  amount_covered NUMERIC, amount_uncovered NUMERIC, revenue NUMERIC,
  covered_encounters INT, uncovered_encounters INT,
  covered_medications INT, uncovered_medications INT,
  covered_procedures INT, uncovered_procedures INT,
  covered_immunizations INT, uncovered_immunizations INT,
  unique_customers INT, qols_avg NUMERIC, member_months INT
);

-- providers.csv
CREATE TABLE IF NOT EXISTS coh.providers (
  id TEXT PRIMARY KEY,
  organization TEXT, name TEXT, gender TEXT, speciality TEXT,
  address TEXT, city TEXT, state TEXT, zip TEXT,
  lat DOUBLE PRECISION, lon DOUBLE PRECISION,
  utilization NUMERIC
);

-- supplies.csv
CREATE TABLE IF NOT EXISTS coh.supplies (
  date TIMESTAMP,
  patient TEXT, encounter TEXT,
  code TEXT, description TEXT,
  quantity INT
);

-- Helpful indexes
CREATE INDEX IF NOT EXISTS idx_enc_patient_start ON coh.encounters (patient, start);
CREATE INDEX IF NOT EXISTS idx_med_patient_start ON coh.medications (patient, start);
CREATE INDEX IF NOT EXISTS idx_obs_patient_date   ON coh.observations (patient, date);
