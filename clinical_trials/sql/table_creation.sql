-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_timestamp()
    RETURNS TRIGGER
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
        NEW.updated_at = now();
        RETURN NEW;
    ELSE
        RETURN OLD;
    END IF;
END;
$BODY$;


-- Table: tbl_studies_info
CREATE TABLE IF NOT EXISTS tbl_studies_info
(
    nct_id varchar(255),
    title text,
    description text,
    study_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_info_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_baseline_details
CREATE TABLE IF NOT EXISTS public.tbl_baseline_details
(
    nct_id varchar(255),
    ctgov_group_code varchar(255),
    baseline_title text,
    title text,
    baseline_measurement_details jsonb,
    baseline_group_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_baseline_details_pkey PRIMARY KEY (nct_id, ctgov_group_code, baseline_title)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);
CREATE INDEX IF NOT EXISTS tbl_studies_info_nct_id_idx ON tbl_studies_info(nct_id);


-- Table: tbl_primary_outcome_measurement
CREATE TABLE IF NOT EXISTS public.tbl_primary_outcome_measurement
(
    nct_id varchar(255),
    title text,
    outcome_primary_measurement_details jsonb,
    outcome_primary_measurement_value_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_primary_outcome_measurement_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_secondary_outcome_measurement
CREATE TABLE IF NOT EXISTS public.tbl_secondary_outcome_measurement
(
    nct_id varchar(255),
    title text,
    outcome_secondary_measurement_details jsonb,
    outcome_secondary_measurement_value_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_secondary_outcome_measurement_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_adverse_details
CREATE TABLE IF NOT EXISTS public.tbl_studies_adverse_details
(
    nct_id varchar(255),
    ctgov_group_code varchar(255),
    title text,
    adverse_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_adverse_details_pkey PRIMARY KEY (nct_id, ctgov_group_code)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);
CREATE INDEX IF NOT EXISTS tbl_studies_info_nct_id_idx ON tbl_studies_info(nct_id);


-- Table: tbl_studies_arms_details
CREATE TABLE IF NOT EXISTS public.tbl_studies_arms_details
(
    nct_id varchar(255),
    title text,
    arm_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_arms_details_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_conditions
CREATE TABLE IF NOT EXISTS public.tbl_studies_conditions
(
    nct_id varchar(255),
    title text,
    condition_name text,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_conditions_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_design_outcomes
CREATE TABLE IF NOT EXISTS public.tbl_studies_design_outcomes
(
    nct_id varchar(255),
    title text,
    design_outcome_measures jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_design_outcomes_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_designs
CREATE TABLE IF NOT EXISTS public.tbl_studies_designs
(
    nct_id varchar(255),
    title text,
    design_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_designs_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_eligibilities
CREATE TABLE IF NOT EXISTS public.tbl_studies_eligibilities
(
    nct_id varchar(255),
    title text,
    eligibility_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_eligibilities_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_interventions
CREATE TABLE IF NOT EXISTS public.tbl_studies_interventions
(
    nct_id varchar(255),
    title text,
    study_intervention_compressed_details jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_interventions_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_pubmed_links
CREATE TABLE IF NOT EXISTS public.tbl_studies_pubmed_links
(
    nct_id varchar(255),
    title text,
    pubmedcitation jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_pubmed_links_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Table: tbl_studies_sponsors
CREATE TABLE IF NOT EXISTS public.tbl_studies_sponsors
(
    nct_id varchar(255),
    title text,
    collaboratordetails jsonb,
    created_at timestamp default now(),
    updated_at timestamp default now(),

    CONSTRAINT tbl_studies_sponsors_pkey PRIMARY KEY (nct_id)
);
CREATE INDEX IF NOT EXISTS tbl_studies_info_updated_at_idx ON tbl_studies_info(updated_at);


-- Trigger setup for updated_at in all tables
DO $$
DECLARE
    tbl_name RECORD;
BEGIN
    FOR tbl_name IN
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    LOOP
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.triggers WHERE trigger_name = 'update_' || tbl_name.table_name || '_timestamp'
        )
        THEN
            EXECUTE format('CREATE TRIGGER update_%s_timestamp BEFORE UPDATE ON %s FOR EACH ROW EXECUTE FUNCTION update_timestamp();', tbl_name.table_name, tbl_name.table_name);
        END IF;
    END LOOP;
END;
$$ language plpgsql;