-- Trigger for updated_at
CREATE OR REPLACE FUNCTION update_timestamp()
    RETURNS TRIGGER
    LANGUAGE 'plpgsql'
AS $BODY$
BEGIN
    -- IF row(NEW.*) IS DISTINCT FROM row(OLD.*) THEN
    --     NEW.updated_at = now();
    --     RETURN NEW;
    -- ELSE
    --     RETURN OLD;
    -- END IF;
    NEW.updated_at = now();
    RETURN NEW;
END;
$BODY$;


-- Table: tbl_studies_info
CREATE TABLE IF NOT EXISTS tbl_studies_info
(
    nct_id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    description text COLLATE pg_catalog."default",
    study_details json,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_info_pkey PRIMARY KEY (nct_id)
);


-- Table: tbl_baseline_details
CREATE TABLE IF NOT EXISTS public.tbl_baseline_details
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    baseline_measurement_details jsonb,
    baseline_group_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_baseline_details_pkey PRIMARY KEY (id)
);


-- Table: tbl_primary_outcome_measurement
CREATE TABLE IF NOT EXISTS public.tbl_primary_outcome_measurement
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    outcome_primary_measurement_details jsonb,
    outcome_primary_measurement_value_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_primary_outcome_measurement_pkey PRIMARY KEY (id)
);


-- Table: tbl_secondary_outcome_measurement
CREATE TABLE IF NOT EXISTS public.tbl_secondary_outcome_measurement
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    outcome_secondary_measurement_details jsonb,
    outcome_secondary_measurement_value_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_secondary_outcome_measurement_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_adverse_details
CREATE TABLE IF NOT EXISTS public.tbl_studies_adverse_details
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    adverse_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_adverse_details_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_arms_details
CREATE TABLE IF NOT EXISTS public.tbl_studies_arms_details
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    arm_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_arms_details_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_conditions
CREATE TABLE IF NOT EXISTS public.tbl_studies_conditions
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    condition_name text COLLATE pg_catalog."default",
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_conditions_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_design_outcomes
CREATE TABLE IF NOT EXISTS public.tbl_studies_design_outcomes
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    design_outcome_measures jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_design_outcomes_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_designs
CREATE TABLE IF NOT EXISTS public.tbl_studies_designs
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    design_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_designs_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_eligibilities
CREATE TABLE IF NOT EXISTS public.tbl_studies_eligibilities
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    eligibility_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_eligibilities_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_interventions
CREATE TABLE IF NOT EXISTS public.tbl_studies_interventions
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    study_intervention_compressed_details jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_interventions_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_pubmed_links
CREATE TABLE IF NOT EXISTS public.tbl_studies_pubmed_links
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    pubmedcitation jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_pubmed_links_pkey PRIMARY KEY (id)
);


-- Table: tbl_studies_sponsors
CREATE TABLE IF NOT EXISTS public.tbl_studies_sponsors
(
    id character varying COLLATE pg_catalog."default",
    title text COLLATE pg_catalog."default",
    collaboratordetails json,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),

    CONSTRAINT tbl_studies_sponsors_pkey PRIMARY KEY (id)
);


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