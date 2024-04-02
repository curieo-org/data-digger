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

-- Trigger setup for updated_at
DO
$$BEGIN
    CREATE TRIGGER update_tbl_studies_info_timestamp
        BEFORE UPDATE ON tbl_studies_info
        FOR EACH ROW
        EXECUTE FUNCTION update_timestamp();
EXCEPTION
    WHEN duplicate_object THEN NULL;
END$$;