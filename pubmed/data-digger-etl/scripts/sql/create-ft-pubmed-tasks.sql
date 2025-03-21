
CREATE TABLE IF NOT EXISTS fulltextdownloads_pm
(
    id integer NOT NULL GENERATED BY DEFAULT AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 2147483647 CACHE 1 ),
    identifier character varying(60) ,
    location character varying(200) ,
    year smallint,
    state smallint,
    "timestamp" timestamp without time zone,
    CONSTRAINT fulltextdownloads_pm_pkey PRIMARY KEY (id),
    CONSTRAINT fulltextdownloads_pm_identifier_key UNIQUE (identifier)
)
