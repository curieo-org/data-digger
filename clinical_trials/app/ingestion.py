from app.database_transfer import PGEngine, TableStructure, transfer_table_data
from app.settings import CTDatabaseReaderSettings, Settings
from app.vector_transfer import VectorTransferProcessor


async def ingest_tbl_studies_info(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    vector_transfer_processor: VectorTransferProcessor,
    database_reader: CTDatabaseReaderSettings,
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_info',
        columns=['nct_id', 'title', 'description', 'study_details'],
        primary_keys=['nct_id'],
        embeddable_columns=['title', 'description'],
        vector_metadata_columns=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

    await vector_transfer_processor.database_to_vectors(
        table_structure,
    )

async def ingest_tbl_baseline_details(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_baseline_details',
        columns=['nct_id', 'ctgov_group_code', 'baseline_title', 'title', 'baseline_measurement_details', 'baseline_group_details'],
        primary_keys=['nct_id', 'ctgov_group_code', 'baseline_title']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_primary_outcome_measurement(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_primary_outcome_measurement',
        columns=['nct_id', 'title', 'outcome_primary_measurement_details', 'outcome_primary_measurement_value_details'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_secondary_outcome_measurement(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_secondary_outcome_measurement',
        columns=['nct_id', 'title', 'outcome_secondary_measurement_details', 'outcome_secondary_measurement_value_details'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_adverse_details(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_adverse_details',
        columns=['nct_id', 'ctgov_group_code', 'title', 'adverse_details'],
        primary_keys=['nct_id', 'ctgov_group_code']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_arms_details(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_arms_details',
        columns=['nct_id', 'title', 'arm_details'],
        primary_keys=['nct_id'],

    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_conditions(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_conditions',
        columns=['nct_id', 'title', 'condition_name'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_design_outcomes(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_design_outcomes',
        columns=['nct_id', 'title', 'design_outcome_measures'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_designs(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_designs',
        columns=['nct_id', 'title', 'design_details'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_eligibilities(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_eligibilities',
        columns=['nct_id', 'title', 'eligibility_details'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_interventions(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_interventions',
        columns=['nct_id', 'title', 'study_intervention_compressed_details'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_pubmed_links(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_pubmed_links',
        columns=['nct_id', 'title', 'pubmedcitation'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_tbl_studies_sponsors(
    source_pg_engine: PGEngine,
    target_pg_engine: PGEngine,
    database_reader: CTDatabaseReaderSettings
) -> None:
    table_structure = TableStructure(
        table_name='tbl_studies_sponsors',
        columns=['nct_id', 'title', 'collaboratordetails'],
        primary_keys=['nct_id']
    )

    await transfer_table_data(
        source_pg_engine,
        target_pg_engine,
        table_structure,
        database_reader
    )

async def ingest_all_tables(settings: Settings) -> None:
    source_pg_engine = PGEngine(settings.psql.temporary_db.get_secret_value(), settings.database_reader)

    target_pg_engine = PGEngine(settings.psql.main_db.get_secret_value(), settings.database_reader)

    vector_transfer_processor = VectorTransferProcessor(target_pg_engine, settings)

    # Setup the prod database if it is not already
    with open(settings.database_reader.table_creation_queries_file, 'r') as file:
        queries = file.read()

        target_pg_engine.execute_query_without_return(queries)

    # Ingest data for all tables
    await ingest_tbl_studies_info(source_pg_engine, target_pg_engine, vector_transfer_processor, settings.database_reader)
    await ingest_tbl_baseline_details(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_primary_outcome_measurement(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_secondary_outcome_measurement(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_adverse_details(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_arms_details(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_conditions(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_design_outcomes(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_designs(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_eligibilities(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_interventions(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_pubmed_links(source_pg_engine, target_pg_engine, settings.database_reader)
    await ingest_tbl_studies_sponsors(source_pg_engine, target_pg_engine, settings.database_reader)
    print('Data ingestion completed for all tables')