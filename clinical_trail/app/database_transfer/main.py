from .utils import (
    PGEngine,
    transfer_table_data
)
from app.config import (
    PG_LOCAL_DATABASE,
    PG_PROD_DATABASE,
    PG_PROD_SETUP_FILE
)

def transfer_tbl_studies_info(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_info',
        ['nct_id', 'title', 'description', 'study_details']
    )

def transfer_tbl_baseline_details(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_baseline_details',
        ['id', 'title', 'baseline_measurement_details', 'baseline_group_details']
    )

def transfer_tbl_primary_outcome_measurement(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_primary_outcome_measurement',
        ['id', 'title', 'outcome_primary_measurement_details', 'outcome_primary_measurement_value_details']
    )

def transfer_tbl_secondary_outcome_measurement(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_secondary_outcome_measurement',
        ['id', 'title', 'outcome_secondary_measurement_details', 'outcome_secondary_measurement_value_details']
    )

def transfer_tbl_studies_adverse_details(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_adverse_details',
        ['id', 'title', 'adverse_details']
    )

def transfer_tbl_studies_arms_details(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_arms_details',
        ['id', 'title', 'arm_details']
    )

def transfer_tbl_studies_conditions(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_conditions',
        ['id', 'title', 'condition_name']
    )

def transfer_tbl_studies_design_outcomes(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_design_outcomes',
        ['id', 'title', 'design_outcome_measures']
    )

def transfer_tbl_studies_designs(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_designs',
        ['id', 'title', 'design_details']
    )

def transfer_tbl_studies_eligibilities(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_eligibilities',
        ['id', 'title', 'eligibility_details']
    )

def transfer_tbl_studies_interventions(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_interventions',
        ['id', 'title', 'study_intervention_compressed_details']
    )

def transfer_tbl_studies_pubmed_links(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_pubmed_links',
        ['id', 'title', 'pubmedcitation']
    )

def transfer_tbl_studies_sponsors(
    local_pg_engine: PGEngine,
    prod_pg_engine: PGEngine
) -> None:
    transfer_table_data(
        local_pg_engine,
        prod_pg_engine,
        'tbl_studies_sponsors',
        ['id', 'title', 'collaboratordetails']
    )

async def transfer_all_tables() -> None:
    local_pg_engine = PGEngine(PG_LOCAL_DATABASE)

    prod_pg_engine = PGEngine(PG_PROD_DATABASE)

    # Setup the prod database if it is not already
    with open(PG_PROD_SETUP_FILE, 'r') as file:
        queries = file.read()

        prod_pg_engine.execute_query_without_return(queries)

    # Transfer data for all tables
    transfer_tbl_studies_info(local_pg_engine, prod_pg_engine)
    transfer_tbl_baseline_details(local_pg_engine, prod_pg_engine)
    transfer_tbl_primary_outcome_measurement(local_pg_engine, prod_pg_engine)
    transfer_tbl_secondary_outcome_measurement(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_adverse_details(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_arms_details(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_conditions(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_design_outcomes(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_designs(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_eligibilities(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_interventions(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_pubmed_links(local_pg_engine, prod_pg_engine)
    transfer_tbl_studies_sponsors(local_pg_engine, prod_pg_engine)
    print('Data transfer completed for all tables')