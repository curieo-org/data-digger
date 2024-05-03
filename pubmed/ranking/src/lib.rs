use data_digger::*;
use settings::Settings;
use startup::AppState;

pub mod data_digger;
mod err;
pub mod secrets;
pub mod settings;
pub mod startup;

pub type Result<T> = std::result::Result<T, err::AppError>;

async fn preprocess_database(state: &AppState) -> Result<()> {
    const DROP_OLD_TABLES_QUERIES: &str = include_str!("../sql/drop_old_tables.sql");
    const FILL_NEW_TABLES_QUERIES: &str = include_str!("../sql/fill_new_tables.sql");

    execute_queries(
        &state.postgres_db,
        DROP_OLD_TABLES_QUERIES.split(";").collect(),
    )
    .await?;
    execute_queries(
        &state.postgres_db,
        FILL_NEW_TABLES_QUERIES.split(";").collect(),
    )
    .await?;

    Ok(())
}

pub async fn run() -> Result<()> {
    color_eyre::install()?;

    let settings = Settings::new();
    let state = startup::start(settings).await?;

    preprocess_database(&state).await?;

    let data = read_citation_counts(&state.postgres_db, "citationcounts").await?;
    println!("There are {} records in the table.", data.len());

    let percentiles = compute_percentiles(data).await;

    let table_name = "pubmed_percentiles";
    let query = format!("CREATE TABLE IF NOT EXISTS {table_name} (year INT NOT NULL, citationcount INT NOT NULL, percentile INT2 NOT NULL);");
    execute_queries(&state.postgres_db, vec![query.as_str()]).await?;
    write_percentiles(&state.postgres_db, table_name, &percentiles).await?;

    Ok(())
}
