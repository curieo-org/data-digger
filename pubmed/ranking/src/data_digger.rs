use crate::Result;
use deadpool_postgres::Pool;
use futures_util::{pin_mut, TryStreamExt};
use std::collections::hash_map::Entry;
use std::collections::HashMap;

pub struct CitationCount {
    pub id: String,
    pub citation_count: i32,
}

#[derive(Default)]
pub struct Percentile {
    pub year: i32,
    pub citation_count: i32,
    pub percentile: i16,
}

struct CitationCountRow {
    identifier: String,
    citationcount: i32,
    year: i32,
}

/**
 * read citation counts from a table in the postgres server.
 * Make sure to give the user reading rights!
 *
 *  [e.g. GRANT SELECT ON datadigger.citationcounts TO datadigger]
 */
pub async fn read_citation_counts(
    db_pool: &Pool,
    table: &str,
) -> Result<HashMap<i32, Vec<CitationCount>>> {
    let query = "SELECT identifier, citationcount, year FROM $1";
    println!("Issuing query {query}");

    let mut map: HashMap<i32, Vec<CitationCount>> = HashMap::new();
    let mut record_count: u32 = 0;

    let client = db_pool.get().await?;
    let result_stream = client.query_raw(query, &[table]).await?;
    pin_mut!(result_stream);

    while let Some(row) = result_stream.try_next().await? {
        let citation_count_row = CitationCountRow {
            identifier: row.get("identifier"),
            citationcount: row.get("citationcount"),
            year: row.get("year"),
        };

        let citationcounts = match map.entry(citation_count_row.year) {
            Entry::Occupied(o) => o.into_mut(),
            Entry::Vacant(v) => v.insert(Vec::new()),
        };

        /* do something with row */
        citationcounts.push(CitationCount {
            id: citation_count_row.identifier,
            citation_count: citation_count_row.citationcount,
        });

        record_count += 1;
    }

    println!("Read {} records from {table}.", record_count);
    Ok(map)
}

/**
 * Compute the percentiles for each year
 */
pub async fn compute_percentiles(data: HashMap<i32, Vec<CitationCount>>) -> Vec<Percentile> {
    let mut percentiles: Vec<Percentile> = Vec::new();

    for (year, mut citation_counts) in data {
        // sort the vectors by citation count
        citation_counts.sort_by(|a, b| b.citation_count.cmp(&a.citation_count));

        let items_per_year = citation_counts.len();
        let percent = items_per_year / 100;
        let mut offset = 0;
        for percentile in 0..100 {
            percentiles.push(Percentile {
                year,
                percentile,
                citation_count: citation_counts[offset].citation_count,
            });
            offset += percent;
        }
    }

    percentiles
}

pub async fn write_percentiles(
    db_pool: &Pool,
    table: &str,
    percentiles: &Vec<Percentile>,
) -> Result<()> {
    let query =
        format!("INSERT INTO {table} (year, citationcount, percentile) VALUES ($1, $2, $3)");
    let mut record_count: i32 = 0;
    let client = db_pool.get().await?;

    for percentile in percentiles {
        client
            .query(
                query.as_str(),
                &[
                    &percentile.year,
                    &percentile.citation_count,
                    &percentile.percentile,
                ],
            )
            .await?;
        record_count += 1;
    }

    println!("Written {} records from to {table}.", record_count);
    Ok(())
}

pub async fn execute_queries(db_pool: &Pool, queries: Vec<&str>) -> Result<()> {
    let client = db_pool.get().await?;

    for query in queries {
        client.query(query, &[]).await?;
    }

    Ok(())
}
