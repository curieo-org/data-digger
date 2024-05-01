use postgres::{Client, NoTls, Config};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::{iter, fs};
use std::io::{Error, ErrorKind};
use postgres::fallible_iterator::FallibleIterator;
use dotenv;



pub fn get_postgres_client() -> Client {
    let mut config : Config = Config::new();
    config.password(dotenv::var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD not in right format").as_str());
    config.user(dotenv::var("POSTGRES_USER").expect("POSTGRES_USER not in right format").as_str());
    config.host(dotenv::var("POSTGRES_HOST").expect("POSTGRES_HOST not specified in credentials").as_str());

    match dotenv::var("POSTGRES_PORT").expect("POSTGRES_PORT not specified in credentials").parse::<u16>() {
        Ok(value) => {
            config.port(value);
        }
        Err(_) => {
            println!("Cannot parse port from credentials");
        }
    }
    config.dbname(dotenv::var("POSTGRES_DBNAME").expect("POSTGRES_DBNAME not specified in .env").as_str());
    config.connect(NoTls).expect("Cannot connect {database}")
}


/**
 * read citation counts from a table in the postgres server.
 * Make sure to give the user reading rights!
 *  
 *  [e.g. GRANT SELECT ON datadigger.citationcounts TO datadigger]
 */
pub fn read_citation_counts(client : &mut Client, table : &str) -> HashMap<i32, Vec<CitationCount>> {
    let query = format!("SELECT identifier, citationcount, year FROM {table}");
    println!("Issuing query {query}");
    let mut it = client.query_raw(query.as_str(), iter::empty::<String>())
                            .expect("Query somehow wrong");
    let mut map : HashMap<i32, Vec<CitationCount>> = HashMap::new();
    let mut record_count : u32 = 0;
    loop {
        match it.next() {
            Ok(Some(row)) => {
                let year : i32 = i32::from(row.get::<usize, i16>(2));
                let citationcounts  = 
                    match map.entry(year) {
                        Entry::Occupied(o) => o.into_mut(),
                        Entry::Vacant(v) => v.insert(Vec::new()),
                    };
                /* do something with row */
                citationcounts.push(CitationCount { 
                    id : row.get(0), 
                    citation_count : i32::try_from(row.get::<usize, i64>(1)).unwrap() 
                });
            }
            Ok(None) => break,
            Err(_) => {break;}
        };
        record_count += 1;
    }

    println!("Read {} records from {table}.", record_count);
    map
}

/**
 * Compute the percentiles for each year
 */
pub fn compute_percentiles(data : HashMap<i32, Vec<CitationCount>>) -> Vec<Percentile> {
    let mut percentiles : Vec<Percentile> = Vec::new();
    // sort the vectors by citation count
    for (year, mut citation_counts) in data {
        citation_counts.sort_by(|a, b| b.citation_count.cmp(&a.citation_count));
        let items_per_year = citation_counts.len();
        let percent = items_per_year/100;
        let mut offset = 0;
        for percentile in 0..100 {
            percentiles.push(Percentile { year : year, percentile : percentile, citation_count : citation_counts[offset].citation_count });
            offset += percent;
        }
    }

    percentiles
}

pub fn write_percentiles(mut client : Client, table : &str, percentiles : &Vec<Percentile>) {
    let query = format!("INSERT INTO {table} (year, citationcount, percentile) VALUES ($1, $2, $3)");
    let mut record_count : i32 = 0;
    for percentile in percentiles {
        client.execute(query.as_str(), &[&percentile.year, &percentile.citation_count, &percentile.percentile]).expect("Error writing");
        record_count += 1;
    }   

    println!("Written {} records from to {table}.", record_count);
}

pub fn execute_sql_file(client : &mut Client, file_path : &str) -> Result<(), Error> {
    let queries = fs::read_to_string(file_path)?;
    let queries : Vec<&str> = queries.split(";").collect();

    for query in queries {
        client.execute(query, &[]).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    }

    Ok(())
}


pub struct CitationCount {
    pub id : String,
    pub citation_count : i32
}

#[derive(Default)]
pub struct Percentile {
    pub year : i32,
    pub citation_count : i32,
    pub percentile : i16
}
