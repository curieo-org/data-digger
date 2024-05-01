use clap_derive::Parser;
use clap::Parser;
use dotenv::dotenv;

pub mod datadigger;


#[derive(Default, Parser, Debug)]
#[clap(version, about)]
/// AEX index page parser
struct Arguments {
    // file to copy output to
    #[clap(long)]
    copy : Option<String>,

    // follow links
    #[clap(short, long)]
    excel_output : Option<String>,
}


fn main() {
    dotenv().ok();
    let args = Arguments::parse();
    println!("{:?}", args); 
    
    let mut client = datadigger::get_postgres_client();

    let setup_file_paths = vec!["sql/drop_old_tables.sql", "sql/fill_new_tables.sql"];

    for file in setup_file_paths {
        datadigger::execute_sql_file(&mut client, file).expect(format!("Error executing file {}", file).as_str());
    }

    let data = datadigger::read_citation_counts(&mut client, "citationcounts");
    let percentiles = datadigger::compute_percentiles(data);

    let table_name = "pubmed_percentiles";
    let _ = client.execute(format!("CREATE TABLE IF NOT EXISTS {table_name} (year INT NOT NULL, citationcount INT NOT NULL, percentile INT2 NOT NULL)").as_str(), &[]);
    datadigger::write_percentiles(client, table_name, &percentiles);

     // println!("There are {} records in the table.", data.len());
}
