use clap_derive::Parser;
use clap::Parser;

pub mod credentials;
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

    // list output
    #[clap(short, long)]
    database : Option<String>,

    #[clap(short, long)]
    credentials: String,
}


fn main() {
    let args = Arguments::parse();
    println!("{:?}", args); 
    
    let creds = credentials::get_credentials(args.credentials.clone());
    let database = args.database.expect("please specify --database");
    let mut client = datadigger::get_postgres_client(creds[database].clone());

    let data = datadigger::read_citation_counts(&mut client, "citationcounts");
    let percentiles = datadigger::compute_percentiles(data);

    let table_name = "pubmed_percentiles";
    let _ = client.execute(format!("CREATE TABLE IF NOT EXISTS {table_name} (year INT NOT NULL, citationcount INT NOT NULL, percentile INT2 NOT NULL)").as_str(), &[]);
    datadigger::write_percentiles(client, table_name, &percentiles);

     // println!("There are {} records in the table.", data.len());
}
