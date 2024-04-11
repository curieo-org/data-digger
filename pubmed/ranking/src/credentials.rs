use std::io::{BufReader};
use std::fs::File;
use serde_json::Value;

pub fn get_credentials(file_path : String) -> Value {
    // Open the file in read-only mode with buffer.
    let file = File::open(file_path).expect("Cannot open {file_path}");
    let reader = BufReader::new(file);

    let credentials: Value = serde_json::from_reader(reader).expect("JSON was not well-formatted");
    credentials
}