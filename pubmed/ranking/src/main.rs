use ranking::{run, Result};

#[tokio::main]
async fn main() -> Result<()> {
    run().await
}
