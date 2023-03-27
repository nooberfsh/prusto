use std::env::var;

use dotenv::dotenv;
use prusto::ClientBuilder;
use prusto_model::types::Row;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let user = var("USER").unwrap();
    let host = var("HOST").unwrap();
    let port = var("PORT").unwrap().parse().unwrap();
    let catalog = var("CATALOG").unwrap();
    let sql = var("SQL").unwrap();

    let cli = ClientBuilder::new(user, host)
        .port(port)
        .catalog(catalog)
        .build()
        .unwrap();

    let data = cli.get_all::<Row>(sql).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
