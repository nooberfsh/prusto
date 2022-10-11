#![allow(incomplete_features)]

use std::env::var;

use dotenv::dotenv;
use futures::{pin_mut, StreamExt};
use prusto::{ClientBuilder, Row};

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

    let s = cli.get_stream::<Row>(sql);
    pin_mut!(s);
    while let Some(value) = s.next().await {
        let v = value.unwrap().into_vec();
        for e in v {
            println!("got {:?}", e);
        }
    }
}
