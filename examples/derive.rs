#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use std::env::var;

use dotenv::dotenv;
use prusto::{Presto, ClientBuilder};

#[derive(Presto, Debug)]
struct Foo {
    a: i64,
    b: f64,
    c: String,
}

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

    let data = cli.get_all::<Foo>(sql).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
