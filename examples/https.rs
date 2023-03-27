use std::env::var;

use dotenv::dotenv;
use prusto::auth::Auth;
use prusto::ssl::Ssl;
use prusto::ClientBuilder;
use prusto_model::types::Row;

#[tokio::main]
async fn main() {
    dotenv().ok();

    let user = var("USERNAME").unwrap();
    let password = var("PASSWORD").unwrap();
    let host = var("HOST").unwrap();
    let port = var("PORT").unwrap().parse().unwrap();
    let catalog = var("CATALOG").unwrap();
    let sql = var("SQL").unwrap();

    let auth = Auth::Basic(user.clone(), Some(password));
    let cli = ClientBuilder::new(user, host)
        .port(port)
        .catalog(catalog)
        .auth(auth)
        .secure(true)
        .ssl(Ssl {
            root_cert: Some(Ssl::read_pem(&"/path/root.pem").unwrap()),
        })
        .build()
        .unwrap();

    let data = cli.get_all::<Row>(sql).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
