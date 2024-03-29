# Prusto

A [presto/trino](https://trino.io/) client library written in rust.



## Installation 

```toml
# Cargo.toml
[dependencies]
prusto = "0.5"
```

In order to use this crate as presto client, enable "presto" feature.
```toml
# Cargo.toml
[dependencies]
prusto = { version = "0.5", features = ["presto"] }
```

## Example

```rust
use prusto::{ClientBuilder, Presto};

#[derive(Presto, Debug)]
struct Foo {
    a: i64,
    b: f64,
    c: String,
}

#[tokio::main]
async fn main() {
    let cli = ClientBuilder::new("user", "localhost")
        .port(8090)
        .catalog("catalog")
        .build()
        .unwrap();

    let sql = "select 1 as a, cast(1.1 as double) as b, 'bar' as c ";

    let data = cli.get_all::<Foo>(sql.into()).await.unwrap().into_vec();

    for r in data {
        println!("{:?}", r)
    }
}
```


## License

MIT
