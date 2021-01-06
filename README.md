# Prusto

A [presto](https://prestosql.io/) client library written in rust.


## Prerequisites
 - latest rust nightly compiler
 

## Installation 

```toml
# Cargo.toml
[dependencies]
prusto = "0.2"
```

## Example

```rust
#![allow(incomplete_features)]
#![feature(generic_associated_types)]

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
