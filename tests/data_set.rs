#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use std::fs::File;
use std::io::Read;

use presto::types::DataSet;
use presto::Presto;

#[derive(Presto, Eq, PartialEq, Debug)]
struct A {
    prefix: String,
    pay_id: Option<String>,
}

fn read(name: &str)-> String  {
    let p = "tests/data/".to_string() + name;
    let mut f = File::open(p).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    buf
}

#[test]
fn test_option() {
    let s = read("option");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap().into_vec();

    assert_eq!(d.len(), 3);
    assert_eq!(d[0], A {
       prefix: "a".to_string() ,
        pay_id: None,
    });
    assert_eq!(d[1], A {
        prefix: "b".to_string() ,
        pay_id: Some("Some(b)".to_string()),
    });
    assert_eq!(d[2], A {
        prefix: "c".to_string() ,
        pay_id: None,
    });
}