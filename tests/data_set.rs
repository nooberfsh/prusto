#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use std::fs::File;
use std::io::Read;

use serde_json::value::Value;

use presto::types::DataSet;
use presto::Column;
use presto::Presto;

#[derive(Presto, Eq, PartialEq, Debug, Clone)]
struct A {
    prefix: String,
    pay_id: Option<String>,
}

fn read(name: &str) -> (String, Value) {
    let p = "tests/data/".to_string() + name;
    let mut f = File::open(p).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();

    let v = serde_json::from_str(&buf).unwrap();
    (buf, v)
}

fn assert_ds<T: Presto>(data_set: DataSet<T>, v: Value) {
    let data_set = serde_json::to_value(data_set).unwrap();
    let (l_meta, l_data) = split(data_set).unwrap();
    let (r_meta, r_data) = split(v).unwrap();

    assert_eq!(l_meta, r_meta);
    assert_eq!(l_data, r_data);
}

// return (meta, data)
fn split(v: Value) -> Option<(Vec<Column>, Value)> {
    if let Value::Object(m) = v {
        if m.len() == 2 {
            let meta = m.get("columns")?.clone();
            let meta = serde_json::from_value(meta).ok()?;
            let data = m.get("data")?.clone();
            Some((meta, data))
        } else {
            None
        }
    } else {
        None
    }
}

#[test]
fn test_option() {
    let (s, v) = read("option");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 3);
    assert_eq!(
        d[0],
        A {
            prefix: "a".to_string(),
            pay_id: None,
        }
    );
    assert_eq!(
        d[1],
        A {
            prefix: "b".to_string(),
            pay_id: Some("Some(b)".to_string()),
        }
    );
    assert_eq!(
        d[2],
        A {
            prefix: "c".to_string(),
            pay_id: None,
        }
    );
}
