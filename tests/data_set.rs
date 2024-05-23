#![allow(incomplete_features)]

use std::collections::*;
use std::fs::File;
use std::io::Read;
use std::iter::FromIterator;
use std::str::FromStr;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use maplit::{btreemap, hashmap};
use serde_json::value::Value;

use prusto::types::{DataSet, Decimal};
use prusto::{Column, FixedChar, IntervalDayToSecond, IntervalYearToMonth, Row};
use prusto::{Presto, PrestoFloat, PrestoInt, PrestoTy};
use std::net::IpAddr;
use uuid::Uuid;

fn read(name: &str) -> (String, Value) {
    let p = "tests/data/types/".to_string() + name;
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
fn test_json() {
    let (s, v) = read("json");
    let d = serde_json::from_str::<DataSet<Row>>(&s).unwrap();
    assert_ds(d.clone(), v);
    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0].clone().into_json(),
        vec![Value::String("abc".to_string())]
    );
}

#[test]
fn test_char() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: FixedChar<3>,
    }

    let (s, v) = read("char");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].a.clone().into_string(), "abc");
}

#[test]
fn test_interval_year_to_month() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: IntervalYearToMonth,
        b: IntervalYearToMonth,
        c: IntervalYearToMonth,
    }

    let (s, v) = read("interval_year_to_month");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].a.total_months(), -112);
    assert_eq!(d[0].b.total_months(), 6);
    assert_eq!(d[0].c.total_months(), 72);
}

#[test]
fn test_ip_address() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: IpAddr,
        b: IpAddr,
    }

    let (s, v) = read("ip_address");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].a, IpAddr::from_str("10.0.0.1").unwrap());
}

#[test]
fn test_uuid() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: Uuid,
    }

    let (s, v) = read("uuid");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0].a.hyphenated().to_string(),
        "12151fd2-7586-11e9-8f9e-2a86e4085a59"
    );
}

#[test]
fn test_interval_day_to_second() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: IntervalDayToSecond,
        b: IntervalDayToSecond,
        c: IntervalDayToSecond,
        d: IntervalDayToSecond,
        e: IntervalDayToSecond,
    }

    let (s, v) = read("interval_day_to_second");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0].a.total_seconds(), 123 * 24 * 3600);
    assert_eq!(d[0].b.total_seconds(), 1 * 24 * 3600 * -1);
    assert_eq!(d[0].c.total_seconds(), 13 * 3600);
    assert_eq!(d[0].d.total_seconds(), 11 * 60);
    assert_eq!(d[0].e.total_seconds(), 611);
}

#[test]
fn test_option() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: String,
        b: Option<String>,
    }

    let (s, v) = read("option");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 3);
    assert_eq!(
        d[0],
        A {
            a: "a".to_string(),
            b: None,
        }
    );
    assert_eq!(
        d[1],
        A {
            a: "b".to_string(),
            b: Some("Some(b)".to_string()),
        }
    );
    assert_eq!(
        d[2],
        A {
            a: "c".to_string(),
            b: None,
        }
    );
}

#[test]
fn test_seq() {
    #[derive(Presto, Debug, Clone)]
    struct A {
        a: Vec<i32>,
        b: LinkedList<i32>,
        c: VecDeque<i32>,
    }

    let (s, v) = read("seq");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let mut d = d.into_vec();
    assert_eq!(d.len(), 1);

    let d = d.pop().unwrap();
    assert_eq!(d.a, vec![1, 2, 3]);
    assert_eq!(d.b, LinkedList::from_iter(vec![1, 2, 3]));
    assert_eq!(d.c, VecDeque::from_iter(vec![1, 2, 3]));
}

#[test]
fn test_seq_other() {
    #[derive(Presto, Debug, Clone)]
    struct A {
        a: HashSet<i32>,
        b: BTreeSet<i32>,
        c: BinaryHeap<i32>,
    }

    let (s, _) = read("seq");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();

    let mut d = d.into_vec();
    assert_eq!(d.len(), 1);

    let mut d = d.pop().unwrap();
    assert_eq!(d.a, HashSet::from_iter(vec![1, 2, 3]));
    assert_eq!(d.b, BTreeSet::from_iter(vec![1, 2, 3]));

    assert_eq!(d.c.pop(), Some(3));
    assert_eq!(d.c.pop(), Some(2));
    assert_eq!(d.c.pop(), Some(1));
    assert_eq!(d.c.pop(), None);
}

#[test]
fn test_map() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: HashMap<String, i32>,
        b: BTreeMap<i32, i32>,
        c: i32,
    }

    let (s, v) = read("map");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: hashmap![
                "foo".to_string() => 1,
                "bar".to_string() => 2,
            ],
            b: btreemap![
                 1 => 1,
                 2 => 2,
            ],
            c: 5,
        }
    );
}

#[test]
fn test_row() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: B,
        b: i32,
    }

    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct B {
        x: i32,
        y: i32,
    }

    let (s, v) = read("row");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: B { x: 1, y: 1 },
            b: 5,
        }
    );
}

#[test]
fn test_integer() {
    #[derive(Presto, Eq, PartialEq, Debug, Clone)]
    struct A {
        a: i8,
        b: i16,
        c: i32,
        d: i64,
        e: u64,
        f: u16,
        g: u32,
        h: u8,
    }

    let (s, v) = read("integer");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: -4,
            b: -3,
            c: -2,
            d: -1,
            e: 1,
            f: 2,
            g: 3,
            h: 4,
        }
    );
}

#[test]
fn test_float() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: f32,
        b: f64,
    }

    let (s, _) = read("float");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(
        d[0],
        A {
            a: -3_f32,
            b: -1_f64,
        }
    );
}

#[test]
fn test_bool() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: bool,
        b: bool,
    }

    let (s, v) = read("boolean");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);
    assert_eq!(d[0], A { a: true, b: false });
}

#[test]
fn test_date_time() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: NaiveDate,
        b: NaiveTime,
        c: NaiveDateTime,
    }

    let (s, v) = read("date_time");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);

    let a = NaiveDate::from_ymd_opt(2001, 8, 22).unwrap();
    let b = NaiveTime::from_hms_milli_opt(1, 2, 3, 456).unwrap();
    let c = NaiveDate::from_ymd_opt(2001, 8, 22)
        .unwrap()
        .and_hms_milli_opt(3, 4, 5, 321)
        .unwrap();
    assert_eq!(d[0], A { a, b, c });
}

#[test]
fn test_decimal() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: Decimal<38, 10>,
    }

    let (s, v) = read("decimal");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let d = d.into_vec();
    assert_eq!(d.len(), 1);

    let s = "1123412341234123412341234.2222222220";
    let a = Decimal::from_str(s).unwrap();

    assert_eq!(d[0], A { a });
}

#[test]
fn test_complex() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        a: String,
        b: i32,
        c: bool,
        d: Vec<i32>,
        e: B,
    }

    #[derive(Presto, PartialEq, Debug, Clone)]
    struct B {
        x: i64,
        y: f64,
    }

    // test custom type
    let (s, v) = read("complex");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();
    assert_ds(d.clone(), v);

    let (t, d) = d.split();
    let ty = PrestoTy::Row(t);
    assert_eq!(d.len(), 1);
    assert_eq!(ty, A::ty());
    assert_eq!(
        d[0],
        A {
            a: "abc".into(),
            b: 10,
            c: true,
            d: vec![1, 2, 3],
            e: B { x: 1, y: 1.1 }
        }
    );
}

#[test]
fn test_complex_row() {
    use PrestoFloat::*;
    use PrestoInt::*;

    let (s, v) = read("complex");
    let d = serde_json::from_str::<DataSet<Row>>(&s).unwrap();
    assert_ds(d.clone(), v);
    let (t, _) = d.split();

    assert_eq!(
        t,
        vec![
            ("a".into(), PrestoTy::Varchar),
            ("b".into(), PrestoTy::PrestoInt(I32)),
            ("c".into(), PrestoTy::Boolean),
            (
                "d".into(),
                PrestoTy::Array(Box::new(PrestoTy::PrestoInt(I32)))
            ),
            (
                "e".into(),
                PrestoTy::Row(vec![
                    ("x".into(), PrestoTy::PrestoInt(I64)),
                    ("y".into(), PrestoTy::PrestoFloat(F64))
                ])
            ),
        ]
    );
}

#[test]
fn test_complex_reorder() {
    #[derive(Presto, PartialEq, Debug, Clone)]
    struct A {
        d: Vec<i32>, //0
        c: bool,     //
        b: i32,      //
        e: B,
        a: String, //4
    }

    #[derive(Presto, PartialEq, Debug, Clone)]
    struct B {
        y: f64,
        x: i64,
    }

    // test custom type
    let (s, _) = read("complex");
    let d = serde_json::from_str::<DataSet<A>>(&s).unwrap();

    let (t, d) = d.split();
    let ty = PrestoTy::Row(t);
    assert_eq!(d.len(), 1);
    assert_eq!(ty, A::ty());
    assert_eq!(
        d[0],
        A {
            a: "abc".into(),
            b: 10,
            c: true,
            d: vec![1, 2, 3],
            e: B { x: 1, y: 1.1 }
        }
    );
}
