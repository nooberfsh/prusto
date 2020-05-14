#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use std::fs::File;
use std::io::Read;

use presto::{Presto, QueryResult, RawQueryResult};

fn read(name: &str) -> String {
    let p = "tests/data/models/".to_string() + name;
    let mut f = File::open(p).unwrap();
    let mut buf = String::new();
    f.read_to_string(&mut buf).unwrap();
    buf
}

/////////////////////////////////////////////////////////////////////
// tests for RawDataSet

#[test]
fn test_raw_queued() {
    let s = read("query_result_queued");
    let d = serde_json::from_str::<RawQueryResult>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200513_074020_00002_mgdh8/x26d7c0451ed24f5fb3d68cb79e6bdad2/1";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data_set.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_raw_planning() {
    let s = read("query_result_planning");
    let d = serde_json::from_str::<RawQueryResult>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200514_063813_02434_mgdh8/xf7e62a5d1e1a4bd49f9341379c477ed1/2";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data_set.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_raw_finished() {
    let s = read("query_result_finished");
    let d = serde_json::from_str::<RawQueryResult>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data_set.is_some());
    assert!(d.error.is_none());
}

#[test]
fn test_raw_failed() {
    let s = read("query_result_failed");
    let d = serde_json::from_str::<RawQueryResult>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data_set.is_none());
    assert!(d.error.is_some());
}

/////////////////////////////////////////////////////////////////////
// tests for DataSet

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

#[test]
fn test_queued() {
    let s = read("query_result_queued");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200513_074020_00002_mgdh8/x26d7c0451ed24f5fb3d68cb79e6bdad2/1";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data_set.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_planning() {
    let s = read("query_result_planning");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();
    let uri = "http://localhost:11032/v1/statement/20200514_063813_02434_mgdh8/xf7e62a5d1e1a4bd49f9341379c477ed1/2";

    assert_eq!(d.next_uri, Some(uri.into()));
    assert!(d.data_set.is_none());
    assert!(d.error.is_none());
}

#[test]
fn test_finished() {
    let s = read("query_result_finished");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data_set.is_some());
    assert!(d.error.is_none());
}

#[test]
fn test_failed() {
    let s = read("query_result_failed");
    let d = serde_json::from_str::<QueryResult<A>>(&s).unwrap();

    assert!(d.next_uri.is_none());
    assert!(d.data_set.is_none());
    assert!(d.error.is_some());
}
