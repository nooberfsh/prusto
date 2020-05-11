#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use presto::types::{Context, PrestoTy};
use presto::Presto;

#[derive(Presto)]
struct A {
    a: String,
    b: i32,
    c: String,
}

#[derive(Presto)]
struct B {
    b: i32,
    c: String,
    a: String,
}

#[derive(Presto)]
struct C {
    a: A,
    b: i32,
}

#[derive(Presto)]
struct D {
    b: i32,
    a: B,
}

#[test]
fn test_simple() {
    let provided = B::ty();
    let ctx = Context::new::<A>(&provided).unwrap();
    let ret = ctx.row_map().unwrap();

    assert_eq!(ret, &[1, 2, 0]);
}

#[test]
fn test_nested() {
    let provided = D::ty();
    let ctx = Context::new::<C>(&provided).unwrap();

    let ret = ctx.row_map().unwrap();
    assert_eq!(ret, &[1, 0]);

    if let PrestoTy::Row(rows) = &provided {
        assert_eq!(rows.len(), 2);

        let ty = &rows[1].1;
        let ctx = ctx.with_ty(ty);

        let ret = ctx.row_map().unwrap();
        assert_eq!(ret, &[1, 2, 0]);
    } else {
        unreachable!()
    }
}

#[test]
fn test_false() {
    let provided = C::ty();
    let res = Context::new::<B>(&provided);

    assert!(res.is_err());
}
