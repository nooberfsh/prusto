#![allow(incomplete_features)]

use prusto::Presto;

#[derive(Presto)]
struct Person  {
    name: String,
    age: i32,
}

#[derive(Presto)]
struct Group  {
    name: String,
    leader: Person,
}

#[derive(Presto)]
struct Foo  {
    name: String,
    bar: i32,
}

#[derive(Presto)]
struct Generic<T: Presto>  {
    name: String,
    t: T,
}

fn test_simple() {
    let p = Person {
        name: "h".to_string(),
        age: 5
    };

    assert_eq!(p.value(), (&"h".to_string(), &5));
}

fn test_nested() {
    let g = Group {
        name: "g1".to_string(),
        leader: Person {
            name: "h".to_string(),
            age: 5
        },
    };

    assert_eq!(g.value(), (&"g1".to_string(), (&"h".to_string(), &5)));
}

fn test_generic() {
    let g = Generic {
        name: "gen".to_string(),
        t: Foo {
            name: "foo".to_string(),
            bar: 10,
        }
    };

    assert_eq!(g.value(), (&"gen".to_string(), (&"foo".to_string(), &10)));
}

fn test_wrap() {
    #[derive(Presto)]
    struct A {
        a: u32,
    }

    let a = A {a: 1};
    assert_eq!(a.value(), (&1,));
}

fn main() {
    test_simple();
    test_nested();
    test_generic();
    test_wrap();
}
