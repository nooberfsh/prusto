#![allow(incomplete_features)]
#![feature(generic_associated_types)]

use presto::Presto;

#[derive(Presto)]
struct Person  {
    name: String,
    age: i32,
}

#[derive(Presto)]
struct Group  {
    name: &'static str,
    leader: Person,
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
        name: "g1",
        leader: Person {
            name: "h".to_string(),
            age: 5
        },
    };

    assert_eq!(g.value(), ("g1", (&"h".to_string(), &5)));
}

fn main() {
    test_simple();
    test_nested();
}
