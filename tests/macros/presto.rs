use prusto::Presto;
use prusto::tuples::*;

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

#[derive(Presto)]
struct BigStruct {
  a1: i16,
  a2: i16,
  a3: i16,
  a4: i16,
  a5: i16,
  a6: i16,
  a7: i16,
  a8: i16,
  a9: i16,
  a10: i16,
  a11: i16,
  a12: i16,
  a13: i16,
  a14: i16,
  a15: i16,
  a16: i16,
  a17: i16,
  a18: i16,
  a19: i16,
  a20: i16,
  a21: i16,
  a22: i16,
  a23: i16,
  a24: i16,
  a25: i16,
  a26: i16,
  a27: i16,
  a28: i16,
  a29: i16,
  a30: i16,
  a31: i16,
  a32: i16,
}

fn test_simple() {
    let p = Person {
        name: "h".to_string(),
        age: 5
    };

    assert_eq!(p.value(), Tuple2(&"h".to_string(), &5));
}

fn test_nested() {
    let g = Group {
        name: "g1".to_string(),
        leader: Person {
            name: "h".to_string(),
            age: 5
        },
    };

    assert_eq!(g.value(), Tuple2(&"g1".to_string(), Tuple2(&"h".to_string(), &5)));
}

fn test_generic() {
    let g = Generic {
        name: "gen".to_string(),
        t: Foo {
            name: "foo".to_string(),
            bar: 10,
        }
    };

    assert_eq!(g.value(), Tuple2(&"gen".to_string(), Tuple2(&"foo".to_string(), &10)));
}

fn test_wrap() {
    #[derive(Presto)]
    struct A {
        a: u32,
    }

    let a = A {a: 1};
    assert_eq!(a.value(), Tuple1(&1));
}


fn main() {
    test_simple();
    test_nested();
    test_generic();
    test_wrap();
}
