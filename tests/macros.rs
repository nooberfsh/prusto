#[test]
fn test_macros() {
    let t = trybuild::TestCases::new();
    t.pass("tests/macros/presto.rs");
}
