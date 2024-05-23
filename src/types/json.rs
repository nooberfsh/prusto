use crate::{Context, Presto, PrestoTy};
use serde::de::DeserializeSeed;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

impl Presto for Value {
    type ValueType<'a> = &'a Value;
    type Seed<'a, 'de> = ValueSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> PrestoTy {
        PrestoTy::Json
    }

    fn seed<'a, 'de>(ctx: &'a Context<'a>) -> Self::Seed<'a, 'de> {
        ValueSeed
    }

    fn empty() -> Self {
        Value::empty()
    }
}

pub struct ValueSeed;

impl<'de> DeserializeSeed<'de> for ValueSeed {
    type Value = Value;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        <Value as Deserialize<'de>>::deserialize(deserializer)
    }
}
