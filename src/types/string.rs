use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoMapKey, PrestoTy};

impl Presto for String {
    type ValueType<'a> = &'a String;
    type Seed<'a, 'de> = StringSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        StringSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

impl PrestoMapKey for String {}

pub struct StringSeed;

impl<'de> DeserializeSeed<'de> for StringSeed {
    type Value = String;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::Value::deserialize(deserializer)
    }
}
