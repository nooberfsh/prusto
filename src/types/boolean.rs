use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoMapKey, PrestoTy};

impl Presto for bool {
    type ValueType<'a> = &'a bool;
    type Seed<'a, 'de> = BoolSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> PrestoTy {
        PrestoTy::Boolean
    }

    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        BoolSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

impl PrestoMapKey for bool {}

pub struct BoolSeed;

impl<'de> DeserializeSeed<'de> for BoolSeed {
    type Value = bool;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        Self::Value::deserialize(deserializer)
    }
}
