use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

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

impl<'de> Visitor<'de> for BoolSeed {
    type Value = bool;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("bool")
    }

    fn visit_bool<E>(self, value: bool) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value)
    }
}

impl<'de> DeserializeSeed<'de> for BoolSeed {
    type Value = bool;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_bool(self)
    }
}
