use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Error, Presto, PrestoMapKey, PrestoTy};

impl Presto for i32 {
    type ValueType<'a> = &'a i32;
    type Seed<'a, 'de> = I32Seed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> PrestoTy {
        PrestoTy::Integer
    }

    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(I32Seed)
    }
}

impl PrestoMapKey for i32 {}

pub struct I32Seed;

impl<'de> Visitor<'de> for I32Seed {
    type Value = i32;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("i32 seed")
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value)
    }
}

impl<'de> DeserializeSeed<'de> for I32Seed {
    type Value = i32;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_i32(self)
    }
}
