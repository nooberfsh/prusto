use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Error, Presto, PrestoMapKey, PrestoTy};

impl<'b> Presto for &'b str {
    type ValueType<'a> = &'a str;
    type Seed<'a, 'de> = StrSeed;

    fn value(&self) -> Self::ValueType<'_> {
        *self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }

    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(StrSeed)
    }
}

impl<'b> PrestoMapKey for &'b str {}

pub struct StrSeed;

impl<'de> Visitor<'de> for StrSeed {
    type Value = &'de str;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("&str seed")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}

impl<'de> DeserializeSeed<'de> for StrSeed {
    type Value = &'de str;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}
