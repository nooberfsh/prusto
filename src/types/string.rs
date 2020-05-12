use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

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

impl<'de> Visitor<'de> for StringSeed {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string")
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }

    fn visit_str<E>(self, s: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
    {
        Ok(s.to_string())
    }
}

impl<'de> DeserializeSeed<'de> for StringSeed {
    type Value = String;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(self)
    }
}
