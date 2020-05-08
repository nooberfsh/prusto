use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Error, Presto, PrestoMapKey, PrestoTy};

impl Presto for String {
    type ValueType<'a> = &'a String;
    type Seed<'a, 'de> = StringSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(StringSeed)
    }
}

impl PrestoMapKey for String {}

pub struct StringSeed;

impl<'de> Visitor<'de> for StringSeed {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string seed")
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
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
