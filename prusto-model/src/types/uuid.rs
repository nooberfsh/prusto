use serde::de::{Deserialize, DeserializeSeed, Deserializer};
use uuid::Uuid;

use super::{Context, Presto, PrestoTy};

impl Presto for Uuid {
    type ValueType<'a> = &'a Uuid;
    type Seed<'a, 'de> = UuidSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Uuid
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        UuidSeed
    }

    fn empty() -> Self {
        Uuid::new_v4()
    }
}

pub struct UuidSeed;

impl<'de> DeserializeSeed<'de> for UuidSeed {
    type Value = Uuid;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        <Uuid as Deserialize<'de>>::deserialize(deserializer)
    }
}
