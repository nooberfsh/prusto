use serde::de::{Deserialize, DeserializeSeed, Deserializer};
use serde_json::Value;

use crate::types::{Context, Presto, PrestoTy};

#[derive(Debug, Clone)]
pub struct Row {
    data: Vec<Value>,
}

impl Row {
    pub fn into_json(self) -> Vec<Value> {
        self.data
    }
}

impl Presto for Row {
    type ValueType<'a> = &'a [Value];
    type Seed<'a, 'de> = RowSeed;

    fn value(&self) -> Self::ValueType<'_> {
        &self.data
    }

    fn ty() -> PrestoTy {
        PrestoTy::Unknown
    }

    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        RowSeed
    }

    fn empty() -> Self {
        Row { data: vec![] }
    }
}

pub struct RowSeed;

impl<'de> DeserializeSeed<'de> for RowSeed {
    type Value = Row;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let data = <Vec<Value>>::deserialize(deserializer)?;
        Ok(Row { data })
    }
}
