use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoTy};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct FixedChar<const P: usize> {
    inner: String,
}

impl<const P: usize> FixedChar<P> {
    pub fn into_string(self) -> String {
        self.inner
    }
}

impl<const P: usize> Presto for FixedChar<P> {
    type ValueType<'a> = &'a str;
    type Seed<'a, 'de> = FixedCharSeed<P>;

    fn value(&self) -> Self::ValueType<'_> {
        &self.inner
    }
    fn ty() -> PrestoTy {
        PrestoTy::Char(P)
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        FixedCharSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct FixedCharSeed<const P: usize>;

impl<'de, const P: usize> DeserializeSeed<'de> for FixedCharSeed<P> {
    type Value = FixedChar<P>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Ok(FixedChar {
            inner: s
        })
    }
}
