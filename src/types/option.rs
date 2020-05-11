use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};
use serde::Serialize;

use super::{Context, Presto, PrestoTy};

impl<T: Presto> Presto for Option<T> {
    type ValueType<'a> = Option<T::ValueType<'a>>;
    type Seed<'a, 'de> = OptionSeed<'a, T>;

    fn value(&self) -> Self::ValueType<'_> {
        self.map(|t|t.value())
    }

    fn ty() -> PrestoTy {
        T::ty() // TODO:
    }

    fn seed<'a, 'de>(ctx: &'a Context) -> Self::Seed<'a, 'de> {
        OptionSeed::new(ctx)
    }

    fn empty() -> Self {
        None
    }
}

pub struct OptionSeed<'a, T> {
    ctx: &'a Context<'a>,
    _marker: PhantomData<T>
}

impl<'a, T> OptionSeed<'a, T> {
    // caller must provide a valid `Context`
    pub(super) fn new(ctx: &'a Context) -> Self {
        OptionSeed {
            ctx,
            _marker: PhantomData,
        }
    }
}

impl<'a, 'de, T: Presto> Visitor<'de> for OptionSeed<'a, T> {
    type Value = Option<T>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str(T::ty().raw_type().to_str())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> where
        E: de::Error, {
        Ok(None)

    }

    fn visit_some<D>(self, deserializer: D) -> Result<Self::Value, D::Error> where
        D: Deserializer<'de> {
        let seed = T::seed(self.ctx);
        seed.deserialize(deserializer)
    }
}

impl<'a, 'de, T: Presto> DeserializeSeed<'de> for OptionSeed<'a, T> {
    type Value = bool;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
    {
        deserializer.deserialize_option(self)
    }
}
