use std::fmt;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use serde::Serialize;

use super::util::SerializeIterator;
use super::{Context, Presto, PrestoTy};

impl<T: Presto> Presto for Vec<T> {
    type ValueType<'a> = impl Serialize;
    type Seed<'a, 'de> = VecSeed<'a, T>;

    fn value(&self) -> Self::ValueType<'_> {
        let iter = self.iter().map(|t| t.value());

        SerializeIterator {
            iter,
            size: Some(self.len()),
        }
    }

    fn ty() -> PrestoTy {
        PrestoTy::Array(Box::new(T::ty()))
    }

    fn seed<'a, 'de>(ctx: &'a Context) -> Self::Seed<'a, 'de> {
        VecSeed::new(ctx)
    }
}

pub struct VecSeed<'a, T> {
    ctx: &'a Context<'a>,
    ty: &'a PrestoTy,
    _marker: PhantomData<T>,
}

impl<'a, T> VecSeed<'a, T> {
    pub(super) fn new(ctx: &'a Context) -> Self {
        if let PrestoTy::Array(ty) = ctx.ty {
            VecSeed {
                ctx,
                ty: &*ty,
                _marker: PhantomData,
            }
        } else {
            panic!("invalid context")
        }
    }
}

impl<'a, 'de, T: Presto> Visitor<'de> for VecSeed<'a, T> {
    type Value = Vec<T>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("vec seed")
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut ret = vec![];
        let ctx = self.ctx.with_ty(self.ty);
        while let Some(d) = seq.next_element_seed(T::seed(&ctx))? {
            ret.push(d)
        }
        Ok(ret)
    }
}

impl<'a, 'de, T: Presto> DeserializeSeed<'de> for VecSeed<'a, T> {
    type Value = Vec<T>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}
