use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde::Serialize;

use super::util::SerializePairIterator;
use super::{Context, Presto, PrestoMapKey, PrestoTy};

impl<K: PrestoMapKey + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
    type ValueType<'a> = impl Serialize;
    type Seed<'a, 'de> = MapSeed<'a, K, V>;

    fn value(&self) -> Self::ValueType<'_> {
        let iter = self.iter().map(|(k, v)| (k.value(), v.value()));

        SerializePairIterator {
            iter,
            size: Some(self.len()),
        }
    }

    fn ty() -> PrestoTy {
        PrestoTy::Map(Box::new(K::ty()), Box::new(V::ty()))
    }

    fn seed<'a, 'de>(ctx: &'a Context<'a>) -> Self::Seed<'a, 'de> {
        if let PrestoTy::Map(t1, t2) = ctx.ty() {
            MapSeed {
                ctx,
                key_ty: &*t1,
                value_ty: &*t2,
                _marker: PhantomData,
            }
        } else {
            unreachable!()
        }
    }

    fn empty() -> Self { Default::default() }
}

pub struct MapSeed<'a, K, V> {
    ctx: &'a Context<'a>,
    key_ty: &'a PrestoTy,
    value_ty: &'a PrestoTy,
    _marker: PhantomData<(K, V)>,
}

impl<'a, 'de, K: PrestoMapKey + Eq + Hash, V: Presto> Visitor<'de> for MapSeed<'a, K, V> {
    type Value = HashMap<K, V>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hash map seed")
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut ret = HashMap::new();
        let key_ctx = self.ctx.with_ty(self.key_ty);
        let value_ctx = self.ctx.with_ty(self.value_ty);
        while let Some((k, v)) = map.next_entry_seed(K::seed(&key_ctx), V::seed(&value_ctx))? {
            ret.insert(k, v);
        }
        Ok(ret)
    }
}

impl<'a, 'de, K: PrestoMapKey + Eq + Hash, V: Presto> DeserializeSeed<'de> for MapSeed<'a, K, V> {
    type Value = HashMap<K, V>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
    }
}
