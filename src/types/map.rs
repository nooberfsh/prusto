use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde::Serialize;

use super::util::SerializePairIterator;
use super::{Error, Presto, PrestoMapKey, PrestoTy};

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

    fn seed<'a, 'de>(ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        if let PrestoTy::Map(t1, t2) = ty {
            Ok(MapSeed(t1, t2, PhantomData))
        } else {
            Err(Error::InvalidPrestoType)
        }
    }
}

pub struct MapSeed<'a, K, V>(&'a PrestoTy, &'a PrestoTy, PhantomData<(K, V)>);

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
        while let Some((k, v)) = map.next_entry_seed(
            K::seed(self.0).map_err(|e| <A::Error as de::Error>::custom(format!("{}", e)))?,
            V::seed(self.1).map_err(|e| <A::Error as de::Error>::custom(format!("{}", e)))?,
        )? {
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
