use std::cmp::Ord;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, Visitor};

use super::util::SerializeVecMap;
use super::{Context, Presto, PrestoMapKey, PrestoTy};

macro_rules! gen_map {
    ($ty:ident < $($bound:ident ),* >,  $seed:ident) => {
        impl<K: PrestoMapKey + $($bound+)*, V: Presto> Presto for $ty<K, V> {
            // TODO: use impl trait after https://github.com/rust-lang/rust/issues/63063 stablized.
            // type ValueType<'a> = impl Serialize + 'a where K: 'a, V: 'a;
            type ValueType<'a> = SerializeVecMap<K::ValueType<'a>, V::ValueType<'a>> where K: 'a, V: 'a;
            type Seed<'a, 'de> = $seed<'a, K, V>;

            // fn value(&self) -> Self::ValueType<'_> {
            //     let iter = self.iter().map(|(k, v)| (k.value(), v.value()));
            //
            //     SerializePairIterator {
            //         iter,
            //         size: Some(self.len()),
            //     }
            // }
            fn value(&self) -> Self::ValueType<'_> {
                SerializeVecMap {
                    iter: self.iter().map(|(k, v)| (k.value(), v.value())).collect()
                }
            }

            fn ty() -> PrestoTy {
                PrestoTy::Map(Box::new(K::ty()), Box::new(V::ty()))
            }

            fn seed<'a, 'de>(ctx: &'a Context<'a>) -> Self::Seed<'a, 'de> {
                if let PrestoTy::Map(t1, t2) = ctx.ty() {
                    $seed {
                        ctx,
                        key_ty: &*t1,
                        value_ty: &*t2,
                        _marker: PhantomData,
                    }
                } else {
                    unreachable!()
                }
            }

            fn empty() -> Self {
                Default::default()
            }
        }

        pub struct $seed<'a, K, V> {
            ctx: &'a Context<'a>,
            key_ty: &'a PrestoTy,
            value_ty: &'a PrestoTy,
            _marker: PhantomData<(K, V)>,
        }

        impl<'a, 'de, K: PrestoMapKey + $($bound+)*, V: Presto> Visitor<'de> for $seed<'a, K, V> {
            type Value = $ty<K, V>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("map")
            }
            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut ret: Self::Value =  Default::default();
                let key_ctx = self.ctx.with_ty(self.key_ty);
                let value_ctx = self.ctx.with_ty(self.value_ty);
                while let Some((k, v)) =
                    map.next_entry_seed(K::seed(&key_ctx), V::seed(&value_ctx))?
                {
                    ret.insert(k, v);
                }
                Ok(ret)
            }
        }

        impl<'a, 'de, K: PrestoMapKey + $($bound+)*, V: Presto> DeserializeSeed<'de>
            for $seed<'a, K, V>
        {
            type Value = $ty<K, V>;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_map(self)
            }
        }
    };
}

gen_map!(HashMap<Eq, Hash>, HashMapSeed);
gen_map!(BTreeMap<Ord>, BTreeMapSeed);
