use std::cmp::Ord;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde::Serialize;

use super::util::SerializePairIterator;
use super::{Context, Presto, PrestoMapKey, PrestoTy};

macro_rules! gen_map {
    ($ty:ident < $($bound:ident ),* >,  $seed:ident) => {
        // TODO: remove 'static for K V
        // error[E0309]: the parameter type `K` may not live long enough
        //   --> src/types/map.rs:16:34
        //    |
        // 16 |             type ValueType<'a> = impl Serialize;
        //    |                                  ^^^^^^^^^^^^^^
        // ...
        // 93 | gen_map!(BTreeMap<Ord>, BTreeMapSeed);
        //    | -------------------------------------- in this macro invocation
        //    |
        //    = help: consider adding an explicit lifetime bound `K: 'a`...
        //    = note: ...so that the type `K` will meet its required lifetime bounds
        //    = note: this error originates in a macro (in Nightly builds, run with -Z macro-backtrace for more info)
        impl<K: 'static + PrestoMapKey + $($bound+)*, V: 'static + Presto> Presto for $ty<K, V> {
            type ValueType<'a> = impl Serialize;
            type Seed<'a, 'de> = $seed<'a, K, V>;

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
