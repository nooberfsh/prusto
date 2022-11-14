use std::cmp::Ord;
use std::collections::*;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use serde::de::{DeserializeSeed, Deserializer, SeqAccess, Visitor};
use serde::Serialize;

use super::util::SerializeIterator;
use super::{Context, Presto, PrestoTy};

macro_rules! gen_seq {
    ($ty:ident,  $insert:ident, $seed:ident) => {
        gen_seq! { $ty<>, $insert, $seed }
    };
    ($ty:ident < $($bound:ident ),* >,  $insert:ident, $seed:ident) => {
        impl<T: Presto + $($bound+)*> Presto for $ty<T> {
            type ValueType<'a> = impl Serialize + 'a where T: 'a;
            type Seed<'a, 'de> = $seed<'a, T>;

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
                $seed::new(ctx)
            }

            fn empty() -> Self {
                Default::default()
            }
        }

        pub struct $seed<'a, T> {
            ctx: &'a Context<'a>,
            ty: &'a PrestoTy,
            _marker: PhantomData<T>,
        }

        impl<'a, T> $seed<'a, T> {
            // caller must provide a valid `Context`
            pub(super) fn new(ctx: &'a Context) -> Self {
                if let PrestoTy::Array(ty) = ctx.ty {
                    $seed {
                        ctx,
                        ty: &*ty,
                        _marker: PhantomData,
                    }
                } else {
                    panic!("invalid context")
                }
            }
        }

        impl<'a, 'de, T: Presto + $($bound+)*> Visitor<'de> for $seed<'a, T> {
            type Value = $ty<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("sequence of same presto type")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
            where
                A: SeqAccess<'de>,
            {
                let mut ret: $ty<T> = Default::default();
                let ctx = self.ctx.with_ty(self.ty);
                while let Some(d) = seq.next_element_seed(T::seed(&ctx))? {
                    ret.$insert(d);
                }
                Ok(ret)
            }
        }

        impl<'a, 'de, T: Presto + $($bound+)*> DeserializeSeed<'de> for $seed<'a, T> {
            type Value = $ty<T>;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_seq(self)
            }
        }
    };
}

gen_seq!(Vec, push, VecSeed);
gen_seq!(LinkedList, push_back, LinkedListSeed);
gen_seq!(VecDeque, push_back, VecDequeSeed);
gen_seq!(HashSet<Ord,Hash>, insert, HashSetSeed);
gen_seq!(BTreeSet<Ord>, insert, BTreeSetSeed);
gen_seq!(BinaryHeap<Ord>, push, BinaryHeapSeed);
