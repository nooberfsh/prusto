use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoInt, PrestoMapKey, PrestoTy};

macro_rules! gen_int {
    ($ty:ty, $seed:ident, $pty:expr) => {
        impl Presto for $ty {
            type ValueType<'a> = &'a $ty;
            type Seed<'a, 'de> = $seed;

            fn value(&self) -> Self::ValueType<'_> {
                self
            }

            fn ty() -> PrestoTy {
                $pty
            }

            fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
                $seed
            }

            fn empty() -> Self {
                Default::default()
            }
        }

        impl PrestoMapKey for $ty {}

        pub struct $seed;

        impl<'de> DeserializeSeed<'de> for $seed {
            type Value = $ty;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                Self::Value::deserialize(deserializer)
            }
        }
    };
}

use PrestoInt::*;
gen_int!(i8, I8Seed, PrestoTy::PrestoInt(I8));
gen_int!(i16, I16Seed, PrestoTy::PrestoInt(I16));
gen_int!(i32, I32Seed, PrestoTy::PrestoInt(I32));
gen_int!(i64, I64Seed, PrestoTy::PrestoInt(I64));

//TODO: u64's presto type is i64, it may > i64::max, same as u8, u16, u32
gen_int!(u8, U8Seed, PrestoTy::PrestoInt(U8));
gen_int!(u16, U16Seed, PrestoTy::PrestoInt(U16));
gen_int!(u32, U32Seed, PrestoTy::PrestoInt(U32));
gen_int!(u64, U64Seed, PrestoTy::PrestoInt(U64));
