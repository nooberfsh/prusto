use std::convert::TryFrom;
use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Context, Presto, PrestoInt, PrestoMapKey, PrestoTy};

macro_rules! gen {
    ($ty:ty, $seed:ident, $pty:expr, $des:expr, $de: ident) => {
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

        impl<'de> Visitor<'de> for $seed {
            type Value = $ty;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str($des)
            }

            fn visit_u8<E>(self, value: u8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Self::Value::try_from(value).map_err(|e| E::custom(e))
            }
        }

        impl<'de> DeserializeSeed<'de> for $seed {
            type Value = $ty;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.$de(self)
            }
        }
    };
}

use PrestoInt::*;
gen!(i8, I8Seed, PrestoTy::PrestoInt(I8), "i8", deserialize_i8);
gen!(
    i16,
    I16Seed,
    PrestoTy::PrestoInt(I16),
    "i16",
    deserialize_i16
);
gen!(
    i32,
    I32Seed,
    PrestoTy::PrestoInt(I32),
    "i32",
    deserialize_i32
);
gen!(
    i64,
    I64Seed,
    PrestoTy::PrestoInt(I64),
    "i64",
    deserialize_i64
);

gen!(u8, U8Seed, PrestoTy::PrestoInt(U8), "u8", deserialize_u8);
gen!(
    u16,
    U16Seed,
    PrestoTy::PrestoInt(U16),
    "u16",
    deserialize_u16
);
gen!(
    u32,
    U32Seed,
    PrestoTy::PrestoInt(U32),
    "u32",
    deserialize_u32
);
gen!(
    u64,
    U64Seed,
    PrestoTy::PrestoInt(U64),
    "u64",
    deserialize_u64
); //TODO: u64's presto type is i64, it may > i64::max
