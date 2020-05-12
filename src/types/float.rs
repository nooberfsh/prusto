use std::fmt;

use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Context, Presto, PrestoFloat, PrestoMapKey, PrestoTy};

macro_rules! gen_float {
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
                Ok(value.into())
            }

            fn visit_i8<E>(self, value: i8) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(value.into())
            }

            fn visit_u16<E>(self, value: u16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(value.into())
            }

            fn visit_i16<E>(self, value: i16) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(value.into())
            }

            fn visit_u32<E>(self, value: u32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value as f64 > Self::Value::MAX as f64 {
                    Err(E::custom(format!("{} out of range: {}", $des, value)))
                } else {
                    Ok(value as Self::Value)
                }
            }

            fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value as f64 > Self::Value::MAX as f64
                    || (value as f64) < Self::Value::MIN as f64
                {
                    Err(E::custom(format!("{} out of range: {}", $des, value)))
                } else {
                    Ok(value as Self::Value)
                }
            }

            fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value as f64 > Self::Value::MAX as f64 {
                    Err(E::custom(format!("{} out of range: {}", $des, value)))
                } else {
                    Ok(value as Self::Value)
                }
            }

            fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value as f64 > Self::Value::MAX as f64
                    || (value as f64) < Self::Value::MIN as f64
                {
                    Err(E::custom(format!("{} out of range: {}", $des, value)))
                } else {
                    Ok(value as Self::Value)
                }
            }

            fn visit_f32<E>(self, value: f32) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                Ok(value as Self::Value)
            }

            fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                if value.is_nan() {
                    return Ok(Self::Value::NAN);
                }
                if value > Self::Value::MAX as f64 || value < Self::Value::MIN as f64 {
                    Err(E::custom(format!("{} out of range: {}", $des, value)))
                } else {
                    Ok(value as Self::Value)
                }
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

use PrestoFloat::*;
gen_float!(
    f32,
    F32Seed,
    PrestoTy::PrestoFloat(F32),
    "f32",
    deserialize_f32
);
gen_float!(
    f64,
    F64Seed,
    PrestoTy::PrestoFloat(F64),
    "f64",
    deserialize_f64
);
