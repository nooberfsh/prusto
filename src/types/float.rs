use serde::de::{Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoFloat, PrestoMapKey, PrestoTy};

macro_rules! gen_float {
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

use PrestoFloat::*;
gen_float!(f32, F32Seed, PrestoTy::PrestoFloat(F32));
gen_float!(f64, F64Seed, PrestoTy::PrestoFloat(F64));
