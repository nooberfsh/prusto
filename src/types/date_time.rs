use chrono::{DateTime, FixedOffset};
use std::fmt;

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Context, Presto, PrestoTy};

macro_rules! gen_date_time {
    ($ty:ty, $seed:ident, $pty:expr, $format:expr, $empty:expr, $expect:expr) => {
        impl Presto for $ty {
            type ValueType<'a> = String;
            type Seed<'a, 'de> = $seed;

            fn value(&self) -> Self::ValueType<'_> {
                self.format($format).to_string()
            }

            fn ty() -> PrestoTy {
                $pty
            }

            fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
                $seed
            }

            fn empty() -> Self {
                $empty
            }
        }

        pub struct $seed;

        impl<'de> Visitor<'de> for $seed {
            type Value = $ty;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str($expect)
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                <$ty>::parse_from_str(v, $format).map_err(|e| {
                    de::Error::custom(format!("deserialize {} failed, reason: {}", $expect, e))
                })
            }
        }

        impl<'de> DeserializeSeed<'de> for $seed {
            type Value = $ty;
            fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
            where
                D: Deserializer<'de>,
            {
                deserializer.deserialize_str(self)
            }
        }
    };
}

gen_date_time!(
    NaiveDate,
    NaiveDateSeed,
    PrestoTy::Date,
    "%Y-%m-%d",
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
    "naive date"
);
gen_date_time!(
    NaiveDateTime,
    NaiveDateTimeSeed,
    PrestoTy::Timestamp,
    "%Y-%m-%d %H:%M:%S%.3f",
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap(),
    "naive time"
);
gen_date_time!(
    NaiveTime,
    NaiveTimeSeed,
    PrestoTy::Time,
    "%H:%M:%S%.3f",
    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    "naive date time"
);
gen_date_time!(
    DateTime<FixedOffset>,
    DateTimeWithZoneSeed,
    PrestoTy::TimestampWithTimeZone,
    "%Y-%m-%d %H:%M:%S%.3f %:z",
    DateTime::parse_from_str(
        "1970-01-01 00:00:00.000 +00:00",
        "%Y-%m-%d %H:%M:%S%.3f %:z"
    )
    .unwrap(),
    "date time with time zone"
);
