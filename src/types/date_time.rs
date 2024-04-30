use std::fmt;
use chrono::{DateTime, Utc};

use chrono::naive::{NaiveDate, NaiveDateTime, NaiveTime};
use serde::de::{self, DeserializeSeed, Deserializer, Visitor};

use super::{Context, Presto, PrestoTy};

macro_rules! gen_date_time {
    ($ty:ty, $parse:expr, $seed:ident, $pty:expr, $format:expr, $empty:expr, $expect:expr, $tzmap:expr) => {
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
                $parse(v, $format).map_err(|e| {
                    de::Error::custom(format!("deserialize {} failed, reason: {}", $expect, e))
                }).map($tzmap)
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
    NaiveDate::parse_from_str,
    NaiveDateSeed,
    PrestoTy::Date,
    "%Y-%m-%d",
    NaiveDate::from_ymd_opt(1970, 1, 1).unwrap(),
    "naive date",
    |t| t
);
gen_date_time!(
    NaiveDateTime,
    NaiveDateTime::parse_from_str,
    NaiveDateTimeSeed,
    PrestoTy::Timestamp,
    "%Y-%m-%d %H:%M:%S%.3f",
    NaiveDate::from_ymd_opt(1970, 1, 1)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap(),
    "naive time",
    |t| t
);
gen_date_time!(
    NaiveTime,
    NaiveTime::parse_from_str,
    NaiveTimeSeed,
    PrestoTy::Time,
    "%H:%M:%S%.3f",
    NaiveTime::from_hms_opt(0, 0, 0).unwrap(),
    "naive date time",
    |t| t
);
gen_date_time!(
    DateTime<Utc>,
    NaiveDateTime::parse_from_str,
    TimestampWithTimeZoneSeed,
    PrestoTy::TimestampWithTimeZone,
    "%Y-%m-%d %H:%M:%S%.3f %Z",
    DateTime::default(),
    "date time with time zone",
    |t| t.and_utc()
);