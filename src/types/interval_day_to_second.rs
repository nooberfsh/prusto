use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Error, Presto, PrestoTy};
use chrono::{NaiveTime, Timelike};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct IntervalDayToSecond {
    negative: bool,
    days: u64,
    hours: u8,
    minutes: u8,
    seconds: u8,
    milliseconds: u32,
}

impl IntervalDayToSecond {
    pub fn total_seconds(&self) -> i64 {
        let total = ((self.days * 24 + self.hours as u64) * 60 + self.minutes as u64) * 60
            + self.seconds as u64;
        let sign = if self.negative { -1 } else { 1 };
        total as i64 * sign
    }
}

impl FromStr for IntervalDayToSecond {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split(' ').collect();
        if parts.len() != 2 {
            return Err(Error::ParseIntervalDayFailed);
        }

        let negative = parts[0].chars().nth(0) == Some('-');
        let day: i64 = parts[0]
            .parse()
            .map_err(|_| Error::ParseIntervalDayFailed)?;

        let time = NaiveTime::parse_from_str(parts[1], "%H:%M:%S%.3f")
            .map_err(|_| Error::ParseIntervalDayFailed)?;
        let d = IntervalDayToSecond {
            negative,
            days: day.abs() as u64,
            hours: time.hour() as u8,
            minutes: time.minute() as u8,
            seconds: time.second() as u8,
            milliseconds: time.nanosecond() / 1000_000,
        };
        Ok(d)
    }
}

impl Presto for IntervalDayToSecond {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = IntervalDayToSecondSeed;

    fn value(&self) -> Self::ValueType<'_> {
        let prefix = if self.negative { "-" } else { "" };
        format!(
            "{}{} {:02}:{:02}:{:02}.{:03}",
            prefix, self.days, self.hours, self.minutes, self.seconds, self.milliseconds
        )
    }
    fn ty() -> PrestoTy {
        PrestoTy::IntervalDayToSecond
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IntervalDayToSecondSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct IntervalDayToSecondSeed;

impl<'de> DeserializeSeed<'de> for IntervalDayToSecondSeed {
    type Value = IntervalDayToSecond;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = IntervalDayToSecond::from_str(s).map_err(de::Error::custom)?;

        Ok(d)
    }
}
