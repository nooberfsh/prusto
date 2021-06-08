use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Error, Presto, PrestoTy};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct IntervalDay {
    day: i64,
}

impl IntervalDay {
    pub fn interval(&self) -> i64 {
        self.day
    }
}

impl FromStr for IntervalDay {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let parts: Vec<_> = s.split(' ').collect();
        if parts.len() != 2 {
            return Err(Error::ParseIntervalDayFailed);
        }
        let day = parts[0]
            .parse()
            .map_err(|_| Error::ParseIntervalDayFailed)?;
        Ok(IntervalDay { day })
    }
}

impl Presto for IntervalDay {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = IntervalDaySeed;

    fn value(&self) -> Self::ValueType<'_> {
        format!("{} 00:00:00.000", self.day)
    }
    fn ty() -> PrestoTy {
        PrestoTy::IntervalDay
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IntervalDaySeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct IntervalDaySeed;

impl<'de> DeserializeSeed<'de> for IntervalDaySeed {
    type Value = IntervalDay;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = IntervalDay::from_str(s).map_err(de::Error::custom)?;

        Ok(d)
    }
}
