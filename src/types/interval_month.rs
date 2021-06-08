use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Error, Presto, PrestoTy};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct IntervalMonth {
    positive: bool,
    year: u32,
    month: u32,
}

impl IntervalMonth {
    pub fn interval(&self) -> i64 {
        let total = self.year * 12 + self.month;
        let sign = if self.positive { 1 } else { -1 };
        total as i64 * sign
    }
}

impl FromStr for IntervalMonth {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (positive, s) = if s.chars().nth(0) == Some('-') {
            (false, &s[1..])
        } else {
            (true, s)
        };
        let parts: Vec<_> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(Error::ParseIntervalMonthFailed);
        }
        let year = parts[0]
            .parse()
            .map_err(|_| Error::ParseIntervalMonthFailed)?;
        let month = parts[1]
            .parse()
            .map_err(|_| Error::ParseIntervalMonthFailed)?;
        Ok(IntervalMonth {
            positive,
            year,
            month,
        })
    }
}

impl Presto for IntervalMonth {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = IntervalMonthSeed;

    fn value(&self) -> Self::ValueType<'_> {
        let prefix = if self.positive { "" } else { "-" };
        format!("{}{}-{}", prefix, self.year, self.month)
    }
    fn ty() -> PrestoTy {
        PrestoTy::IntervalMonth
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IntervalMonthSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct IntervalMonthSeed;

impl<'de> DeserializeSeed<'de> for IntervalMonthSeed {
    type Value = IntervalMonth;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = IntervalMonth::from_str(s).map_err(de::Error::custom)?;

        Ok(d)
    }
}
