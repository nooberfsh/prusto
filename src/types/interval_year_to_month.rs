use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Error, Presto, PrestoTy};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct IntervalYearToMonth {
    negative: bool,
    years: u32,
    months: u32,
}

impl IntervalYearToMonth {
    pub fn total_months(&self) -> i64 {
        let total = self.years * 12 + self.months;
        let sign = if self.negative { -1 } else { 1 };
        total as i64 * sign
    }
}

impl FromStr for IntervalYearToMonth {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (negative, s) = if s.chars().nth(0) == Some('-') {
            (true, &s[1..])
        } else {
            (false, s)
        };
        let parts: Vec<_> = s.split('-').collect();
        if parts.len() != 2 {
            return Err(Error::ParseIntervalMonthFailed);
        }
        let years = parts[0]
            .parse()
            .map_err(|_| Error::ParseIntervalMonthFailed)?;
        let months = parts[1]
            .parse()
            .map_err(|_| Error::ParseIntervalMonthFailed)?;
        Ok(IntervalYearToMonth {
            negative,
            years,
            months,
        })
    }
}

impl Presto for IntervalYearToMonth {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = IntervalYearToMonthSeed;

    fn value(&self) -> Self::ValueType<'_> {
        let prefix = if self.negative { "-" } else { "" };
        format!("{}{}-{}", prefix, self.years, self.months)
    }
    fn ty() -> PrestoTy {
        PrestoTy::IntervalYearToMonth
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IntervalYearToMonthSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct IntervalYearToMonthSeed;

impl<'de> DeserializeSeed<'de> for IntervalYearToMonthSeed {
    type Value = IntervalYearToMonth;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = IntervalYearToMonth::from_str(s).map_err(de::Error::custom)?;

        Ok(d)
    }
}
