use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Error, Presto, PrestoTy};

#[derive(Debug, Default, Eq, PartialEq, Clone)]
pub struct Decimal<const P: usize, const S: usize> {
    inner: bigdecimal::BigDecimal,
}

impl<const P: usize, const S: usize> Decimal<P, S> {
    pub fn into_bigdecimal(self) -> bigdecimal::BigDecimal {
        self.inner
    }
}

impl<const P: usize, const S: usize> FromStr for Decimal<P, S> {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        bigdecimal::BigDecimal::from_str(s)
            .map(|inner| Self { inner })
            .map_err(|e| Error::ParseDecimalFailed(format!("{}", e)))
    }
}

impl<const P: usize, const S: usize> Presto for Decimal<P, S> {
    type ValueType<'a> = String;
    type Seed<'a, 'de> = DecimalSeed<P, S>;

    fn value(&self) -> Self::ValueType<'_> {
        format!("{}", self.inner)
    }
    fn ty() -> PrestoTy {
        PrestoTy::Decimal(P, S)
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        DecimalSeed
    }

    fn empty() -> Self {
        Default::default()
    }
}

pub struct DecimalSeed<const P: usize, const S: usize>;

impl<'de, const P: usize, const S: usize> DeserializeSeed<'de> for DecimalSeed<P, S> {
    type Value = Decimal<P, S>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = bigdecimal::BigDecimal::from_str(s).map_err(|e| de::Error::custom(e))?;

        Ok(Decimal { inner: d })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn smoke() {
        let data = "1123412341234123412341234.2222222220";
        let d = Decimal::<40, 10>::from_str(data).unwrap();
        let s = format!("{}", d.into_bigdecimal());
        assert_eq!(s, data);
    }
}
