use std::net::{IpAddr, Ipv4Addr};
use std::str::FromStr;

use serde::de::{self, Deserialize, DeserializeSeed, Deserializer};

use super::{Context, Presto, PrestoTy};

impl Presto for IpAddr {
    type ValueType<'a> = &'a IpAddr;
    type Seed<'a, 'de> = IpAddrSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::IpAddress
    }
    fn seed<'a, 'de>(_ctx: &'a Context) -> Self::Seed<'a, 'de> {
        IpAddrSeed
    }

    fn empty() -> Self {
        IpAddr::V4(Ipv4Addr::new(0,0,0,0))
    }
}

pub struct IpAddrSeed;

impl<'de> DeserializeSeed<'de> for IpAddrSeed {
    type Value = IpAddr;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
        where
            D: Deserializer<'de>,
    {
        let s = <&'de str as Deserialize<'de>>::deserialize(deserializer)?;
        let d = IpAddr::from_str(s).map_err(de::Error::custom)?;

        Ok(d)
    }
}
