use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use strum::{Display, EnumString, IntoStaticStr};

/// Raw Types representing what the Presto/Trino client application can return
/// 
/// Trino sourced from <https://github.com/trinodb/trino/blob/master/core/trino-spi/src/main/java/io/trino/spi/type/StandardTypes.java>
/// Presto sourced from <https://github.com/prestodb/presto/blob/master/presto-common/src/main/java/com/facebook/presto/common/type/StandardTypes.java>
#[derive(Clone, Copy, Debug, Eq, PartialEq, EnumString, IntoStaticStr, Display)]
#[strum(serialize_all = "lowercase")]
pub enum RawPrestoTy {
    /// A 64-bit signed two’s complement integer with a minimum value of `-2^63` and a maximum value of `2^63 - 1`.
    BigInt,
    /// A 32-bit signed two’s complement integer with a minimum value of `-2^31` and a maximum value of `2^31 - 1`.
    Integer,
    /// A 16-bit signed two’s complement integer with a minimum value of `-2^15` and a maximum value of `2^15 - 1`.
    SmallInt,
    /// A 8-bit signed two’s complement integer with a minimum value of `-2^7` and a maximum value of `2^7 - 1`.
    TinyInt,
    /// This type captures boolean values `true` and `false`.
    Boolean,
    /// Calendar date (year, month, day).
    Date,
    /// A fixed precision decimal number. Precision up to 38 digits is supported but performance is best up to 18 digits.
    Decimal,
    /// A real is a 32-bit inexact, variable-precision implementing the IEEE Standard 754 for Binary Floating-Point Arithmetic.
    Real,
    /// A double is a 64-bit inexact, variable-precision implementing the IEEE Standard 754 for Binary Floating-Point Arithmetic.
    Double,
    /// A HyperLogLog sketch allows efficient computation of approx_distinct(). It starts as a sparse representation, switching to a dense representation when it becomes more efficient.
    #[strum(serialize = "HyperLogLog")]
    HyperLogLog,
    /// A quantile digest (qdigest) is a summary structure which captures the approximate distribution of data for a given input set, and can be queried to retrieve approximate quantile values from the distribution. The level of accuracy for a qdigest is tunable, allowing for more precise results at the expense of space.
    ///
    /// A qdigest can be used to give approximate answer to queries asking for what value belongs at a certain quantile. A useful property of qdigests is that they are additive, meaning they can be merged together without losing precision.
    ///
    /// A qdigest may be helpful whenever the partial results of approx_percentile can be reused. For example, one may be interested in a daily reading of the 99th percentile values that are read over the course of a week. Instead of calculating the past week of data with approx_percentile, qdigests could be stored daily, and quickly merged to retrieve the 99th percentile value.
    QDigest,
    /// A T-digest (tdigest) is a summary structure which, similarly to qdigest, captures the approximate distribution of data for a given input set. It can be queried to retrieve approximate quantile values from the distribution.
    /// 
    /// TDigest has the following advantages compared to QDigest:
    /// * higher performance
    /// * lower memory usage
    /// * higher accuracy at high and low percentiles
    /// 
    /// T-digests are additive, meaning they can be merged together.
    TDigest,
    /// A P4HyperLogLog sketch is similar to HyperLogLog, but it starts (and remains) in the dense representation.
    #[strum(serialize = "P4HyperLogLog")]
    P4HyperLogLog,
    /// Span of days, hours, minutes, seconds and milliseconds.
    #[strum(serialize = "interval day to second")]
    IntervalDayToSecond,
    /// Span of years and months.
    #[strum(serialize = "interval year to month")]
    IntervalYearToMonth,
    /// Calendar date and time of day without a time zone 
    Timestamp,
    #[strum(serialize = "timestamp with time zone")]
    /// Calendar date and time of day with a time zone 
    TimestampWithTimeZone,
    /// Time of day (hour, minute, second) without a time zone.
    Time,
    /// Time of day (hour, minute, second, millisecond) with a time zone.
    #[strum(serialize = "time with time zone")]
    TimeWithTimeZone,
    /// Variable length binary data.
    VarBinary,
    /// Variable length character data with an optional maximum length.
    VarChar,
    /// Fixed length character data. A CHAR type without length specified has a default length of 1. 
    Char,
    /// A structure made up of fields that allows mixed types. The fields may be of any SQL type.
    /// 
    /// By default, row fields are not named, but names can be assigned.
    Row,
    /// An array of the given component type.
    Array,
    /// A map between the given component types.
    Map,
    /// JSON value type, which can be a JSON object, a JSON array, a JSON number, a JSON string, `true`, `false` or null.
    Json,
    /// An IP address that can represent either an IPv4 or IPv6 address. 
    /// 
    /// Internally, the type is a pure IPv6 address. Support for IPv4 is handled using the IPv4-mapped IPv6 address range (RFC 4291). When creating an IPADDRESS, IPv4 addresses will be mapped into that range.
    /// 
    /// When formatting an IPADDRESS, any address within the mapped range will be formatted as an IPv4 address. Other addresses will be formatted as IPv6 using the canonical format defined in RFC 5952.
    IpAddress,
    /// An IP routing prefix that can represent either an IPv4 or IPv6 address.
    /// 
    /// Internally, an address is a pure IPv6 address. Support for IPv4 is handled using the IPv4-mapped IPv6 address range (RFC 4291#section-2.5.5.2). When creating an IPPREFIX, IPv4 addresses will be mapped into that range. Additionally, addresses will be reduced to the first address of a network.
    /// 
    /// IPPREFIX values will be formatted in CIDR notation, written as an IP address, a slash (‘/’) character, and the bit-length of the prefix. Any address within the IPv4-mapped IPv6 address range will be formatted as an IPv4 address. Other addresses will be formatted as IPv6 using the canonical format defined in RFC 5952.
    #[cfg(feature="presto")]
    IpPrefix,
    Geometry,
    /// This type represents a UUID (Universally Unique IDentifier), also known as a GUID (Globally Unique IDentifier), using the format defined in RFC 4122.
    Uuid,
    Unknown,
}

impl RawPrestoTy {
    #[deprecated(since = "0.7.0", note = "replaced with From trait implementation")]
    #[inline(always)]
    pub fn to_str(&self) -> &'static str {
        self.into()
    }

    #[deprecated(since = "0.7.0", note = "replaced with FromStr trait implementation")]
    #[inline(always)]
    pub fn parse(s: &str) -> Option<Self> {
        use std::str::FromStr;
        RawPrestoTy::from_str(s).ok()
    }
}

impl Serialize for RawPrestoTy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.into())
    }
}

impl<'de> Deserialize<'de> for RawPrestoTy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TyVistor;

        impl<'de> Visitor<'de> for TyVistor {
            type Value = RawPrestoTy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("need str")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                v.try_into().map_err(|_| E::custom(format!("invalid presto type: {}", v)))
            }
        }

        deserializer.deserialize_str(TyVistor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ser() {
        let ty = RawPrestoTy::Char;
        let s = serde_json::to_string(&ty).unwrap();
        assert_eq!(s, "\"char\"");
    }

    #[test]
    fn test_de() {
        let data = "\"char\"";
        let ty = serde_json::from_str::<RawPrestoTy>(data).unwrap();
        assert_eq!(ty, RawPrestoTy::Char);

        let invalid = "\"xxx\"";
        let res = serde_json::from_str::<RawPrestoTy>(invalid);
        assert!(res.is_err());
    }
}
