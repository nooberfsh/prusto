use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrestoTy {
    BIGINT,
    INTEGER,
    SMALLINT,
    TINYINT,
    BOOLEAN,
    DATE,
    DECIMAL,
    REAL,
    DOUBLE,
    HYPER_LOG_LOG,
    QDIGEST,
    P4_HYPER_LOG_LOG,
    INTERVAL_DAY_TO_SECOND,
    INTERVAL_YEAR_TO_MONTH,
    TIMESTAMP,
    TIMESTAMP_WITH_TIME_ZONE,
    TIME,
    TIME_WITH_TIME_ZONE,
    VARBINARY,
    VARCHAR,
    CHAR,
    ROW,
    ARRAY,
    MAP,
    JSON,
    IPADDRESS,
    UUID,
    GEOMETRY,
    BING_TILE,
}

impl PrestoTy {
    pub fn to_str(&self) -> &'static str {
        use PrestoTy::*;
        match *self {
            BIGINT => "bigint",
            INTEGER => "integer",
            SMALLINT => "smallint",
            TINYINT => "tinyint",
            BOOLEAN => "boolean",
            DATE => "date",
            DECIMAL => "decimal",
            REAL => "real",
            DOUBLE => "double",
            HYPER_LOG_LOG => "HyperLogLog",
            QDIGEST => "qdigest",
            P4_HYPER_LOG_LOG => "P4HyperLogLog",
            INTERVAL_DAY_TO_SECOND => "interval day to second",
            INTERVAL_YEAR_TO_MONTH => "interval year to month",
            TIMESTAMP => "timestamp",
            TIMESTAMP_WITH_TIME_ZONE => "timestamp with time zone",
            TIME => "time",
            TIME_WITH_TIME_ZONE => "time with time zone",
            VARBINARY => "varbinary",
            VARCHAR => "varchar",
            CHAR => "char",
            ROW => "row",
            ARRAY => "array",
            MAP => "map",
            JSON => "json",
            IPADDRESS => "ipaddress",
            UUID => "uuid",
            GEOMETRY => "Geometry",
            BING_TILE => "BingTile",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        use PrestoTy::*;
        let ty = match s {
            "bigint" => BIGINT,
            "integer" => INTEGER,
            "smallint" => SMALLINT,
            "tinyint" => TINYINT,
            "boolean" => BOOLEAN,
            "date" => DATE,
            "decimal" => DECIMAL,
            "real" => REAL,
            "double" => DOUBLE,
            "HyperLogLog" => HYPER_LOG_LOG,
            "qdigest" => QDIGEST,
            "P4HyperLogLog" => P4_HYPER_LOG_LOG,
            "interval day to second" => INTERVAL_DAY_TO_SECOND,
            "interval year to month" => INTERVAL_YEAR_TO_MONTH,
            "timestamp" => TIMESTAMP,
            "timestamp with time zone" => TIMESTAMP_WITH_TIME_ZONE,
            "time" => TIME,
            "time with time zone" => TIME_WITH_TIME_ZONE,
            "varbinary" => VARBINARY,
            "varchar" => VARCHAR,
            "char" => CHAR,
            "row" => ROW,
            "array" => ARRAY,
            "map" => MAP,
            "json" => JSON,
            "ipaddress" => IPADDRESS,
            "uuid" => UUID,
            "Geometry" => GEOMETRY,
            "BingTile" => BING_TILE,
            _ => return None,
        };
        Some(ty)
    }
}

impl Serialize for PrestoTy {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_str())
    }
}

impl<'de> Deserialize<'de> for PrestoTy {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct TyVistor;

        impl<'de> Visitor<'de> for TyVistor {
            type Value = PrestoTy;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("need str")
            }

            fn visit_str<E>(self, v: &str) -> Result<Self::Value, E>
            where
                E: de::Error,
            {
                match PrestoTy::parse(v) {
                    Some(d) => Ok(d),
                    None => Err(E::custom(format!("invalid presto type: {}", v))),
                }
            }
        }

        deserializer.deserialize_str(TyVistor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_se() {
        let ty = PrestoTy::CHAR;
        let s = serde_json::to_string(&ty).unwrap();
        assert_eq!(s, "\"char\"");
    }

    #[test]
    fn test_de() {
        let data = "\"char\"";
        let ty = serde_json::from_str::<PrestoTy>(data).unwrap();
        assert_eq!(ty, PrestoTy::CHAR);

        let invalid = "\"xxx\"";
        let res = serde_json::from_str::<PrestoTy>(invalid);
        assert!(res.is_err());
    }
}
