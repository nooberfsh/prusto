mod boolean;
mod data_set;
mod date_time;
mod decimal;
mod fixed_char;
mod float;
mod integer;
mod interval_day_to_second;
mod interval_year_to_month;
mod ip_address;
pub mod json;
mod map;
mod option;
mod row;
mod seq;
mod string;
pub(self) mod util;
pub mod uuid;

pub use self::uuid::*;
pub use boolean::*;
pub use data_set::*;
pub use date_time::*;
pub use decimal::*;
pub use fixed_char::*;
pub use float::*;
pub use integer::*;
pub use interval_day_to_second::*;
pub use interval_year_to_month::*;
pub use ip_address::*;
pub use map::*;
pub use option::*;
pub use row::*;
pub use seq::*;
pub use string::*;

//mod str;
//pub use self::str::*;

use std::borrow::Cow;
use std::collections::HashMap;
use std::iter::FromIterator;
use std::sync::Arc;

use derive_more::Display;
use iterable::*;
use serde::de::{DeserializeSeed, IntoDeserializer};
use serde::Serialize;

use crate::PrestoTy::Uuid;
use crate::{
    ClientTypeSignatureParameter, Column, NamedTypeSignature, RawPrestoTy, RowFieldName,
    TypeSignature,
};

//TODO: refine it
#[derive(Display, Debug)]
pub enum Error {
    InvalidPrestoType,
    InvalidColumn,
    InvalidTypeSignature,
    ParseDecimalFailed(String),
    ParseIntervalMonthFailed,
    ParseIntervalDayFailed,
    EmptyInPrestoRow,
    NonePrestoRow,
}

pub trait Presto {
    type ValueType<'a>: Serialize
    where
        Self: 'a;
    type Seed<'a, 'de>: DeserializeSeed<'de, Value = Self>;

    fn value(&self) -> Self::ValueType<'_>;

    fn ty() -> PrestoTy;

    /// caller must provide a valid context
    fn seed<'a, 'de>(ctx: &'a Context<'a>) -> Self::Seed<'a, 'de>;

    fn empty() -> Self;
}

pub trait PrestoMapKey: Presto {}

#[derive(Debug)]
pub struct Context<'a> {
    ty: &'a PrestoTy,
    map: Arc<HashMap<usize, Vec<usize>>>,
}

impl<'a> Context<'a> {
    pub fn new<T: Presto>(provided: &'a PrestoTy) -> Result<Self, Error> {
        let target = T::ty();
        let ret = extract(&target, provided)?;
        let map = HashMap::from_iter(ret.into_iter());
        Ok(Context {
            ty: provided,
            map: Arc::new(map),
        })
    }

    pub fn with_ty(&'a self, ty: &'a PrestoTy) -> Context<'a> {
        Context {
            ty,
            map: self.map.clone(),
        }
    }

    pub fn ty(&self) -> &PrestoTy {
        self.ty
    }

    pub fn row_map(&self) -> Option<&[usize]> {
        let key = self.ty as *const PrestoTy as usize;
        self.map.get(&key).map(|r| &**r)
    }
}

fn extract(target: &PrestoTy, provided: &PrestoTy) -> Result<Vec<(usize, Vec<usize>)>, Error> {
    use PrestoTy::*;

    match (target, provided) {
        (Unknown, _) => Ok(vec![]),
        (Decimal(p1, s1), Decimal(p2, s2)) if p1 == p2 && s1 == s2 => Ok(vec![]),
        (Option(ty), provided) => extract(ty, provided),
        (Boolean, Boolean) => Ok(vec![]),
        (Date, Date) => Ok(vec![]),
        (Time, Time) => Ok(vec![]),
        (Timestamp, Timestamp) => Ok(vec![]),
        (IntervalYearToMonth, IntervalYearToMonth) => Ok(vec![]),
        (IntervalDayToSecond, IntervalDayToSecond) => Ok(vec![]),
        (PrestoInt(_), PrestoInt(_)) => Ok(vec![]),
        (PrestoFloat(_), PrestoFloat(_)) => Ok(vec![]),
        (Varchar, Varchar) => Ok(vec![]),
        (Char(a), Char(b)) if a == b => Ok(vec![]),
        (Tuple(t1), Tuple(t2)) => {
            if t1.len() != t2.len() {
                Err(Error::InvalidPrestoType)
            } else {
                t1.lazy_zip(t2).try_flat_map(|(l, r)| extract(l, r))
            }
        }
        (Row(t1), Row(t2)) => {
            if t1.len() != t2.len() {
                Err(Error::InvalidPrestoType)
            } else {
                // create a vector of the original element's reference
                let t1k = t1.sorted_by(|t1, t2| Ord::cmp(&t1.0, &t2.0));
                let t2k = t2.sorted_by(|t1, t2| Ord::cmp(&t1.0, &t2.0));

                let ret = t1k.lazy_zip(t2k).try_flat_map(|(l, r)| {
                    if l.0 == r.0 {
                        extract(&l.1, &r.1)
                    } else {
                        Err(Error::InvalidPrestoType)
                    }
                })?;

                let map = t2.map(|provided| t1.position(|target| provided.0 == target.0).unwrap());
                let key = provided as *const PrestoTy as usize;
                Ok(ret.add_one((key, map)))
            }
        }
        (Array(t1), Array(t2)) => extract(t1, t2),
        (Map(t1k, t1v), Map(t2k, t2v)) => Ok(extract(t1k, t2k)?.chain(extract(t1v, t2v)?)),
        (IpAddress, IpAddress) => Ok(vec![]),
        (Uuid, Uuid) => Ok(vec![]),
        (Json, Json) => Ok(vec![]),
        _ => Err(Error::InvalidPrestoType),
    }
}

// TODO:
// VarBinary Json
// TimestampWithTimeZone TimeWithTimeZone
// HyperLogLog P4HyperLogLog
// QDigest
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoTy {
    Date,
    Time,
    Timestamp,
    Uuid,
    IntervalYearToMonth,
    IntervalDayToSecond,
    Option(Box<PrestoTy>),
    Boolean,
    PrestoInt(PrestoInt),
    PrestoFloat(PrestoFloat),
    Varchar,
    Char(usize),
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
    Decimal(usize, usize),
    IpAddress,
    Json,
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoInt {
    I8,
    I16,
    I32,
    I64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoFloat {
    F32,
    F64,
}

impl PrestoTy {
    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        use PrestoFloat::*;
        use PrestoInt::*;

        let ty = match sig.raw_type {
            RawPrestoTy::Date => PrestoTy::Date,
            RawPrestoTy::Time => PrestoTy::Time,
            RawPrestoTy::Timestamp => PrestoTy::Timestamp,
            RawPrestoTy::IntervalYearToMonth => PrestoTy::IntervalYearToMonth,
            RawPrestoTy::IntervalDayToSecond => PrestoTy::IntervalDayToSecond,
            RawPrestoTy::Unknown => PrestoTy::Unknown,
            RawPrestoTy::Decimal if sig.arguments.len() == 2 => {
                let s_sig = sig.arguments.pop().unwrap();
                let p_sig = sig.arguments.pop().unwrap();
                if let (
                    ClientTypeSignatureParameter::LongLiteral(p),
                    ClientTypeSignatureParameter::LongLiteral(s),
                ) = (p_sig, s_sig)
                {
                    PrestoTy::Decimal(p as usize, s as usize)
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Boolean => PrestoTy::Boolean,
            RawPrestoTy::TinyInt => PrestoTy::PrestoInt(I8),
            RawPrestoTy::SmallInt => PrestoTy::PrestoInt(I16),
            RawPrestoTy::Integer => PrestoTy::PrestoInt(I32),
            RawPrestoTy::BigInt => PrestoTy::PrestoInt(I64),
            RawPrestoTy::Real => PrestoTy::PrestoFloat(F32),
            RawPrestoTy::Double => PrestoTy::PrestoFloat(F64),
            RawPrestoTy::VarChar => PrestoTy::Varchar,
            RawPrestoTy::Char if sig.arguments.len() == 1 => {
                if let ClientTypeSignatureParameter::LongLiteral(p) = sig.arguments.pop().unwrap() {
                    PrestoTy::Char(p as usize)
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Array if sig.arguments.len() == 1 => {
                let sig = sig.arguments.pop().unwrap();
                if let ClientTypeSignatureParameter::TypeSignature(sig) = sig {
                    let inner = Self::from_type_signature(sig)?;
                    PrestoTy::Array(Box::new(inner))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Map if sig.arguments.len() == 2 => {
                let v_sig = sig.arguments.pop().unwrap();
                let k_sig = sig.arguments.pop().unwrap();
                if let (
                    ClientTypeSignatureParameter::TypeSignature(k_sig),
                    ClientTypeSignatureParameter::TypeSignature(v_sig),
                ) = (k_sig, v_sig)
                {
                    let k_inner = Self::from_type_signature(k_sig)?;
                    let v_inner = Self::from_type_signature(v_sig)?;
                    PrestoTy::Map(Box::new(k_inner), Box::new(v_inner))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Row if !sig.arguments.is_empty() => {
                let ir = sig.arguments.try_map(|arg| match arg {
                    ClientTypeSignatureParameter::NamedTypeSignature(sig) => {
                        let name = sig.field_name.map(|n| n.name);
                        let ty = Self::from_type_signature(sig.type_signature)?;
                        Ok((name, ty))
                    }
                    _ => Err(Error::InvalidTypeSignature),
                })?;

                let is_named = ir[0].0.is_some();

                if is_named {
                    let row = ir.try_map(|(name, ty)| match name {
                        Some(n) => Ok((n, ty)),
                        None => Err(Error::InvalidTypeSignature),
                    })?;
                    PrestoTy::Row(row)
                } else {
                    let tuple = ir.try_map(|(name, ty)| match name {
                        Some(_) => Err(Error::InvalidTypeSignature),
                        None => Ok(ty),
                    })?;
                    PrestoTy::Tuple(tuple)
                }
            }
            RawPrestoTy::IpAddress => PrestoTy::IpAddress,
            RawPrestoTy::Uuid => PrestoTy::Uuid,
            RawPrestoTy::Json => PrestoTy::Json,
            _ => return Err(Error::InvalidTypeSignature),
        };

        Ok(ty)
    }

    pub fn from_column(column: Column) -> Result<(String, Self), Error> {
        let name = column.name;
        if let Some(sig) = column.type_signature {
            let ty = Self::from_type_signature(sig)?;
            Ok((name, ty))
        } else {
            Err(Error::InvalidColumn)
        }
    }

    pub fn from_columns(columns: Vec<Column>) -> Result<Self, Error> {
        let row = columns.try_map(Self::from_column)?;
        Ok(PrestoTy::Row(row))
    }

    pub fn into_type_signature(self) -> TypeSignature {
        use PrestoTy::*;

        let raw_ty = self.raw_type();

        let params = match self {
            Unknown => vec![],
            Decimal(p, s) => vec![
                ClientTypeSignatureParameter::LongLiteral(p as u64),
                ClientTypeSignatureParameter::LongLiteral(s as u64),
            ],
            Date => vec![],
            Time => vec![],
            Timestamp => vec![],
            IntervalYearToMonth => vec![],
            IntervalDayToSecond => vec![],
            Option(t) => return t.into_type_signature(),
            Boolean => vec![],
            PrestoInt(_) => vec![],
            PrestoFloat(_) => vec![],
            Varchar => vec![ClientTypeSignatureParameter::LongLiteral(2147483647)],
            Char(a) => vec![ClientTypeSignatureParameter::LongLiteral(a as u64)],
            Tuple(ts) => ts.map(|ty| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: None,
                    type_signature: ty.into_type_signature(),
                })
            }),
            Row(ts) => ts.map(|(name, ty)| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: Some(RowFieldName::new(name)),
                    type_signature: ty.into_type_signature(),
                })
            }),
            Array(t) => vec![ClientTypeSignatureParameter::TypeSignature(
                t.into_type_signature(),
            )],
            Map(t1, t2) => vec![
                ClientTypeSignatureParameter::TypeSignature(t1.into_type_signature()),
                ClientTypeSignatureParameter::TypeSignature(t2.into_type_signature()),
            ],
            IpAddress => vec![],
            Uuid => vec![],
            Json => vec![],
        };

        TypeSignature::new(raw_ty, params)
    }

    pub fn full_type(&self) -> Cow<'static, str> {
        use PrestoTy::*;

        match self {
            Unknown => RawPrestoTy::Unknown.to_str().into(),
            Decimal(p, s) => format!("{}({},{})", RawPrestoTy::Decimal.to_str(), p, s).into(),
            Option(t) => t.full_type(),
            Date => RawPrestoTy::Date.to_str().into(),
            Time => RawPrestoTy::Time.to_str().into(),
            Timestamp => RawPrestoTy::Timestamp.to_str().into(),
            IntervalYearToMonth => RawPrestoTy::IntervalYearToMonth.to_str().into(),
            IntervalDayToSecond => RawPrestoTy::IntervalDayToSecond.to_str().into(),
            Boolean => RawPrestoTy::Boolean.to_str().into(),
            PrestoInt(ty) => ty.raw_type().to_str().into(),
            PrestoFloat(ty) => ty.raw_type().to_str().into(),
            Varchar => RawPrestoTy::VarChar.to_str().into(),
            Char(a) => format!("{}({})", RawPrestoTy::Char.to_str(), a).into(),
            Tuple(ts) => format!(
                "{}({})",
                RawPrestoTy::Row.to_str(),
                ts.lazy_map(|ty| ty.full_type()).join(",")
            )
            .into(),
            Row(ts) => format!(
                "{}({})",
                RawPrestoTy::Row.to_str(),
                ts.lazy_map(|(name, ty)| format!("{} {}", name, ty.full_type()))
                    .join(",")
            )
            .into(),
            Array(t) => format!("{}({})", RawPrestoTy::Array.to_str(), t.full_type()).into(),
            Map(t1, t2) => format!(
                "{}({},{})",
                RawPrestoTy::Map.to_str(),
                t1.full_type(),
                t2.full_type()
            )
            .into(),
            IpAddress => RawPrestoTy::IpAddress.to_str().into(),
            Uuid => RawPrestoTy::Uuid.to_str().into(),
            Json => RawPrestoTy::Json.to_str().into(),
        }
    }

    pub fn raw_type(&self) -> RawPrestoTy {
        use PrestoTy::*;

        match self {
            Unknown => RawPrestoTy::Unknown,
            Date => RawPrestoTy::Date,
            Time => RawPrestoTy::Time,
            Timestamp => RawPrestoTy::Timestamp,
            IntervalYearToMonth => RawPrestoTy::IntervalYearToMonth,
            IntervalDayToSecond => RawPrestoTy::IntervalDayToSecond,
            Decimal(_, _) => RawPrestoTy::Decimal,
            Option(ty) => ty.raw_type(),
            Boolean => RawPrestoTy::Boolean,
            PrestoInt(ty) => ty.raw_type(),
            PrestoFloat(ty) => ty.raw_type(),
            Varchar => RawPrestoTy::VarChar,
            Char(_) => RawPrestoTy::Char,
            Tuple(_) => RawPrestoTy::Row,
            Row(_) => RawPrestoTy::Row,
            Array(_) => RawPrestoTy::Array,
            Map(_, _) => RawPrestoTy::Map,
            IpAddress => RawPrestoTy::IpAddress,
            Uuid => RawPrestoTy::Uuid,
            Json => RawPrestoTy::Json,
        }
    }
}

impl PrestoInt {
    pub fn raw_type(&self) -> RawPrestoTy {
        use PrestoInt::*;
        match self {
            I8 => RawPrestoTy::TinyInt,
            I16 => RawPrestoTy::SmallInt,
            I32 => RawPrestoTy::Integer,
            I64 => RawPrestoTy::BigInt,
        }
    }
}

impl PrestoFloat {
    pub fn raw_type(&self) -> RawPrestoTy {
        use PrestoFloat::*;
        match self {
            F32 => RawPrestoTy::Real,
            F64 => RawPrestoTy::Double,
        }
    }
}
