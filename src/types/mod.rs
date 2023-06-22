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
use std::fmt;
use std::iter::FromIterator;
use std::sync::Arc;

use iterable::*;
use serde::de::DeserializeSeed;
use serde::Serialize;
use strum::Display;

use crate::{
    ClientTypeSignatureParameter, Column, NamedTypeSignature, RawPrestoTy, RowFieldName,
    TypeSignature,
};

//TODO: refine it
#[derive(Debug, Display)]
pub enum Error {
    InvalidPrestoType,
    InvalidColumn,
    InvalidTypeSignature,
    ParseDecimalFailed(String),
    ParseIntervalMonthFailed,
    ParseIntervalDayFailed,
    ParseRoleFailed,
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
    Unknown,
}

impl fmt::Display for PrestoTy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use PrestoTy::*;

        match self {
            Date => write!(f, "{}", RawPrestoTy::Date)?,
            Time => write!(f, "{}", RawPrestoTy::Time)?,
            Timestamp => write!(f, "{}", RawPrestoTy::Timestamp)?,
            Uuid => write!(f, "{}", RawPrestoTy::Uuid)?,
            IntervalYearToMonth => write!(f, "{}", RawPrestoTy::IntervalYearToMonth)?,
            IntervalDayToSecond => write!(f, "{}", RawPrestoTy::IntervalDayToSecond)?,
            Option(ty) => write!(f, "{}", *ty)?,
            Boolean => write!(f, "{}", RawPrestoTy::Boolean)?,
            PrestoInt(ty) => write!(f, "{}", RawPrestoTy::from(*ty))?,
            PrestoFloat(ty) => write!(f, "{}", RawPrestoTy::from(*ty))?,
            Varchar => write!(f, "{}", RawPrestoTy::VarChar)?,
            Char(a) => write!(f, "{}({})", RawPrestoTy::Char, a)?,
            Tuple(ts) => write!(
                f,
                "{}({})",
                RawPrestoTy::Row,
                ts.lazy_map(|ty| ty.to_string()).join(",")
            )?,
            Row(ts) => write!(
                f,
                "{}({})",
                RawPrestoTy::Row,
                ts.lazy_map(|(name, ty)| format!("{} {}", name, ty)).join(",")
            )?,
            Array(ty) => write!(
                f,
                "{}({})",
                RawPrestoTy::Array,
                *ty,
            )?,
            Map(ty1, ty2) => write!(
                f,
                "{}({},{})",
                RawPrestoTy::Map,
                *ty1,
                *ty2,
            )?,
            Decimal(p, s) => write!(f, "{}({},{})", RawPrestoTy::Decimal, p, s)?,
            IpAddress => write!(f, "{}", RawPrestoTy::IpAddress)?,
            Unknown => write!(f, "{}", RawPrestoTy::Unknown)?,
        }
        Ok(())
    }
}

/// Represents the four different kind of integers that Presto/Trino supports
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrestoInt {
    I8,
    I16,
    I32,
    I64,
}

/// Represents two different types of floats that Presto/Trino supports
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PrestoFloat {
    F32,
    F64,
}

impl PrestoTy {
    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper TryFrom trait implementation"
    )]
    #[inline(always)]
    pub fn from_type_signature(sig: TypeSignature) -> Result<Self, Error> {
        sig.try_into()
    }

    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper TryFrom trait implementation"
    )]
    #[inline(always)]
    pub fn from_column(column: Column) -> Result<(String, Self), Error> {
        column.try_into()
    }

    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper TryFrom trait implementation"
    )]
    #[inline(always)]
    pub fn from_columns(columns: Vec<Column>) -> Result<Self, Error> {
        columns.try_into()
    }

    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper From trait implementation"
    )]
    #[inline(always)]
    pub fn into_type_signature(self) -> TypeSignature {
        self.into()
    }

    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper fmt::Display trait implementation"
    )]
    pub fn full_type(&self) -> Cow<'static, str> {
        self.to_string().into()
    }

    #[deprecated(
        since = "0.7.0",
        note = "replaced with proper From trait implementation"
    )]
    #[inline(always)]
    pub fn raw_type(&self) -> RawPrestoTy {
        self.clone().into()
    }
}

impl TryFrom<Column> for (String, PrestoTy) {
    type Error = Error;

    fn try_from(value: Column) -> Result<Self, Self::Error> {
        let name = value.name;
        if let Some(sig) = value.type_signature {
            let ty = sig.try_into()?;
            Ok((name, ty))
        } else {
            Err(Error::InvalidColumn)
        }
    }
}

impl TryFrom<Vec<Column>> for PrestoTy {
    type Error = Error;

    fn try_from(value: Vec<Column>) -> Result<Self, Self::Error> {
        let row = value.try_map(|col| col.try_into())?;
        Ok(PrestoTy::Row(row))
    }
}

impl TryFrom<TypeSignature> for PrestoTy {
    type Error = Error;

    fn try_from(mut value: TypeSignature) -> Result<Self, Self::Error> {
        use PrestoFloat::*;
        use PrestoInt::*;

        let ty = match value.raw_type {
            RawPrestoTy::Date => PrestoTy::Date,
            RawPrestoTy::Time => PrestoTy::Time,
            RawPrestoTy::Timestamp => PrestoTy::Timestamp,
            RawPrestoTy::IntervalYearToMonth => PrestoTy::IntervalYearToMonth,
            RawPrestoTy::IntervalDayToSecond => PrestoTy::IntervalDayToSecond,
            RawPrestoTy::Unknown => PrestoTy::Unknown,
            RawPrestoTy::Decimal if value.arguments.len() == 2 => {
                let s_sig = value.arguments.pop().unwrap();
                let p_sig = value.arguments.pop().unwrap();
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
            RawPrestoTy::Char if value.arguments.len() == 1 => {
                if let ClientTypeSignatureParameter::LongLiteral(p) = value.arguments.pop().unwrap()
                {
                    PrestoTy::Char(p as usize)
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Array if value.arguments.len() == 1 => {
                let sig = value.arguments.pop().unwrap();
                if let ClientTypeSignatureParameter::TypeSignature(sig) = sig {
                    PrestoTy::Array(Box::new(sig.try_into()?))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Map if value.arguments.len() == 2 => {
                let v_sig = value.arguments.pop().unwrap();
                let k_sig = value.arguments.pop().unwrap();
                if let (
                    ClientTypeSignatureParameter::TypeSignature(k_sig),
                    ClientTypeSignatureParameter::TypeSignature(v_sig),
                ) = (k_sig, v_sig)
                {
                    PrestoTy::Map(Box::new(k_sig.try_into()?), Box::new(v_sig.try_into()?))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            RawPrestoTy::Row if !value.arguments.is_empty() => {
                let ir = value.arguments.try_map(|arg| match arg {
                    ClientTypeSignatureParameter::NamedTypeSignature(sig) => {
                        let name = sig.field_name.map(|n| n.name);
                        let ty = sig.type_signature.try_into()?;
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
            _ => return Err(Error::InvalidTypeSignature),
        };

        Ok(ty)
    }
}

impl From<PrestoTy> for TypeSignature {
    fn from(value: PrestoTy) -> Self {
        use PrestoTy::*;

        let params = match value.clone() {
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
            Option(ty) => return (*ty).into(),
            Boolean => vec![],
            PrestoInt(_) => vec![],
            PrestoFloat(_) => vec![],
            Varchar => vec![ClientTypeSignatureParameter::LongLiteral(2147483647)],
            Char(a) => vec![ClientTypeSignatureParameter::LongLiteral(a as u64)],
            Tuple(ts) => ts.map(|ty| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: None,
                    type_signature: ty.into(),
                })
            }),
            Row(ts) => ts.map(|(name, ty)| {
                ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                    field_name: Some(RowFieldName::new(name)),
                    type_signature: ty.into(),
                })
            }),
            Array(ty) => vec![ClientTypeSignatureParameter::TypeSignature(
                (*ty).into(),
            )],
            Map(ty1, ty2) => vec![
                ClientTypeSignatureParameter::TypeSignature((*ty1).into()),
                ClientTypeSignatureParameter::TypeSignature((*ty2).into()),
            ],
            IpAddress => vec![],
            Uuid => vec![],
        };

        TypeSignature::new(value.into(), params)
    }
}

impl From<PrestoTy> for RawPrestoTy {
    fn from(value: PrestoTy) -> RawPrestoTy {
        match value {
            PrestoTy::Unknown => RawPrestoTy::Unknown,
            PrestoTy::Date => RawPrestoTy::Date,
            PrestoTy::Time => RawPrestoTy::Time,
            PrestoTy::Timestamp => RawPrestoTy::Timestamp,
            PrestoTy::IntervalYearToMonth => RawPrestoTy::IntervalYearToMonth,
            PrestoTy::IntervalDayToSecond => RawPrestoTy::IntervalDayToSecond,
            PrestoTy::Decimal(_, _) => RawPrestoTy::Decimal,
            PrestoTy::Option(ty) => (*ty).into(),
            PrestoTy::Boolean => RawPrestoTy::Boolean,
            PrestoTy::PrestoInt(ty) => ty.into(),
            PrestoTy::PrestoFloat(ty) => ty.into(),
            PrestoTy::Varchar => RawPrestoTy::VarChar,
            PrestoTy::Char(_) => RawPrestoTy::Char,
            PrestoTy::Tuple(_) => RawPrestoTy::Row,
            PrestoTy::Row(_) => RawPrestoTy::Row,
            PrestoTy::Array(_) => RawPrestoTy::Array,
            PrestoTy::Map(_, _) => RawPrestoTy::Map,
            PrestoTy::IpAddress => RawPrestoTy::IpAddress,
            PrestoTy::Uuid => RawPrestoTy::Uuid,
        }
    }
}

impl From<PrestoInt> for RawPrestoTy {
    fn from(value: PrestoInt) -> Self {
        match value {
            PrestoInt::I8 => RawPrestoTy::TinyInt,
            PrestoInt::I16 => RawPrestoTy::SmallInt,
            PrestoInt::I32 => RawPrestoTy::Integer,
            PrestoInt::I64 => RawPrestoTy::BigInt,
        }
    }
}

impl PrestoInt {
    #[deprecated(since = "0.7.0", note = "replaced with From trait implementation")]
    #[inline(always)]
    pub fn raw_type(&self) -> RawPrestoTy {
        (*self).into()
    }
}

impl From<PrestoFloat> for RawPrestoTy {
    fn from(value: PrestoFloat) -> Self {
        match value {
            PrestoFloat::F32 => RawPrestoTy::Real,
            PrestoFloat::F64 => RawPrestoTy::Double,
        }
    }
}

impl PrestoFloat {
    #[deprecated(since = "0.7.0", note = "replaced with From trait implementation")]
    #[inline(always)]
    pub fn raw_type(&self) -> RawPrestoTy {
        (*self).into()
    }
}
