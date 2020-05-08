mod data_set;
mod map;
mod number;
mod str;
mod string;
pub(self) mod util;
mod vec;

pub use self::str::*;
pub use map::*;
pub use number::*;
pub use number::*;
pub use string::*;
pub use vec::*;

use std::borrow::Cow;

use derive_more::Display;
use itertools::Itertools;
use serde::de::DeserializeSeed;
use serde::Serialize;

use crate::{
    ClientTypeSignatureParameter, Column, NamedTypeSignature, RawPrestoTy, RowFieldName,
    TypeSignature,
};

//TODO: refine it
#[derive(Display)]
pub enum Error {
    InvalidPrestoType,
    InvalidColumn,
    InvalidTypeSignature,
}

pub trait Presto {
    type ValueType<'a>: Serialize;
    type Seed<'a, 'de>: DeserializeSeed<'de, Value = Self>;

    fn value(&self) -> Self::ValueType<'_>;
    fn ty() -> PrestoTy;
    fn seed<'a, 'de>(ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error>;
}

pub trait PrestoMapKey: Presto {}

#[derive(Clone, Debug)]
pub enum PrestoTy {
    Integer,
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
}

impl PrestoTy {
    pub fn is_match(&self, other: &PrestoTy) -> bool {
        use PrestoTy::*;

        match (self, other) {
            (Integer, Integer) => true,
            (Varchar, Varchar) => true,
            (Tuple(t1), Tuple(t2)) => {
                if t1.len() != t2.len() {
                    false
                } else {
                    t1.iter().zip(t2.iter()).all(|(l, r)| l.is_match(r))
                }
            }
            (Row(t1), Row(t2)) => {
                if t1.len() != t2.len() {
                    false
                } else {
                    let mut t1k: Vec<_> = t1.clone();
                    t1k.sort_by(|t1, t2| t1.0.cmp(&t2.0));
                    let mut t2k: Vec<_> = t2.clone();
                    t2k.sort_by(|t1, t2| t1.0.cmp(&t2.0));

                    t1k.iter()
                        .zip(t2k.iter())
                        .all(|(l, r)| l.0 == r.0 && l.1.is_match(&r.1))
                }
            }
            (Map(t1k, t1v), Map(t2k, t2v)) => t1k.is_match(t2k) && t1v.is_match(t2v),
            _ => false,
        }
    }

    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        let ty = match sig.raw_type {
            RawPrestoTy::Integer => PrestoTy::Integer,
            RawPrestoTy::VarChar => PrestoTy::Varchar,
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
                let mut ir = Vec::with_capacity(sig.arguments.len());
                for arg in sig.arguments {
                    match arg {
                        ClientTypeSignatureParameter::NamedTypeSignature(sig) => {
                            let name = sig.field_name.map(|n| n.name);
                            let ty = Self::from_type_signature(sig.type_signature)?;
                            ir.push((name, ty));
                        }
                        _ => return Err(Error::InvalidTypeSignature),
                    }
                }

                let is_named = ir[0].0.is_some();

                if is_named {
                    let mut ret = Vec::with_capacity(ir.len());
                    for (name, ty) in ir {
                        if let Some(n) = name {
                            ret.push((n, ty))
                        } else {
                            return Err(Error::InvalidTypeSignature);
                        }
                    }
                    PrestoTy::Row(ret)
                } else {
                    let mut ret = Vec::with_capacity(ir.len());
                    for (name, ty) in ir {
                        if let Some(_) = name {
                            return Err(Error::InvalidTypeSignature);
                        } else {
                            ret.push(ty)
                        }
                    }
                    PrestoTy::Tuple(ret)
                }
            }
            _ => return Err(Error::InvalidTypeSignature),
        };

        Ok(ty)
    }

    pub fn from_columns(columns: Vec<Column>) -> Result<Self, Error> {
        let mut ret = Vec::with_capacity(columns.len());
        for column in columns {
            if let Some(sig) = column.type_signature {
                let ty = Self::from_type_signature(sig)?;
                ret.push((column.name, ty));
            } else {
                return Err(Error::InvalidColumn);
            }
        }

        Ok(PrestoTy::Row(ret))
    }

    pub fn into_type_signature(self) -> TypeSignature {
        use PrestoTy::*;

        let raw_ty = self.raw_type();

        let params = match self {
            Integer => vec![],
            Varchar => vec![ClientTypeSignatureParameter::LongLiteral(2147483647)],
            Tuple(ts) => ts
                .into_iter()
                .map(|ty| {
                    ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                        field_name: None,
                        type_signature: ty.into_type_signature(),
                    })
                })
                .collect(),
            Row(ts) => ts
                .into_iter()
                .map(|(name, ty)| {
                    ClientTypeSignatureParameter::NamedTypeSignature(NamedTypeSignature {
                        field_name: Some(RowFieldName::new(name)),
                        type_signature: ty.into_type_signature(),
                    })
                })
                .collect(),
            Array(t) => vec![ClientTypeSignatureParameter::TypeSignature(
                t.into_type_signature(),
            )],
            Map(t1, t2) => vec![
                ClientTypeSignatureParameter::TypeSignature(t1.into_type_signature()),
                ClientTypeSignatureParameter::TypeSignature(t2.into_type_signature()),
            ],
        };

        TypeSignature::new(raw_ty, params)
    }

    pub fn full_type(&self) -> Cow<'static, str> {
        use PrestoTy::*;

        match self {
            Integer => RawPrestoTy::Integer.to_str().into(),
            Varchar => RawPrestoTy::VarChar.to_str().into(),
            Tuple(ts) => format!(
                "{}({})",
                RawPrestoTy::Row.to_str(),
                ts.iter().map(|ty| ty.full_type()).join(",")
            )
            .into(),
            Row(ts) => format!(
                "{}({})",
                RawPrestoTy::Row.to_str(),
                ts.iter()
                    .map(|(name, ty)| format!("{} {}", name, ty.full_type()))
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
        }
    }

    pub fn raw_type(&self) -> RawPrestoTy {
        use PrestoTy::*;

        match self {
            Integer => RawPrestoTy::Integer,
            Varchar => RawPrestoTy::VarChar,
            Tuple(_) => RawPrestoTy::Row,
            Row(_) => RawPrestoTy::Row,
            Array(_) => RawPrestoTy::Array,
            Map(_, _) => RawPrestoTy::Map,
        }
    }
}
