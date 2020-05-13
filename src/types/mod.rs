mod boolean;
mod data_set;
mod decimal;
mod float;
mod integer;
mod map;
mod option;
mod seq;
mod string;
pub(self) mod util;

pub use boolean::*;
pub use data_set::*;
pub use decimal::*;
pub use float::*;
pub use integer::*;
pub use integer::*;
pub use map::*;
pub use option::*;
pub use seq::*;
pub use string::*;

//mod str;
//pub use self::str::*;

use std::borrow::Cow;
use std::collections::HashMap;
use std::sync::Arc;

use derive_more::Display;
use itertools::Itertools;
use serde::de::DeserializeSeed;
use serde::Serialize;

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
    ParseFailed(String),
}

pub trait Presto {
    type ValueType<'a>: Serialize;
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
        let mut data = HashMap::new();

        if extract(&target, provided, &mut data) {
            Ok(Context {
                ty: provided,
                map: Arc::new(data),
            })
        } else {
            Err(Error::InvalidPrestoType)
        }
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

fn extract(target: &PrestoTy, provided: &PrestoTy, data: &mut HashMap<usize, Vec<usize>>) -> bool {
    use PrestoTy::*;

    match (target, provided) {
        (Decimal(p1, s1), Decimal(p2, s2)) if p1 == p2 && s1 == s2 => true,
        (Option(ty), provided) => extract(ty, provided, data),
        (Boolean, Boolean) => true,
        (PrestoInt(_), PrestoInt(_)) => true,
        (PrestoFloat(_), PrestoFloat(_)) => true,
        (Varchar, Varchar) => true,
        (Tuple(t1), Tuple(t2)) => {
            if t1.len() != t2.len() {
                false
            } else {
                t1.iter().zip(t2.iter()).all(|(l, r)| extract(l, r, data))
            }
        }
        (Row(t1), Row(t2)) => {
            if t1.len() != t2.len() {
                false
            } else {
                // create a vector of the original element's reference
                let mut t1k: Vec<_> = t1.iter().collect();
                t1k.sort_by(|t1, t2| t1.0.cmp(&t2.0));
                let mut t2k: Vec<_> = t2.iter().collect();
                t2k.sort_by(|t1, t2| t1.0.cmp(&t2.0));

                let ret = t1k
                    .iter()
                    .zip(t2k.iter())
                    .all(|(l, r)| l.0 == r.0 && extract(&l.1, &r.1, data));
                if !ret {
                    return ret;
                }

                let mut map = Vec::with_capacity(t2.len());
                for (provided, _) in t2 {
                    for (i, (target, _)) in t1.iter().enumerate() {
                        if provided == target {
                            map.push(i);
                            break;
                        }
                    }
                }
                assert_eq!(map.len(), t2.len());

                let key = provided as *const PrestoTy as usize;
                let prev = data.insert(key, map);
                assert!(prev.is_none());

                true
            }
        }
        (Array(t1), Array(t2)) => extract(t1, t2, data),
        (Map(t1k, t1v), Map(t2k, t2v)) => extract(t1k, t2k, data) && extract(t1v, t2v, data),
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub enum PrestoTy {
    Option(Box<PrestoTy>),
    Boolean,
    PrestoInt(PrestoInt),
    PrestoFloat(PrestoFloat),
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
    Decimal(usize, usize),
}

#[derive(Clone, Debug)]
pub enum PrestoInt {
    I8,
    I16,
    I32,
    I64,
    U8,
    U16,
    U32,
    U64,
}

#[derive(Clone, Debug)]
pub enum PrestoFloat {
    F32,
    F64,
}

impl PrestoTy {
    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        use PrestoFloat::*;
        use PrestoInt::*;

        let ty = match sig.raw_type {
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
            Decimal(p, s) => vec![
                ClientTypeSignatureParameter::LongLiteral(p as u64),
                ClientTypeSignatureParameter::LongLiteral(s as u64),
            ],
            Option(t) => return t.into_type_signature(),
            Boolean => vec![],
            PrestoInt(_) => vec![],
            PrestoFloat(_) => vec![],
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
            Decimal(p, s) => format!("{}({},{})", RawPrestoTy::Decimal.to_str(), p, s).into(),
            Option(t) => t.full_type(),
            Boolean => RawPrestoTy::Boolean.to_str().into(),
            PrestoInt(ty) => ty.raw_type().to_str().into(),
            PrestoFloat(ty) => ty.raw_type().to_str().into(),
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
            Decimal(_, _) => RawPrestoTy::Decimal,
            Option(ty) => ty.raw_type(),
            Boolean => RawPrestoTy::Boolean,
            PrestoInt(ty) => ty.raw_type(),
            PrestoFloat(ty) => ty.raw_type(),
            Varchar => RawPrestoTy::VarChar,
            Tuple(_) => RawPrestoTy::Row,
            Row(_) => RawPrestoTy::Row,
            Array(_) => RawPrestoTy::Array,
            Map(_, _) => RawPrestoTy::Map,
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
            U8 => RawPrestoTy::TinyInt,
            U16 => RawPrestoTy::SmallInt,
            U32 => RawPrestoTy::Integer,
            U64 => RawPrestoTy::BigInt,
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
