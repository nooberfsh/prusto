mod boolean;
mod data_set;
mod map;
mod number;
mod option;
mod string;
pub(self) mod util;
mod vec;

pub use boolean::*;
pub use map::*;
pub use number::*;
pub use number::*;
pub use option::*;
pub use string::*;
pub use vec::*;

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
        (Option(ty), provided) => extract(ty, provided, data),
        (Integer, Integer) => true,
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
        (Map(t1k, t1v), Map(t2k, t2v)) => extract(t1k, t2k, data) && extract(t1v, t2v, data),
        _ => false,
    }
}

#[derive(Clone, Debug)]
pub enum PrestoTy {
    Option(Box<PrestoTy>),
    Boolean,
    Integer,
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
}

impl PrestoTy {
    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        let ty = match sig.raw_type {
            RawPrestoTy::Boolean => PrestoTy::Boolean,
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
            Option(t) => vec![ClientTypeSignatureParameter::TypeSignature(
                t.into_type_signature(),
            )],
            Boolean => vec![],
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
            Option(t) => format!("{}({})", RawPrestoTy::Array.to_str(), t.full_type()).into(),
            Boolean => RawPrestoTy::Boolean.to_str().into(),
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
            Option(ty) => ty.raw_type(),
            Boolean => RawPrestoTy::Boolean,
            Integer => RawPrestoTy::Integer,
            Varchar => RawPrestoTy::VarChar,
            Tuple(_) => RawPrestoTy::Row,
            Row(_) => RawPrestoTy::Row,
            Array(_) => RawPrestoTy::Array,
            Map(_, _) => RawPrestoTy::Map,
        }
    }
}
