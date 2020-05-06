use std::borrow::Cow;
use std::collections::HashMap;
use std::hash::Hash;

use itertools::Itertools;
use serde::ser::{self, Serialize, SerializeMap, SerializeSeq, SerializeStruct, Serializer};

use crate::{
    models, ClientTypeSignatureParameter, NamedTypeSignature, RowFieldName, TypeSignature,
};

pub trait Presto {
    type ValueType<'a>: Serialize;

    fn value(&self) -> Self::ValueType<'_>;
    fn ty() -> PrestoTy;
}

// TODO: can avoid alloc? use something like &'static PresotTy
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoTy {
    Integer,
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
}

impl PrestoTy {
    pub fn into_type_signature(self) -> models::TypeSignature {
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
            Integer => models::RawPrestoTy::Integer.to_str().into(),
            Varchar => models::RawPrestoTy::VarChar.to_str().into(),
            Tuple(ts) => format!(
                "{}({})",
                models::RawPrestoTy::Row.to_str(),
                ts.iter().map(|ty| ty.full_type()).join(",")
            )
            .into(),
            Row(ts) => format!(
                "{}({})",
                models::RawPrestoTy::Row.to_str(),
                ts.iter()
                    .map(|(name, ty)| format!("{} {}", name, ty.full_type()))
                    .join(",")
            )
            .into(),
            Array(t) => {
                format!("{}({})", models::RawPrestoTy::Array.to_str(), t.full_type()).into()
            }
            Map(t1, t2) => format!(
                "{}({},{})",
                models::RawPrestoTy::Map.to_str(),
                t1.full_type(),
                t2.full_type()
            )
            .into(),
        }
    }

    pub fn raw_type(&self) -> models::RawPrestoTy {
        use PrestoTy::*;

        match self {
            Integer => models::RawPrestoTy::Integer,
            Varchar => models::RawPrestoTy::VarChar,
            Tuple(_) => models::RawPrestoTy::Row,
            Row(_) => models::RawPrestoTy::Row,
            Array(_) => models::RawPrestoTy::Array,
            Map(_, _) => models::RawPrestoTy::Map,
        }
    }
}

impl Presto for i32 {
    type ValueType<'a> = &'a i32;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> PrestoTy {
        PrestoTy::Integer
    }
}

impl Presto for String {
    type ValueType<'a> = &'a String;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
}

impl<T: Presto> Presto for Vec<T> {
    type ValueType<'a> = impl Serialize;

    fn value(&self) -> Self::ValueType<'_> {
        let iter = self.iter().map(|t| t.value());

        SerializeIterator {
            iter,
            size: Some(self.len()),
        }
    }

    fn ty() -> PrestoTy {
        PrestoTy::Array(Box::new(T::ty()))
    }
}

impl<K: Presto + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
    type ValueType<'a> = impl Serialize;

    fn value(&self) -> Self::ValueType<'_> {
        let iter = self.iter().map(|(k, v)| (k.value(), v.value()));

        SerializePairIterator {
            iter,
            size: Some(self.len()),
        }
    }

    fn ty() -> PrestoTy {
        PrestoTy::Map(Box::new(K::ty()), Box::new(V::ty()))
    }
}

pub struct DataSet<T: Presto> {
    data: Vec<T>,
}

impl<T: Presto> Serialize for DataSet<T> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        use PrestoTy::*;
        let mut state = serializer.serialize_struct("DataSet", 2)?;

        let columns = match T::ty() {
            Row(r) if !r.is_empty() => {
                let mut ret = vec![];
                for (name, ty) in r {
                    let column = models::Column {
                        name,
                        ty: ty.full_type().into_owned(),
                        type_signature: Some(ty.into_type_signature()),
                    };
                    ret.push(column);
                }
            }
            _ => {
                return Err(ser::Error::custom(format!(
                    "only row type can be serialized"
                )))
            }
        };
        let data = SerializeIterator {
            iter: self.data.iter().map(|d| d.value()),
            size: Some(self.data.len()),
        };
        state.serialize_field("columns", &columns)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

///////////////////////////////////////////////////////////////////////////////////

// serialize iterator
// https://github.com/serde-rs/serde/issues/571#issuecomment-252004224
struct SerializeIterator<T: Serialize, I: Iterator<Item = T> + Clone> {
    iter: I,
    size: Option<usize>,
}

impl<T, I> Serialize for SerializeIterator<T, I>
where
    I: Iterator<Item = T> + Clone,
    T: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_seq(self.size)?;
        for e in self.iter.clone() {
            s.serialize_element(&e)?;
        }
        s.end()
    }
}

struct SerializePairIterator<K: Serialize, V: Serialize, I: Iterator<Item = (K, V)> + Clone> {
    iter: I,
    size: Option<usize>,
}

impl<K, V, I> Serialize for SerializePairIterator<K, V, I>
where
    K: Serialize,
    V: Serialize,
    I: Iterator<Item = (K, V)> + Clone,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_map(self.size)?;
        for (k, v) in self.iter.clone() {
            s.serialize_entry(&k, &v)?;
        }
        s.end()
    }
}
