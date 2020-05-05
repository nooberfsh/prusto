use std::collections::HashMap;
use std::hash::Hash;

use serde::ser::{self, Serialize, SerializeSeq, SerializeStruct, Serializer};

use crate::models;

pub struct Error;

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
    pub fn type_signature(&self) -> models::TypeSignature {
        todo!()
    }

    pub fn full_type(&self) -> String {
        todo!()
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
    type ValueType<'a> = Vec<T::ValueType<'a>>;

    fn value(&self) -> Self::ValueType<'_> {
        self.iter().map(|t| t.value()).collect()
    }

    fn ty() -> PrestoTy {
        PrestoTy::Array(Box::new(T::ty()))
    }
}

impl<K: Presto + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
    type ValueType<'a> = Vec<(K::ValueType<'a>, V::ValueType<'a>)>;

    fn value(&self) -> Self::ValueType<'_> {
        self.iter()
            .map(|(k, v)| (k.value(), v.value()))
            .collect()
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
                        ty: ty.full_type(),
                        type_signature: Some(ty.type_signature()),
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
        let data = { todo!() };
        state.serialize_field("columns", &columns)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}
