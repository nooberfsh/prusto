use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use derive_more::Display;
use itertools::Itertools;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, Visitor};
use serde::ser::{self, SerializeMap, SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use crate::{
    models, ClientTypeSignatureParameter, Column, NamedTypeSignature, RowFieldName, TypeSignature,
};

pub trait Presto {
    type ValueType<'a>: Serialize;

    fn value(&self) -> Self::ValueType<'_>;
    fn ty() -> PrestoTy;
}

pub trait PrestoMapKey: Presto {}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoTy {
    Integer,
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>, Box<PrestoTy>),
}

#[derive(Display)]
pub struct FromSigError;

impl PrestoTy {
    pub fn from_type_signature(sig: TypeSignature) -> Result<Self, FromSigError> {
        todo!()
    }

    pub fn from_columns(columns: Vec<Column>) -> Result<Self, FromSigError> {
        let mut ret = Vec::with_capacity(columns.len());
        for column in columns {
            if let Some(sig) = column.type_signature {
                let ty = Self::from_type_signature(sig)?;
                ret.push((column.name, ty));
            } else {
                return Err(FromSigError);
            }
        }

        Ok(PrestoTy::Row(ret))
    }

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
impl PrestoMapKey for i32 {}

impl Presto for String {
    type ValueType<'a> = &'a String;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
}
impl PrestoMapKey for String {}

impl<'b> Presto for &'b str {
    type ValueType<'a> = &'a str;

    fn value(&self) -> Self::ValueType<'_> {
        *self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
}
impl<'b> PrestoMapKey for &'b str {}

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

impl<K: PrestoMapKey + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
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
                ret
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

impl<'de, T: Presto> Deserialize<'de> for DataSet<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(field_identifier, rename_all = "lowercase")]
        enum Field {
            Columns,
            Data,
        }

        struct DataSetVisitor<T: Presto>(PhantomData<T>);

        impl<'de, T: Presto> Visitor<'de> for DataSetVisitor<T> {
            type Value = DataSet<T>;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct DataSet")
            }

            fn visit_map<V>(self, mut map: V) -> Result<DataSet<T>, V::Error>
            where
                V: MapAccess<'de>,
            {
                let ty = if let Some(Field::Columns) = map.next_key()? {
                    let columns: Vec<Column> = map.next_value()?;
                    PrestoTy::from_columns(columns).map_err(|e| {
                        de::Error::custom(format!("deserialize presto type failed, reason: {}", e))
                    })?
                } else {
                    return Err(de::Error::missing_field("columns"));
                };

                if ty != T::ty() {
                    return Err(de::Error::custom(format!("presto type does not match")));
                }

                let data = if let Some(Field::Data) = map.next_key()? {
                    let seed = WrapData {
                        ty: &ty,
                        _marker: PhantomData,
                    };
                    map.next_value_seed(seed)?
                } else {
                    return Err(de::Error::missing_field("data"));
                };

                match map.next_key::<Field>()? {
                    Some(Field::Columns) => return Err(de::Error::duplicate_field("columns")),
                    Some(Field::Data) => return Err(de::Error::duplicate_field("data")),
                    None => {}
                }

                Ok(DataSet { data })
            }
        }

        const FIELDS: &'static [&'static str] = &["columns", "data"];
        deserializer.deserialize_struct("DataSet", FIELDS, DataSetVisitor(PhantomData))
    }
}

struct WrapData<'a, T: Presto> {
    ty: &'a PrestoTy,
    _marker: PhantomData<Vec<T>>,
}

impl<'a, 'de, T: Presto> DeserializeSeed<'de> for WrapData<'a, T> {
    type Value = Vec<T>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        todo!()
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
