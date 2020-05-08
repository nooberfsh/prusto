use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt;
use std::hash::Hash;
use std::marker::PhantomData;

use derive_more::Display;
use itertools::Itertools;
use serde::de::{self, DeserializeSeed, Deserializer, MapAccess, SeqAccess, Visitor};
use serde::ser::{self, SerializeMap, SerializeSeq, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use crate::{
    models, ClientTypeSignatureParameter, Column, NamedTypeSignature, RowFieldName, TypeSignature,
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
    pub fn from_type_signature(mut sig: TypeSignature) -> Result<Self, Error> {
        let ty = match sig.raw_type {
            models::RawPrestoTy::Integer => PrestoTy::Integer,
            models::RawPrestoTy::VarChar => PrestoTy::Varchar,
            models::RawPrestoTy::Array if sig.arguments.len() == 1 => {
                let sig = sig.arguments.pop().unwrap();
                if let ClientTypeSignatureParameter::TypeSignature(sig) = sig {
                    let inner = Self::from_type_signature(sig)?;
                    PrestoTy::Array(Box::new(inner))
                } else {
                    return Err(Error::InvalidTypeSignature);
                }
            }
            models::RawPrestoTy::Map if sig.arguments.len() == 2 => {
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
            models::RawPrestoTy::Row if !sig.arguments.is_empty() => {
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
    type Seed<'a, 'de> = I32Seed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }

    fn ty() -> PrestoTy {
        PrestoTy::Integer
    }

    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(I32Seed)
    }
}
impl PrestoMapKey for i32 {}
pub struct I32Seed;
impl<'de> Visitor<'de> for I32Seed {
    type Value = i32;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("i32 seed")
    }

    fn visit_i32<E>(self, value: i32) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(value)
    }
}
impl<'de> DeserializeSeed<'de> for I32Seed {
    type Value = i32;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_i32(self)
    }
}

impl Presto for String {
    type ValueType<'a> = &'a String;
    type Seed<'a, 'de> = StringSeed;

    fn value(&self) -> Self::ValueType<'_> {
        self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }
    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(StringSeed)
    }
}
impl PrestoMapKey for String {}
pub struct StringSeed;
impl<'de> Visitor<'de> for StringSeed {
    type Value = String;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("string seed")
    }
    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}
impl<'de> DeserializeSeed<'de> for StringSeed {
    type Value = String;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_string(self)
    }
}

impl<'b> Presto for &'b str {
    type ValueType<'a> = &'a str;
    type Seed<'a, 'de> = StrSeed;

    fn value(&self) -> Self::ValueType<'_> {
        *self
    }
    fn ty() -> PrestoTy {
        PrestoTy::Varchar
    }

    fn seed<'a, 'de>(_ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        Ok(StrSeed)
    }
}
impl<'b> PrestoMapKey for &'b str {}
pub struct StrSeed;
impl<'de> Visitor<'de> for StrSeed {
    type Value = &'de str;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("&str seed")
    }
    fn visit_borrowed_str<E>(self, v: &'de str) -> Result<Self::Value, E>
    where
        E: de::Error,
    {
        Ok(v)
    }
}
impl<'de> DeserializeSeed<'de> for StrSeed {
    type Value = &'de str;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(self)
    }
}

impl<T: Presto> Presto for Vec<T> {
    type ValueType<'a> = impl Serialize;
    type Seed<'a, 'de> = VecSeed<'a, T>;

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

    fn seed<'a, 'de>(ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        if let PrestoTy::Array(ty) = ty {
            Ok(VecSeed(ty, PhantomData))
        } else {
            Err(Error::InvalidPrestoType)
        }
    }
}
pub struct VecSeed<'a, T>(&'a PrestoTy, PhantomData<T>);
impl<'a, 'de, T: Presto> Visitor<'de> for VecSeed<'a, T> {
    type Value = Vec<T>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("vec seed")
    }
    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: SeqAccess<'de>,
    {
        let mut ret = vec![];
        while let Some(d) = seq.next_element_seed(
            T::seed(self.0).map_err(|e| <A::Error as de::Error>::custom(format!("{}", e)))?,
        )? {
            ret.push(d)
        }
        Ok(ret)
    }
}
impl<'a, 'de, T: Presto> DeserializeSeed<'de> for VecSeed<'a, T> {
    type Value = Vec<T>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_seq(self)
    }
}

impl<K: PrestoMapKey + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
    type ValueType<'a> = impl Serialize;
    type Seed<'a, 'de> = MapSeed<'a, K, V>;

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

    fn seed<'a, 'de>(ty: &'a PrestoTy) -> Result<Self::Seed<'a, 'de>, Error> {
        if let PrestoTy::Map(t1, t2) = ty {
            Ok(MapSeed(t1, t2, PhantomData))
        } else {
            Err(Error::InvalidPrestoType)
        }
    }
}

pub struct MapSeed<'a, K, V>(&'a PrestoTy, &'a PrestoTy, PhantomData<(K, V)>);
impl<'a, 'de, K: PrestoMapKey + Eq + Hash, V: Presto> Visitor<'de> for MapSeed<'a, K, V> {
    type Value = HashMap<K, V>;
    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("hash map seed")
    }
    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: MapAccess<'de>,
    {
        let mut ret = HashMap::new();
        while let Some((k, v)) = map.next_entry_seed(
            K::seed(self.0).map_err(|e| <A::Error as de::Error>::custom(format!("{}", e)))?,
            V::seed(self.1).map_err(|e| <A::Error as de::Error>::custom(format!("{}", e)))?,
        )? {
            ret.insert(k, v);
        }
        Ok(ret)
    }
}
impl<'a, 'de, K: PrestoMapKey + Eq + Hash, V: Presto> DeserializeSeed<'de> for MapSeed<'a, K, V> {
    type Value = HashMap<K, V>;
    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_map(self)
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

                let ty = PrestoTy::Array(Box::new(ty));

                let data = if let Some(Field::Data) = map.next_key()? {
                    let seed = VecSeed(&ty, PhantomData);
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
