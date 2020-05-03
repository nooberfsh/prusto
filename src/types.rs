use std::collections::HashMap;
use std::hash::Hash;

use serde::ser::{self, SerializeStruct, Serialize, Serializer, SerializeSeq};
use serde_json::Value;

use crate::models;

pub struct Error;

pub trait Presto {
    type ValueType<'a>: Serialize;

    fn ty() -> PrestoTy;
    fn parse(ty: &PrestoTy, v: Value)  -> Result<Self, Error> where Self: Sized;
    fn deserialize_as_map_key(ty: &PrestoTy, s: &str) -> Result<Self, Error> where Self: Sized {
        Err(Error)
    }
    fn serialize_as_map_key(&self) -> Result<String, Error> where Self: Sized {
        Err(Error)
    }

    fn value_type(& self) -> Self::ValueType<'_>;
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PrestoTy {
    Integer,
    Varchar,
    Tuple(Vec<PrestoTy>),
    Row(Vec<(String, PrestoTy)>),
    Array(Box<PrestoTy>),
    Map(Box<PrestoTy>,Box<PrestoTy>),
}

impl PrestoTy {
    fn type_signature(&self) -> models::TypeSignature {
        todo!()
    }

    fn full_type(&self) -> String {
        todo!()
    }

    fn raw_type(&self) -> models::PrestoTy {
        use PrestoTy::*;

        match self {
            Integer => models::PrestoTy::INTEGER,
            Varchar => models::PrestoTy::VARCHAR,
            Tuple(_) => models::PrestoTy::ROW,
            Row(_) => models::PrestoTy::ROW,
            Array(_) => models::PrestoTy::ARRAY,
            Map(_, _) => models::PrestoTy::MAP,
        }
    }
}

impl Presto for i32 {
    type ValueType<'a> = &'a i32;

    fn value_type(&self) -> Self::ValueType<'_> {self}

    fn ty() -> PrestoTy {PrestoTy::Integer}
    fn parse(ty: &PrestoTy, v: Value)  -> Result<Self, Error> {
        match (ty, v) {
            (PrestoTy::Integer, Value::Number(n))  if n.is_i64() => Ok(n.as_i64().unwrap() as i32),
            _ => Err(Error)
        }
    }
}

impl Presto for String {
    type ValueType<'a> = &'a String;

    fn value_type(&self) -> Self::ValueType<'_> {self}
    fn ty() -> PrestoTy{PrestoTy::Varchar}
    fn parse(ty: &PrestoTy, v: Value)  -> Result<Self, Error> {
        match (ty, v) {
            (PrestoTy::Varchar, Value::String(s))  => Ok(s),
            _ => Err(Error)
        }
    }
}

impl<T: Presto> Presto for Vec<T> {
    type ValueType<'a> = Vec<T::ValueType<'a>>;

    fn value_type(&self) -> Self::ValueType<'_> {
        self.iter().map(|t| t.value_type()).collect()
    }

    fn ty() -> PrestoTy{PrestoTy::Array(Box::new(T::ty()))}
    fn parse(ty: &PrestoTy, v: Value)  -> Result<Self, Error> {
        if let PrestoTy::Array(ty) = ty {
            if let Value::Array(values)  = v {
                let mut ret = Vec::with_capacity(values.len());
                for v in values {
                    let t = T::parse(&ty, v)?;
                    ret.push(t);
                }
                return Ok(ret)
            }
        }
        Err(Error)
    }
}

impl<K: Presto + Eq + Hash, V: Presto> Presto for HashMap<K, V> {
    type ValueType<'a> = Vec<(K::ValueType<'a>, V::ValueType<'a>)>;

    fn value_type(&self) -> Self::ValueType<'_> {
        self.iter().map(|(k ,v)| (k.value_type(), v.value_type())).collect()
    }

    fn ty() -> PrestoTy {
        PrestoTy::Map(Box::new(K::ty()), Box::new(V::ty()))
    }

    fn parse(ty: &PrestoTy, v: Value)  -> Result<Self, Error> {
        if let PrestoTy::Map(t1, t2) = ty {
            if let Value::Object(o)  = v {
                let mut ret = HashMap::new();
                for (k, v) in o {
                    let k = K::deserialize_as_map_key(&t1, &k)?;
                    let v = V::parse(&t2, v)?;
                    ret.insert(k, v);
                }
                return Ok(ret)
            }
        }
        Err(Error)
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

        let columns = match T::ty()  {
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
            _ => return Err(ser::Error::custom(format!("only row type can be serialized"))),
        };
        let data = {
          todo!()
        };
        state.serialize_field("columns", &columns)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}
