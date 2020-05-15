use std::fmt;
use std::marker::PhantomData;

use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::{self, SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::util::SerializeIterator;
use super::{Context, Presto, PrestoTy, VecSeed};
use crate::models::Column;

#[derive(Debug)]
pub struct DataSet<T: Presto> {
    data: Vec<T>,
}

// TODO: do we need some basic check when deserialize
#[derive(Debug, Clone)]
pub struct RawDataSet {
    columns: Vec<(String, PrestoTy)>,
    data: Vec<Vec<Value>>,
}

impl<T: Presto> DataSet<T> {
    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn merge(&mut self, other: DataSet<T>) {
        self.data.extend(other.data)
    }
}

impl<T: Presto + Clone> Clone for DataSet<T> {
    fn clone(&self) -> Self {
        DataSet {
            data: self.data.clone(),
        }
    }
}

impl RawDataSet {
    pub fn column_ty(&self, name: &str) -> Option<&PrestoTy> {
        self.columns
            .iter()
            .find_map(|(n, ty)| if n == name { Some(ty) } else { None })
    }

    pub fn columns(&self) -> &[(String, PrestoTy)] {
        &self.columns
    }

    pub fn split(self) -> (PrestoTy, Vec<Vec<Value>>) {
        let ty = PrestoTy::Row(self.columns);
        let data = self.data;
        (ty, data)
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub(crate) fn merge(&mut self, other: RawDataSet) -> bool {
        if self.columns == other.columns {
            self.data.extend(other.data);
            true
        } else {
            false
        }
    }
}

///////////////////////////////////////////////////////////////////////////////////
// Serialize

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
                    let column = Column {
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

impl Serialize for RawDataSet {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("RawDataSet", 2)?;

        let columns: Vec<_> = self
            .columns
            .iter()
            .map(|(k, v)| Column {
                name: k.clone(),
                ty: v.full_type().into_owned(),
                type_signature: Some(v.clone().into_type_signature()),
            })
            .collect();
        let data = &self.data;
        state.serialize_field("columns", &columns)?;
        state.serialize_field("data", &data)?;
        state.end()
    }
}

///////////////////////////////////////////////////////////////////////////////////
// Deserialize

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum Field {
    Columns,
    Data,
}

const FIELDS: &'static [&'static str] = &["columns", "data"];

impl<'de, T: Presto> Deserialize<'de> for DataSet<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
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

                let ty = PrestoTy::Array(Box::new(ty));
                let ctx = Context::new::<Vec<T>>(&ty).map_err(|e| {
                    de::Error::custom(format!("invalid presto type, reason: {}", e))
                })?;
                let seed = VecSeed::new(&ctx);

                let data = if let Some(Field::Data) = map.next_key()? {
                    map.next_value_seed(seed)?
                } else {
                    // it is empty when there is no data
                    vec![]
                };

                match map.next_key::<Field>()? {
                    Some(Field::Columns) => return Err(de::Error::duplicate_field("columns")),
                    Some(Field::Data) => return Err(de::Error::duplicate_field("data")),
                    None => {}
                }

                Ok(DataSet { data })
            }
        }

        deserializer.deserialize_struct("DataSet", FIELDS, DataSetVisitor(PhantomData))
    }
}

impl<'de> Deserialize<'de> for RawDataSet {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct RawDataSetVisitor;

        impl<'de> Visitor<'de> for RawDataSetVisitor {
            type Value = RawDataSet;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("struct RawDataSet")
            }

            fn visit_map<V>(self, mut map: V) -> Result<Self::Value, V::Error>
            where
                V: MapAccess<'de>,
            {
                let columns = if let Some(Field::Columns) = map.next_key()? {
                    let columns: Vec<Column> = map.next_value()?;
                    let mut ret = Vec::with_capacity(columns.len());
                    for col in columns {
                        let p = PrestoTy::from_column(col).map_err(|e| {
                            de::Error::custom(format!(
                                "deserialize presto type failed, reason: {}",
                                e
                            ))
                        })?;
                        ret.push(p);
                    }
                    ret
                } else {
                    return Err(de::Error::missing_field("columns"));
                };

                let data = if let Some(Field::Data) = map.next_key()? {
                    map.next_value()?
                } else {
                    // it is empty when there is no data
                    vec![]
                };

                match map.next_key::<Field>()? {
                    Some(Field::Columns) => return Err(de::Error::duplicate_field("columns")),
                    Some(Field::Data) => return Err(de::Error::duplicate_field("data")),
                    None => {}
                }

                Ok(RawDataSet { columns, data })
            }
        }

        deserializer.deserialize_struct("RawDataSet", FIELDS, RawDataSetVisitor)
    }
}
