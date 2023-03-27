use std::fmt;
use std::marker::PhantomData;

use iterable::Iterable;
use serde::de::{self, Deserializer, MapAccess, Visitor};
use serde::ser::{SerializeStruct, Serializer};
use serde::{Deserialize, Serialize};

use super::util::SerializeIterator;
use super::{Context, Error, Presto, PrestoTy, VecSeed};
use crate::models::Column;
use crate::types::Row;

#[derive(Debug)]
pub struct DataSet<T: Presto> {
    types: Vec<(String, PrestoTy)>,
    data: Vec<T>,
}

impl<T: Presto> DataSet<T> {
    pub fn new(data: Vec<T>) -> Result<Self, Error> {
        let types = match T::ty() {
            PrestoTy::Row(r) => {
                if r.is_empty() {
                    return Err(Error::EmptyInPrestoRow);
                } else {
                    r
                }
            }
            _ => return Err(Error::NonePrestoRow),
        };

        Ok(DataSet { types, data })
    }

    pub fn split(self) -> (Vec<(String, PrestoTy)>, Vec<T>) {
        (self.types, self.data)
    }

    pub fn into_vec(self) -> Vec<T> {
        self.data
    }

    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    pub fn len(&self) -> usize {
        self.data.len()
    }

    pub fn as_slice(&self) -> &[T] {
        self.data.as_slice()
    }

    pub fn merge(&mut self, other: DataSet<T>) {
        self.data.extend(other.data)
    }
}

impl DataSet<Row> {
    pub fn new_row(types: Vec<(String, PrestoTy)>, data: Vec<Row>) -> Result<Self, Error> {
        if types.is_empty() {
            return Err(Error::EmptyInPrestoRow);
        }
        Ok(DataSet { types, data })
    }
}

impl<T: Presto + Clone> Clone for DataSet<T> {
    fn clone(&self) -> Self {
        DataSet {
            types: self.types.clone(),
            data: self.data.clone(),
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
        let mut state = serializer.serialize_struct("DataSet", 2)?;

        let columns = self.types.clone().map(|(name, ty)| Column {
            name,
            ty: ty.full_type().into_owned(),
            type_signature: Some(ty.into_type_signature()),
        });

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
// Deserialize

#[derive(Deserialize)]
#[serde(field_identifier, rename_all = "lowercase")]
enum Field {
    Columns,
    Data,
}

const FIELDS: &[&str] = &["columns", "data"];

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
                let types = if let Some(Field::Columns) = map.next_key()? {
                    let columns: Vec<Column> = map.next_value()?;
                    columns.try_map(PrestoTy::from_column).map_err(|e| {
                        de::Error::custom(format!("deserialize presto type failed, reason: {}", e))
                    })?
                } else {
                    return Err(de::Error::missing_field("columns"));
                };

                let array_ty = PrestoTy::Array(Box::new(PrestoTy::Row(types.clone())));
                let ctx = Context::new::<Vec<T>>(&array_ty).map_err(|e| {
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

                if let PrestoTy::Unknown = T::ty() {
                    Ok(DataSet { types, data })
                } else {
                    DataSet::new(data).map_err(|e| {
                        de::Error::custom(format!("construct data failed, reason: {}", e))
                    })
                }
            }
        }

        deserializer.deserialize_struct("DataSet", FIELDS, DataSetVisitor(PhantomData))
    }
}
