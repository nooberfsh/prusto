use serde::ser::{SerializeMap, SerializeSeq};
use serde::{Serialize, Serializer};

// serialize iterator
// https://github.com/serde-rs/serde/issues/571#issuecomment-252004224
pub struct SerializeIterator<T: Serialize, I: Iterator<Item = T> + Clone> {
    pub iter: I,
    pub size: Option<usize>,
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

pub struct SerializePairIterator<K: Serialize, V: Serialize, I: Iterator<Item = (K, V)> + Clone> {
    pub iter: I,
    pub size: Option<usize>,
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

pub struct SerializeVecMap<K: Serialize, V: Serialize> {
    pub iter: Vec<(K, V)>,
}

impl<K, V> Serialize for SerializeVecMap<K, V>
where
    K: Serialize,
    V: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut s = serializer.serialize_map(Some(self.iter.len()))?;
        for (k, v) in &self.iter {
            s.serialize_entry(&k, &v)?;
        }
        s.end()
    }
}
