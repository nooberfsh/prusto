use serde::Deserialize;

use super::*;
use crate::types::{DataSet, Presto};

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult<T: Presto> {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,

    #[serde(flatten)]
    #[serde(bound(deserialize = "Option<DataSet<T>>: Deserialize<'de>"))]
    pub data_set: Option<DataSet<T>>,
    pub error: Option<QueryError>,

    pub stats: Stat,
    pub warnings: Vec<Warning>,

    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}
