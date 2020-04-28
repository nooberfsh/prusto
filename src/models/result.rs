use serde::Deserialize;
use serde_json::Value;

use super::*;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryResult {
    pub id: String,
    pub info_uri: String,
    pub partial_cancel_uri: Option<String>,
    pub next_uri: Option<String>,
    pub columns: Option<Vec<Column>>,
    pub data: Option<Vec<Vec<Value>>>,
    pub stats: Stat,
    pub error: Option<QueryError>,
    pub warnings: Vec<Warning>,
    pub update_type: Option<String>,
    pub update_count: Option<u64>,
}