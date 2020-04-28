use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub message: String,
    pub sql_state: String,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    pub error_location: ErrorLocation,
    pub failure_info: FailureInfo,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ErrorLocation {
    pub line_number: u32,
    pub column_number: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FailureInfo {
    #[serde(rename = "type")]
    pub ty: String,
    pub suppressed: Vec<FailureInfo>,
    pub stack: Vec<String>,
    pub message: Option<String>,
    pub cause: Option<Box<FailureInfo>>,
    pub error_location: Option<ErrorLocation>,
}
