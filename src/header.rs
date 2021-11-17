// request headers
pub static HEADER_USER: &str = "X-Presto-User";
pub static HEADER_SOURCE: &str = "X-Presto-Source";
pub static HEADER_CATALOG: &str = "X-Presto-Catalog";
pub static HEADER_SCHEMA: &str = "X-Presto-Schema";
pub static HEADER_PATH: &str = "X-Presto-Path";
pub static HEADER_TIME_ZONE: &str = "X-Presto-Time-Zone";
#[allow(dead_code)]
pub static HEADER_LANGUAGE: &str = "X-Presto-Language";
pub static HEADER_TRACE_TOKEN: &str = "X-Presto-Trace-Token";
pub static HEADER_SESSION: &str = "X-Presto-Session";
pub static HEADER_ROLE: &str = "X-Presto-Role";
pub static HEADER_PREPARED_STATEMENT: &str = "X-Presto-Prepared-Statement";
pub static HEADER_TRANSACTION: &str = "X-Presto-Transaction-Id";
pub static HEADER_CLIENT_INFO: &str = "X-Presto-Client-Info";
pub static HEADER_CLIENT_TAGS: &str = "X-Presto-Client-Tags";
pub static HEADER_CLIENT_CAPABILITIES: &str = "X-Presto-Client-Capabilities";
pub static HEADER_RESOURCE_ESTIMATE: &str = "X-Presto-Resource-Estimate";
pub static HEADER_EXTRA_CREDENTIAL: &str = "X-Presto-Extra-Credential";

// response headers
pub static HEADER_SET_CATALOG: &str = "X-Presto-Set-Catalog";
pub static HEADER_SET_SCHEMA: &str = "X-Presto-Set-Schema";
pub static HEADER_SET_PATH: &str = "X-Presto-Set-Path";
pub static HEADER_SET_SESSION: &str = "X-Presto-Set-Session";
pub static HEADER_CLEAR_SESSION: &str = "X-Presto-Clear-Session";
pub static HEADER_SET_ROLE: &str = "X-Presto-Set-Role";
pub static HEADER_ADDED_PREPARE: &str = "X-Presto-Added-Prepare";
pub static HEADER_DEALLOCATED_PREPARE: &str = "X-Presto-Deallocated-Prepare";
pub static HEADER_STARTED_TRANSACTION_ID: &str = "X-Presto-Started-Transaction-Id";
pub static HEADER_CLEAR_TRANSACTION_ID: &str = "X-Presto-Clear-Transaction-Id";
