// request headers
pub static HEADER_USER: &str = "X-Trino-User";
pub static HEADER_SOURCE: &str = "X-Trino-Source";
pub static HEADER_CATALOG: &str = "X-Trino-Catalog";
pub static HEADER_SCHEMA: &str = "X-Trino-Schema";
pub static HEADER_PATH: &str = "X-Trino-Path";
pub static HEADER_TIME_ZONE: &str = "X-Trino-Time-Zone";
pub static HEADER_LANGUAGE: &str = "X-Trino-Language";
pub static HEADER_TRACE_TOKEN: &str = "X-Trino-Trace-Token";
pub static HEADER_SESSION: &str = "X-Trino-Session";
pub static HEADER_ROLE: &str = "X-Trino-Role";
pub static HEADER_PREPARED_STATEMENT: &str = "X-Trino-Prepared-Statement";
pub static HEADER_TRANSACTION: &str = "X-Trino-Transaction-Id";
pub static HEADER_CLIENT_INFO: &str = "X-Trino-Client-Info";
pub static HEADER_CLIENT_TAGS: &str = "X-Trino-Client-Tags";
pub static HEADER_CLIENT_CAPABILITIES: &str = "X-Trino-Client-Capabilities";
pub static HEADER_RESOURCE_ESTIMATE: &str = "X-Trino-Resource-Estimate";
pub static HEADER_EXTRA_CREDENTIAL: &str = "X-Trino-Extra-Credential";

// response headers
pub static HEADER_SET_CATALOG: &str = "X-Trino-Set-Catalog";
pub static HEADER_SET_SCHEMA: &str = "X-Trino-Set-Schema";
pub static HEADER_SET_PATH: &str = "X-Trino-Set-Path";
#[allow(unused)]
pub static HEADER_SET_SESSION: &str = "X-Trino-Set-Session";
#[allow(unused)]
pub static HEADER_CLEAR_SESSION: &str = "X-Trino-Clear-Session";
pub static HEADER_SET_ROLE: &str = "X-Trino-Set-Role";
pub static HEADER_ADDED_PREPARE: &str = "X-Trino-Added-Prepare";
pub static HEADER_DEALLOCATED_PREPARE: &str = "X-Trino-Deallocated-Prepare";
#[allow(unused)]
pub static HEADER_STARTED_TRANSACTION_ID: &str = "X-Trino-Started-Transaction-Id";
pub static HEADER_CLEAR_TRANSACTION_ID: &str = "X-Trino-Clear-Transaction-Id";
