pub const DEFAULT_PORT: u16 = 8080;
pub const DEFAULT_MAX_ATTEMPTS: u32 = 3;
pub const DEFAULT_REQUEST_TIMEOUT: f32 = 30.0; // type: float

pub static DEFAULT_SOURCE: &str = "presto-python-client";
pub static DEFAULT_CATALOG: &str = "";
pub static DEFAULT_SCHEMA: &str = "";
pub static DEFAULT_AUTH: &str = ""; //TODO

pub static URL_STATEMENT_PATH: &str = "/v1/statement";

pub static HEADER_PREFIX: &str = "X-Presto-";
pub static HEADER_CATALOG: &str = "X-Presto-Catalog";
pub static HEADER_SCHEMA: &str = "X-Presto-Schema";
pub static HEADER_SOURCE: &str = "X-Presto-Source";
pub static HEADER_USER: &str = "X-Presto-User";

pub static HEADER_SESSION: &str = "X-Presto-Session";
pub static HEADER_SET_SESSION: &str = "X-Presto-Set-Session";
pub static HEADER_CLEAR_SESSION: &str = "X-Presto-Clear-Session";

pub static HEADER_STARTED_TRANSACTION: &str = "X-Presto-Started-Transaction-Id";
pub static HEADER_TRANSACTION: &str = "X-Presto-Transaction-Id";
