use reqwest::header::HeaderName;
use reqwest::StatusCode;
use thiserror::Error;

use crate::models::QueryError;

#[derive(Error, Debug)]
pub enum Error {
    #[error("invalid catalog")]
    InvalidCatalog,
    #[error("invalid schema")]
    InvalidSchema,
    #[error("invalid source")]
    InvalidSource,
    #[error("invalid user")]
    InvalidUser,
    #[error("invalid properties")]
    InvalidProperties,
    #[error("duplicate header")]
    DuplicateHeader(HeaderName),
    #[error("invalid empty auth")]
    EmptyAuth,
    #[error("basic auth can not be used with http")]
    BasicAuthWithHttp,
    #[error("http error, reason: {0}")]
    HttpError(#[from] reqwest::Error),
    #[error("http not ok, code: {0}, reason: {1}")]
    HttpNotOk(StatusCode, String),
    #[error("query error, reason: {0}")]
    QueryError(#[from] QueryError),
    #[error("inconsistent data")]
    InconsistentData,
    #[error("empty data")]
    EmptyData,
    #[error("reach max attempt: {0}")]
    ReachMaxAttempt(usize),
    #[error("invalid host: {0}")]
    InvalidHost(String),
}

pub type Result<T> = std::result::Result<T, Error>;
