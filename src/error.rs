use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    InvalidCatalog,
    InvalidSchema,
    InvalidSource,
    InvalidUser,
    InvalidProperties,
    DuplicateHeader,
    EmptyAuth,
    BasicAuthWithHttp,
    #[error("http error, reason: {0}")]
    HttpError(#[from], reqwest::Error),
}

pub type Result<T> = std::result::Result<T, Error>;
