use std::collections::{HashMap, HashSet};
use std::time::Duration;

use chrono_tz::Tz;
use http::uri::Scheme;
use reqwest::Url;

use crate::error::*;
use crate::selected_role::SelectedRole;
use crate::transaction::TransactionId;

// TODO remove this when `std::default::default` is stabilized
fn default<T: Default>() -> T {
    Default::default()
}

#[derive(Debug)]
pub struct Session {
    pub url: Url,
    pub user: String,
    pub source: String,
    pub trace_token: Option<String>,
    pub client_tags: HashSet<String>,
    pub client_info: Option<String>,
    pub catalog: Option<String>,
    pub schema: Option<String>,
    pub path: Option<String>,
    // TODO: set system timezone as default value
    pub timezone: Option<Tz>,
    // TODO: add locale
    pub resource_estimates: HashMap<String, String>,
    pub properties: HashMap<String, String>,
    pub prepared_statements: HashMap<String, String>,
    pub roles: HashMap<String, SelectedRole>,
    pub extra_credentials: HashMap<String, String>,
    pub transaction_id: TransactionId,
    pub client_request_timeout: Duration,
    pub compression_disabled: bool,
}

#[derive(Debug)]
pub(crate) struct SessionBuilder {
    pub(crate) host: String,
    pub(crate) port: u16,
    pub(crate) secure: bool,
    pub(crate) user: String,
    pub(crate) source: String,
    pub(crate) trace_token: Option<String>,
    pub(crate) client_tags: HashSet<String>,
    pub(crate) client_info: Option<String>,
    pub(crate) catalog: Option<String>,
    pub(crate) schema: Option<String>,
    pub(crate) path: Option<String>,
    pub(crate) timezone: Option<Tz>,
    // TODO: add locale
    pub(crate) resource_estimates: HashMap<String, String>,
    pub(crate) properties: HashMap<String, String>,
    pub(crate) prepared_statements: HashMap<String, String>,
    pub(crate) roles: HashMap<String, SelectedRole>,
    pub(crate) extra_credentials: HashMap<String, String>,
    pub(crate) transaction_id: TransactionId,
    pub(crate) client_request_timeout: Duration,
    pub(crate) compression_disabled: bool,
}

impl SessionBuilder {
    pub fn new(user: impl ToString, host: impl ToString) -> SessionBuilder {
        SessionBuilder {
            host: host.to_string(),
            port: 80,
            secure: false,
            user: user.to_string(),
            source: "trino-rust-client".to_string(),
            trace_token: None,
            client_tags: default(),
            client_info: None,
            catalog: None,
            schema: None,
            path: None,
            timezone: None,
            resource_estimates: default(),
            properties: default(),
            prepared_statements: default(),
            roles: default(),
            extra_credentials: default(),
            transaction_id: TransactionId::NoTransaction,
            client_request_timeout: Duration::from_secs(30),
            compression_disabled: false,
        }
    }

    pub fn build(self) -> Result<Session> {
        let scheme = if self.secure {
            Scheme::HTTPS
        } else {
            Scheme::HTTP
        };
        let host = self.host;
        let s = format!("{}://{}:{}/v1/statement", scheme, host, self.port);
        let url = Url::parse(&s).map_err(|_| Error::InvalidHost(host))?;
        let ret = Session {
            url,
            user: self.user,
            source: self.source,
            trace_token: self.trace_token,
            client_tags: self.client_tags,
            client_info: self.client_info,
            catalog: self.catalog,
            schema: self.schema,
            path: self.path,
            timezone: self.timezone,
            resource_estimates: self.resource_estimates,
            properties: self.properties,
            prepared_statements: self.prepared_statements,
            roles: self.roles,
            extra_credentials: self.extra_credentials,
            transaction_id: self.transaction_id,
            client_request_timeout: self.client_request_timeout,
            compression_disabled: self.compression_disabled,
        };
        Ok(ret)
    }
}
