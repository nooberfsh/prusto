use serde::Deserialize;

use crate::error::Result;

//TODO:
//        session_properties=None,
//        http_headers=None,
//        http_scheme=constants.HTTP,
//        auth=constants.DEFAULT_AUTH,
//        redirect_handler=presto.redirect.GatewayRedirectHandler(),
//        max_attempts=constants.DEFAULT_MAX_ATTEMPTS,
//        request_timeout=constants.DEFAULT_REQUEST_TIMEOUT,
//        isolation_level=IsolationLevel.AUTOCOMMIT,
pub struct ClientBuilder {
    host: String,
    port: Option<u16>,
    user: Option<String>,
    source: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl ClientBuilder {
    pub fn new(host: impl ToString) -> ClientBuilder {
        ClientBuilder {
            host: host.to_string(),
            port: None,
            user: None,
            source: None,
            catalog: None,
            schema: None,
        }
    }

    pub fn port(mut self, port: u16) -> ClientBuilder {
        self.port = Some(port);
        self
    }

    pub fn user(mut self, user: impl ToString) -> ClientBuilder {
        self.user = Some(user.to_string());
        self
    }

    pub fn source(mut self, source: impl ToString) -> ClientBuilder {
        self.source = Some(source.to_string());
        self
    }

    pub fn catalog(mut self, catalog: impl ToString) -> ClientBuilder {
        self.catalog = Some(catalog.to_string());
        self
    }

    pub fn schema(mut self, schema: impl ToString) -> ClientBuilder {
        self.schema = Some(schema.to_string());
        self
    }

    pub fn build(self) -> Client {
        todo!()
    }
}

pub struct Client {
    client: reqwest::Client,
}

impl Client {}
