use reqwest::header::HeaderMap;
use http::uri::Scheme;
use reqwest::Url;
use reqwest::header::HeaderValue;

use crate::{DataSet, Presto, RawDataSet, Result ,Error};
use crate::transaction::TransactionId;
use crate::constants::*;

// TODO: allow_redirects proxies  request_timeout handle_retry max_attempts

#[derive(Clone, Debug)]
pub enum Auth {
    Basic(String, String),
}

pub struct SessionProperties {

}

impl SessionProperties {
    fn to_string(&self) -> String {
        todo!()
    }
}

pub struct ClientSession {
    user: String,
    source: String,
    catalog: Option<String>,
    schema: Option<String>,
    session_properties: Option<SessionProperties>,
    http_headers: Option<HeaderMap>,
    transaction_id: Option<TransactionId>,
}

pub struct ClientBuilder {
    host: String,
    port: u16,
    session: ClientSession,
    http_scheme: Scheme,
    auth: Option<Auth>,
    max_attempts: u32,
    request_timeout: f32, // in seconds
}

impl ClientBuilder {
    pub fn new(user: String, host: String) -> Self {
        let session = ClientSession {
            user,
            source: "presto-python-client".to_string(),
            catalog: None,
            schema: None,
            session_properties: None,
            http_headers: None,
            transaction_id: None,
        };
        Self {
            host,
            session,
            port : 8080, // default
            http_scheme: Scheme::Http, // default is http
            auth: None,
            max_attempts: 3, // default
            request_timeout: 30.0, //default
        }
    }

    pub fn port(mut self, s: u16) -> Self {
        self.port = s;
        self
    }

    pub fn source(mut self, s: String) -> Self {
        self.session.source = s;
        self
    }

    pub fn catalog(mut self, s: String) -> Self {
        self.session.catalog = Some(s);
        self
    }

    pub fn schema(mut self, s: String) -> Self {
        self.session.catalog = Some(s);
        self
    }

    pub fn session_properties(mut self, s: SessionProperties) -> Self {
        self.session.session_properties = Some(s);
        self
    }

    pub fn http_headers(mut self, s: HeaderMap) -> Self {
        self.session.http_headers = Some(s);
        self
    }

    pub fn transaction_id(mut self, s: TransactionId) -> Self {
        self.session.transaction_id = Some(s);
        self
    }

    pub fn http_scheme(mut self, s: Scheme) -> Self {
        self.http_scheme = s;
        self
    }
    pub fn auth(mut self, s: Auth) -> Self {
        self.auth = Some(s);
        self
    }

    pub fn max_attempts(mut self, s: u32) -> Self {
        self.max_attempts = s;
        self
    }

    pub fn request_timeout(mut self, s: f32) -> Self {
        self.request_timeout = s;
        self
    }

    pub fn build(self) -> Result<Client> {
        let statement_url = self.statement_url();
        let http_headers = self.headers()?;
        if let Some(_) = &self.auth {
            if self.http_scheme == Scheme::HTTP {
                return Err(Error::BasicAuthWithHttp)
            }
        }
        let cli = Client {
            http: reqwest::Client::new(),
            session: self.session,
            auth: self.auth,
            http_headers,
            statement_url,
        };

        Ok(cli)
    }

    fn statement_url(&self) -> Url {
        let s = format!("{}://{host}:{port}/v1/statement", self.http_scheme, self.host, self.port);
        Url::parse(&s).unwrap()
    }

    fn headers(&self) -> Result<HeaderMap> {
        use  Error::*;

        let mut headers = HeaderMap::new();
        if  let Some(s )  = &self.session.catalog {
            headers.insert(HEADER_CATALOG, HeaderValue::from_str(s).map_err(|_| InvalidCatalog)?);
        }
        if  let Some(s )  = &self.session.schema {
            headers.insert(HEADER_SCHEMA, HeaderValue::from_str(s).map_err(|_| InvalidSchema)?);
        }

        headers.insert(HEADER_SOURCE, HeaderValue::from_str(&self.session.source).map_err(|_| InvalidSource)?);

        headers.insert(HEADER_USER, HeaderValue::from_str(&self.session.user).map_err(|_| InvalidUser)?);

        if let Some(s) = &self.session.session_properties {
            headers.insert(HEADER_USER, HeaderValue::from_str(&s.to_string()).map_err(|_| InvalidProperties)?);
        }

        if let Some(d) = &self.session.transaction_id {
            let s = d.to_str();
            headers.insert(HEADER_TRANSACTION, HeaderValue::from_static(s));
        }

        if let Some(d) = &self.session.http_headers {
            for (k, v) in d {
                if let Some(_) = headers.insert(k, v.clone()) {
                    return Err(Error::DuplicateHeaderk.clone())
                }
            }
        }

        Ok(headers)
    }
}

pub struct Client {
    http: reqwest::Client,
    session: ClientSession,
    auth: Option<Auth>,
    http_headers: HeaderMap,
    statement_url: Url,
}

impl Client {
    pub async fn get_raw_data_set(&self, sql: &str) -> Result<RawDataSet> {
        let req = self.client.post(self.statement_url.clone())
            .headers(self.http_headers.clone())
        .body(sql);

        let req = if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, p),
            }
        } else {
            req
        };



    }

    pub async fn get_data_set<T: Presto + 'static>(&self, sql: &str) -> Result<DataSet<T>> {
        todo!()
    }
}
