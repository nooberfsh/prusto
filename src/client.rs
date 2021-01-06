use std::collections::HashMap;

use async_stream::try_stream;
use futures::Stream;
use http::uri::Scheme;
use iterable::*;
use reqwest::header::HeaderMap;
use reqwest::header::HeaderValue;
use reqwest::Url;
use tokio::time::{sleep, Duration};

use crate::error::{Error, Result};
use crate::header::*;
use crate::transaction::TransactionId;
use crate::{DataSet, Presto, QueryResult};

// TODO:
// allow_redirects
// proxies
// cancel

#[derive(Clone, Debug)]
pub enum Auth {
    Basic(String, String),
}

pub struct ClientSession {
    user: String,
    source: String,
    catalog: Option<String>,
    schema: Option<String>,
    session_properties: HashMap<String, String>,
    http_headers: Option<HeaderMap>,
    transaction_id: Option<TransactionId>,
}

pub struct ClientBuilder {
    host: String,
    port: u16,
    session: ClientSession,
    http_scheme: Scheme,
    auth: Option<Auth>,
    max_attempt: usize,
    request_timeout: u64, // in seconds
}

impl ClientBuilder {
    pub fn new(user: impl ToString, host: impl ToString) -> Self {
        let session = ClientSession {
            user: user.to_string(),
            source: "presto-python-client".to_string(),
            catalog: None,
            schema: None,
            session_properties: HashMap::new(),
            http_headers: None,
            transaction_id: None,
        };
        Self {
            session,
            host: host.to_string(),
            port: 8080,                // default
            http_scheme: Scheme::HTTP, // default is http
            auth: None,
            max_attempt: 3,      // default
            request_timeout: 30, //default
        }
    }

    pub fn port(mut self, s: u16) -> Self {
        self.port = s;
        self
    }

    pub fn source(mut self, s: impl ToString) -> Self {
        self.session.source = s.to_string();
        self
    }

    pub fn catalog(mut self, s: impl ToString) -> Self {
        self.session.catalog = Some(s.to_string());
        self
    }

    pub fn schema(mut self, s: impl ToString) -> Self {
        self.session.catalog = Some(s.to_string());
        self
    }

    pub fn session_properties(mut self, s: HashMap<String, String>) -> Self {
        self.session.session_properties = s;
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

    pub fn max_attempt(mut self, s: usize) -> Self {
        self.max_attempt = s;
        self
    }

    pub fn request_timeout(mut self, s: u64) -> Self {
        self.request_timeout = s;
        self
    }

    pub fn build(self) -> Result<Client> {
        let statement_url = self.statement_url();
        let http_headers = self.headers()?;
        let max_attempt = self.max_attempt;
        let client = reqwest::ClientBuilder::new()
            .timeout(std::time::Duration::from_secs(self.request_timeout))
            .build()
            .unwrap();
        if self.auth.is_some() && self.http_scheme == Scheme::HTTP {
            return Err(Error::BasicAuthWithHttp);
        }
        let cli = Client {
            session: self.session,
            auth: self.auth,
            client,
            http_headers,
            statement_url,
            max_attempt,
        };

        Ok(cli)
    }

    fn statement_url(&self) -> Url {
        let s = format!(
            "{}://{}:{}/v1/statement",
            self.http_scheme, self.host, self.port
        );
        Url::parse(&s).unwrap()
    }

    fn headers(&self) -> Result<HeaderMap> {
        use Error::*;

        let mut headers = HeaderMap::new();
        if let Some(s) = &self.session.catalog {
            headers.insert(
                HEADER_CATALOG,
                HeaderValue::from_str(s).map_err(|_| InvalidCatalog)?,
            );
        }
        if let Some(s) = &self.session.schema {
            headers.insert(
                HEADER_SCHEMA,
                HeaderValue::from_str(s).map_err(|_| InvalidSchema)?,
            );
        }

        headers.insert(
            HEADER_SOURCE,
            HeaderValue::from_str(&self.session.source).map_err(|_| InvalidSource)?,
        );

        headers.insert(
            HEADER_USER,
            HeaderValue::from_str(&self.session.user).map_err(|_| InvalidUser)?,
        );

        if !self.session.session_properties.is_empty() {
            let v = self
                .session
                .session_properties
                .by_ref()
                .lazy_map(|(k, v)| format!("{}={}", k, v))
                .join(",");
            headers.insert(
                HEADER_SESSION,
                HeaderValue::from_str(&v).map_err(|_| InvalidProperties)?,
            );
        }

        if let Some(d) = &self.session.transaction_id {
            let s = d.to_str();
            headers.insert(HEADER_TRANSACTION, HeaderValue::from_static(s));
        }

        if let Some(d) = &self.session.http_headers {
            for (k, v) in d {
                if headers.insert(k, v.clone()).is_some() {
                    return Err(Error::DuplicateHeader(k.clone()));
                }
            }
        }

        Ok(headers)
    }
}

pub struct Client {
    client: reqwest::Client,
    #[allow(unused)]
    session: ClientSession,
    auth: Option<Auth>,
    http_headers: HeaderMap,
    statement_url: Url,
    max_attempt: usize,
}

macro_rules! retry {
    ($self:expr, $f:ident, $param:expr, $max_attempt:expr) => {{
        for _ in 0..$max_attempt {
            let res: std::result::Result<QueryResult<_>, reqwest::Error> =
                $self.$f($param.clone()).await;
            match res {
                Ok(d) => match d.error {
                    Some(e) => return Err(Error::QueryError(e)),
                    None => return Ok(d),
                },
                Err(e) => match e.status() {
                    Some(code) if code == 503 => {
                        sleep(Duration::from_millis(100)).await;
                        continue;
                    }
                    _ => return Err(Error::HttpError(e)),
                },
            }
        }

        Err(Error::ReachMaxAttempt($max_attempt))
    }};
}

impl Client {
    pub fn get_stream<T: Presto + Unpin + 'static>(
        &self,
        sql: String,
    ) -> impl Stream<Item = Result<DataSet<T>>> + '_ {
        try_stream! {
            let res = self.get_retry::<T>(sql).await?;
            if let Some(e) = res.error {
                Err(Error::QueryError(e))?;
            } else {
                let mut next = res.next_uri;
                while let Some(url) = next {
                    let res = self.get_next_retry(&url).await?;
                    next = res.next_uri;

                    if let Some(d)  = res.data_set {
                        yield d
                    }
                }
            }
        }
    }

    pub async fn get_all<T: Presto + 'static>(&self, sql: String) -> Result<DataSet<T>> {
        let res = self.get_retry(sql).await?;
        let mut ret = res.data_set;

        let mut next = res.next_uri;
        while let Some(url) = &next {
            let res = self.get_next_retry(url).await?;
            next = res.next_uri;
            if let Some(d) = res.data_set {
                match &mut ret {
                    Some(ret) => ret.merge(d),
                    None => ret = Some(d),
                }
            }
        }

        if let Some(d) = ret {
            Ok(d)
        } else {
            Err(Error::EmptyData)
        }
    }

    async fn get_retry<T: Presto + 'static>(&self, sql: String) -> Result<QueryResult<T>> {
        retry!(&self, get, sql, self.max_attempt)
    }

    async fn get_next_retry<T: Presto + 'static>(&self, url: &str) -> Result<QueryResult<T>> {
        retry!(&self, get_next, url, self.max_attempt)
    }

    async fn get<T: Presto + 'static>(
        &self,
        sql: String,
    ) -> std::result::Result<QueryResult<T>, reqwest::Error> {
        let req = self
            .client
            .post(self.statement_url.clone())
            .headers(self.http_headers.clone())
            .body(sql);

        let req = if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, Some(p)),
            }
        } else {
            req
        };

        let data = req.send().await?.json::<QueryResult<T>>().await?;
        Ok(data)
    }

    async fn get_next<T: Presto + 'static>(
        &self,
        url: &str,
    ) -> std::result::Result<QueryResult<T>, reqwest::Error> {
        let data = self
            .client
            .get(url)
            .send()
            .await?
            .json::<QueryResult<T>>()
            .await?;
        Ok(data)
    }
}
