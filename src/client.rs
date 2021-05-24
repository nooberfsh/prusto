use std::collections::{HashMap, HashSet};

use futures_async_stream::try_stream;
use http::header::{ACCEPT_ENCODING, USER_AGENT};
use iterable::*;
use reqwest::RequestBuilder;
use tokio::time::{sleep, Duration};

use crate::error::{Error, Result};
use crate::header::*;
use crate::session::{Session, SessionBuilder};
use crate::transaction::TransactionId;
use crate::{DataSet, Presto, QueryResult};

// TODO:
// allow_redirects
// proxies
// cancel

pub struct Client {
    client: reqwest::Client,
    session: Session,
    auth: Option<Auth>,
    max_attempt: usize,
}

#[derive(Clone, Debug)]
pub enum Auth {
    Basic(String, Option<String>),
}

pub struct ClientBuilder {
    session: SessionBuilder,
    auth: Option<Auth>,
    max_attempt: usize,
}

impl Auth {
    pub fn new_basic(username: impl ToString, password: Option<impl ToString>) -> Auth {
        Auth::Basic(username.to_string(), password.map(|p| p.to_string()))
    }
}

impl ClientBuilder {
    pub fn new(user: impl ToString, host: impl ToString) -> Self {
        let builder = SessionBuilder::new(user, host);
        Self {
            session: builder,
            auth: None,
            max_attempt: 3,
        }
    }

    pub fn port(mut self, s: u16) -> Self {
        self.session.port = s;
        self
    }

    pub fn secure(mut self, s: bool) -> Self {
        self.session.secure = s;
        self
    }

    pub fn source(mut self, s: impl ToString) -> Self {
        self.session.source = s.to_string();
        self
    }

    pub fn trace_token(mut self, s: impl ToString) -> Self {
        self.session.trace_token = Some(s.to_string());
        self
    }

    pub fn client_tags(mut self, s: HashSet<String>) -> Self {
        self.session.client_tags = s;
        self
    }

    pub fn client_info(mut self, s: impl ToString) -> Self {
        self.session.client_info = Some(s.to_string());
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

    pub fn path(mut self, s: impl ToString) -> Self {
        self.session.path = Some(s.to_string());
        self
    }

    pub fn resource_estimates(mut self, s: HashMap<String, String>) -> Self {
        self.session.resource_estimates = s;
        self
    }

    pub fn properties(mut self, s: HashMap<String, String>) -> Self {
        self.session.properties = s;
        self
    }

    pub fn prepared_statements(mut self, s: HashMap<String, String>) -> Self {
        self.session.prepared_statements = s;
        self
    }

    pub fn extra_credentials(mut self, s: HashMap<String, String>) -> Self {
        self.session.extra_credentials = s;
        self
    }

    pub fn transaction_id(mut self, s: TransactionId) -> Self {
        self.session.transaction_id = s;
        self
    }

    pub fn client_request_timeout(mut self, s: Duration) -> Self {
        self.session.client_request_timeout = s;
        self
    }

    pub fn compression_disabled(mut self, s: bool) -> Self {
        self.session.compression_disabled = s;
        self
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////

    pub fn auth(mut self, s: Auth) -> Self {
        self.auth = Some(s);
        self
    }

    pub fn max_attempt(mut self, s: usize) -> Self {
        self.max_attempt = s;
        self
    }

    pub fn build(self) -> Result<Client> {
        let session = self.session.build()?;
        let max_attempt = self.max_attempt;
        let client = reqwest::ClientBuilder::new()
            .timeout(session.client_request_timeout)
            .build()
            .unwrap();
        if self.auth.is_some() && session.url.scheme() == "http" {
            return Err(Error::BasicAuthWithHttp);
        }
        let cli = Client {
            auth: self.auth,
            session,
            client,
            max_attempt,
        };

        Ok(cli)
    }
}

fn add_prepare_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    builder = builder.header(HEADER_USER, &session.user);
    // TODO: difference with session.source?
    builder = builder.header(USER_AGENT, "trino-rust-client");
    if session.compression_disabled {
        builder = builder.header(ACCEPT_ENCODING, "identity")
    }
    builder
}

fn add_session_header(mut builder: RequestBuilder, session: &Session) -> RequestBuilder {
    builder = add_prepare_header(builder, session);
    builder = builder.header(HEADER_SOURCE, &session.source);

    if let Some(v) = &session.trace_token {
        builder = builder.header(HEADER_TRACE_TOKEN, v);
    }

    if !session.client_tags.is_empty() {
        builder = builder.header(HEADER_CLIENT_TAGS, session.client_tags.by_ref().join(","));
    }

    if let Some(v) = &session.client_info {
        builder = builder.header(HEADER_CLIENT_INFO, v);
    }

    if let Some(v) = &session.catalog {
        builder = builder.header(HEADER_CATALOG, v);
    }

    if let Some(v) = &session.schema {
        builder = builder.header(HEADER_SCHEMA, v);
    }

    if let Some(v) = &session.path {
        builder = builder.header(HEADER_PATH, v);
    }
    // TODO: add timezone and locale
    builder = add_header_map(builder, HEADER_SESSION, &session.properties);
    builder = add_header_map(
        builder,
        HEADER_RESOURCE_ESTIMATE,
        &session.resource_estimates,
    );
    builder = add_header_map(
        builder,
        HEADER_ROLE,
        &session
            .roles
            .by_ref()
            .map_kv(|(k, v)| (k.to_string(), v.to_string())),
    );
    builder = add_header_map(builder, HEADER_EXTRA_CREDENTIAL, &session.extra_credentials);
    builder = add_header_map(
        builder,
        HEADER_PREPARED_STATEMENT,
        &session.prepared_statements,
    );
    builder = builder.header(HEADER_TRANSACTION, session.transaction_id.to_str());
    builder = builder.header(HEADER_CLIENT_CAPABILITIES, "PATH,PARAMETRIC_DATETIME");
    builder
}

fn add_header_map<'a>(
    mut builder: RequestBuilder,
    header: &str,
    map: impl IntoIterator<Item = (&'a String, &'a String)>,
) -> RequestBuilder {
    for (k, v) in map {
        let kv = format!("{}={}", k, urlencoding::encode(v));
        builder = builder.header(header, kv);
    }
    builder
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
    #[try_stream(ok = DataSet<T>, error = Error)]
    pub async fn get_stream<T: Presto + Unpin + 'static>(&self, sql: String) {
        let res = self.get_retry::<T>(sql).await?;
        if let Some(e) = res.error {
            Err(Error::QueryError(e))?;
        } else {
            let mut next = res.next_uri;
            while let Some(url) = next {
                let res = self.get_next_retry(&url).await?;
                next = res.next_uri;
                if let Some(d) = res.data_set {
                    yield d
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
        let req = self.client.post(self.session.url.clone()).body(sql);

        let req = add_session_header(req, &self.session);

        let req = if let Some(auth) = self.auth.as_ref() {
            match auth {
                Auth::Basic(u, p) => req.basic_auth(u, p.as_ref()),
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
        let req = self.client.get(url);
        let req = add_prepare_header(req, &self.session);

        let data = req.send().await?.json::<QueryResult<T>>().await?;
        Ok(data)
    }
}
