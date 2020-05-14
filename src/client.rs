use crate::{DataSet, Presto, RawDataSet, Result};

pub struct ConnectionInfo {
    pub host: String,
    pub port: u16,
    pub user: String,
    pub password: Option<String>,
}

pub struct ClientBuilder {
    conn: ConnectionInfo,
    source: Option<String>,
    catalog: Option<String>,
    schema: Option<String>,
}

impl ClientBuilder {
    pub fn new(conn: ConnectionInfo) -> Self {
        Self {
            conn,
            source: None,
            catalog: None,
            schema: None,
        }
    }

    pub fn source(mut self, s: String) -> Self {
        self.source = Some(s);
        self
    }

    pub fn catalog(mut self, s: String) -> Self {
        self.catalog = Some(s);
        self
    }

    pub fn schema(mut self, s: String) -> Self {
        self.schema = Some(s);
        self
    }

    pub fn build(self) -> Client {
        Client {
            http: reqwest::Client::new(),
            source: self.source.unwrap_or("presto-rust-client".into()),
            catalog: self.catalog,
            schema: self.schema,
        }
    }
}

pub struct Client {
    http: reqwest::Client,
    source: String,
    catalog: Option<String>,
    schema: Option<String>,
}

impl Client {
    pub async fn get_raw_data_set(sql: &str) -> Result<RawDataSet> {
        todo!()
    }

    pub async fn get_data_set<T: Presto + 'static>(sql: &str) -> Result<DataSet<T>> {
        todo!()
    }
}
