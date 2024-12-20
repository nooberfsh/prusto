use http::HeaderValue;
use reqwest::Proxy;

use crate::error::Result;

#[derive(Default)]
pub struct ProxyBuilder {
    pub url: Option<String>,
    pub auth: Option<ProxyAuth>,
}

impl ProxyBuilder {
    pub fn build(self) -> Result<Option<Proxy>> {
        if let Some(url) = &self.url {
            let mut proxy = Proxy::all(url)?;
            match self.auth {
                Some(ProxyAuth::Basic(user, pass)) => {
                    proxy = proxy.basic_auth(user.as_str(), pass.as_str());
                }
                Some(ProxyAuth::CustomHTTP(header_value)) => {
                    proxy = proxy.custom_http_auth(header_value);
                }
                None => {}
            }
            Ok(Some(proxy))
        } else {
            Ok(None)
        }
    }
}

pub enum ProxyAuth {
    Basic(String, String),
    CustomHTTP(HeaderValue),
}
