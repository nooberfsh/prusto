use reqwest::Certificate;
use crate::error::Result;
use std::io::Read;
use std::path::Path;

#[derive(Clone)]
pub struct Ssl {
    pub root_cert: Option<Certificate>,
}

impl Default for Ssl {
    fn default() -> Self {
        Ssl {
            root_cert: None,
        }
    }
}

impl Ssl {
    pub fn cert_from_pem<P: AsRef<Path>>(root_certificate_path: P) -> Result<reqwest::Certificate> {
        let mut buf = Vec::new();
        std::fs::File::open(root_certificate_path.as_ref())
            .unwrap()
            .read_to_end(&mut buf)
            .unwrap();

        reqwest::Certificate::from_pem(&buf).map_err(|e| {
            crate::error::Error::InternalError(format!("Cannot load PEM certificate{:?}", e))
        })
    }

    pub fn cert_from_der<P: AsRef<Path> + std::fmt::Display>(root_certificate_path: &P) -> Result<reqwest::Certificate> {
        let mut buf = Vec::new();
        std::fs::File::open(root_certificate_path)
            .expect(&format!(
                "Error opening certificate file {}",
                root_certificate_path
            ))
            .read_to_end(&mut buf)
            .expect(&format!(
                "Error reading certificate file {}",
                root_certificate_path
            ));

        reqwest::Certificate::from_der(&buf).map_err(|e| {
            crate::error::Error::InternalError(format!("Cannot load DER certificate {:?}", e))
        })
    }
}
