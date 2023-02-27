use crate::error::Result;
use std::io::Read;
use std::path::Path;

#[derive(Clone)]
pub struct Certificate(pub(crate) reqwest::Certificate);

#[derive(Clone)]
pub struct Ssl {
    pub root_cert: Option<Certificate>,
}

impl Default for Ssl {
    fn default() -> Self {
        Ssl { root_cert: None }
    }
}

impl Ssl {
    pub fn read_pem<P: AsRef<Path>>(root_certificate_path: &P) -> Result<Certificate> {
        let buf = Self::read_file(&root_certificate_path)?;
        match reqwest::Certificate::from_pem(&buf) {
            Ok(cert) => Ok(Certificate(cert)),
            Err(e) => Err(crate::error::Error::InternalError(format!(
                "Cannot load PEM certificate {:?}",
                e
            ))),
        }
    }

    pub fn read_der<P: AsRef<Path>>(root_certificate_path: &P) -> Result<Certificate> {
        let buf = Self::read_file(&root_certificate_path)?;
        match reqwest::Certificate::from_der(&buf) {
            Ok(cert) => Ok(Certificate(cert)),
            Err(e) => Err(crate::error::Error::InternalError(format!(
                "Cannot load DER certificate {:?}",
                e
            ))),
        }
    }

    fn read_file<P: AsRef<Path>>(file_path: &P) -> Result<Vec<u8>> {
        let mut buf = Vec::new();
        std::fs::File::open(file_path)
            .map_err(|e| {
                crate::error::Error::InternalError(format!(
                    "Error opening file {}. {}",
                    file_path.as_ref().display(),
                    e
                ))
            })?
            .read_to_end(&mut buf)
            .map_err(|e| {
                crate::error::Error::InternalError(format!(
                    "Error reading file {}. {}",
                    file_path.as_ref().display(),
                    e
                ))
            })?;

        Ok(buf)
    }
}
