use std::fmt;

use serde::Deserialize;

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct QueryError {
    pub message: String,
    pub sql_state: Option<String>,
    pub error_code: i32,
    pub error_name: String,
    pub error_type: String,
    pub error_location: Option<ErrorLocation>,
    pub failure_info: FailureInfo,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ErrorLocation {
    pub line_number: u32,
    pub column_number: u32,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct FailureInfo {
    #[serde(rename = "type")]
    pub ty: String,
    pub suppressed: Vec<FailureInfo>,
    pub stack: Vec<String>,
    pub message: Option<String>,
    pub cause: Option<Box<FailureInfo>>,
    pub error_location: Option<ErrorLocation>,
}

impl fmt::Display for QueryError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "message: {}", self.message)?;
        if let Some(st) = &self.sql_state {
            writeln!(f, "sql_state: {}", st)?;
        }
        writeln!(f, "error_code: {}", self.error_code)?;
        writeln!(f, "error_type: {}", self.error_name)?;
        if let Some(loc) = &self.error_location {
            writeln!(f, "error_location: {}", loc)?;
        }
        writeln!(f, "failure_info: {}", self.failure_info)
    }
}

impl std::error::Error for QueryError {}

impl fmt::Display for ErrorLocation {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "({}, {})", self.line_number, self.column_number)
    }
}

impl fmt::Display for FailureInfo {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        writeln!(f, "ty: {}", self.ty)?;
        if let Some(msg) = &self.message {
            writeln!(f, "message: {}", msg)?;
        }
        if let Some(loc) = &self.error_location {
            writeln!(f, "loc: {}", loc)?;
        }
        writeln!(f, "stack:")?;
        for s in &self.stack {
            writeln!(f, "\ttype: {}", s)?;
        }
        if let Some(cause) = &self.cause {
            writeln!(f, "cause: {}", cause)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_loc() {
        let loc = ErrorLocation {
            line_number: 100,
            column_number: 15,
        };

        assert_eq!("(100, 15)", format!("{}", loc));
    }

    #[test]
    fn test_failure() {
        let failure = FailureInfo {
            ty: "xxxty".into(),
            suppressed: vec![],
            stack: vec!["stack_1".into(), "stack_2".into(), "stack_3".into()],
            message: None,
            cause: None,
            error_location: None,
        };

        println!("{}", failure);
    }
}
