use std::fmt;
use std::str::FromStr;

use lazy_static::lazy_static;
use regex::Regex;
use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, PartialEq, EnumString, Display, IntoStaticStr)]
#[strum(serialize_all = "UPPERCASE")]
pub enum RoleType {
    Role,
    All,
    None,
}

#[derive(Debug, PartialEq)]
pub struct SelectedRole {
    pub ty: RoleType,
    pub role: Option<String>,
}

lazy_static! {
    static ref PATTERN: Regex = Regex::new(r"^(ROLE|ALL|NONE)(\{(.*)\})?$").unwrap();
}

impl SelectedRole {
    pub fn new(ty: RoleType, role: Option<String>) -> Self {
        SelectedRole { ty, role }
    }
}

impl FromStr for SelectedRole {
    type Err = crate::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let cap = PATTERN.captures(s).ok_or(crate::Error::ParseRoleFailed)?;
        let ty: RoleType = match cap.get(1).unwrap().as_str().try_into() {
            Ok(val) => val,
            Err(_) => unreachable!(),
        };
        let role = cap.get(3).map(|m| m.as_str().to_string());
        Ok(Self::new(ty, role))
    }
}

impl fmt::Display for SelectedRole {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let ty = &self.ty;
        write!(f, "{}", ty)?;
        if let Some(role) = &self.role {
            write!(f, "{{{}}}", role)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_to_string() {
        let a = SelectedRole::new(RoleType::All, None);
        let res = a.to_string();
        assert_eq!(res, "ALL");

        let a = SelectedRole::new(RoleType::Role, Some("admin".to_string()));
        let res = a.to_string();
        assert_eq!(res, "ROLE{admin}");
    }

    #[test]
    fn test_from_str() {
        let a = "ALL";
        let res = SelectedRole::from_str(a).unwrap();
        assert_eq!(res.ty, RoleType::All);
        assert_eq!(res.role, None);

        let a = "ROLE{admin}";
        let res = SelectedRole::from_str(a).unwrap();
        assert_eq!(res.ty, RoleType::Role);
        assert_eq!(res.role, Some("admin".to_string()));
    }
}
