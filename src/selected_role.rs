use lazy_static::lazy_static;
use regex::Regex;

#[derive(Debug, PartialEq)]
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

    pub fn from_str(s: &str) -> Option<Self> {
        let cap = PATTERN.captures(s)?;
        let ty = match cap.get(1).unwrap().as_str() {
            "ROLE" => RoleType::Role,
            "ALL" => RoleType::All,
            "NONE" => RoleType::None,
            _ => unreachable!(),
        };
        let role = cap.get(3).map(|m| m.as_str().to_string());
        Some(Self::new(ty, role))
    }
}

impl ToString for RoleType {
    fn to_string(&self) -> String {
        use RoleType::*;
        match self {
            Role => "ROLE".to_string(),
            All => "ALL".to_string(),
            None => "NONE".to_string(),
        }
    }
}

impl ToString for SelectedRole {
    fn to_string(&self) -> String {
        let ty = self.ty.to_string();
        if let Some(role) = &self.role {
            format!("{}{{{}}}", ty, role)
        } else {
            ty
        }
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
