#[derive(Debug)]
pub enum RoleType {
    Role,
    All,
    None,
}

#[derive(Debug)]
pub struct SelectedRole {
    pub ty: RoleType,
    pub role: Option<String>,
}

impl SelectedRole {
    pub fn new(ty: RoleType, role: Option<String>) -> Self {
        SelectedRole { ty, role }
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
}
