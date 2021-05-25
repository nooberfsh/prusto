#[derive(Debug, Copy, Clone)]
pub enum TransactionId {
    NoTransaction,
    StartTransaction,
    RollBack,
    Commit,
}

impl TransactionId {
    pub fn to_str(&self) -> &'static str {
        use TransactionId::*;
        match *self {
            NoTransaction => "NONE",
            StartTransaction => "START TRANSACTION",
            RollBack => "ROLLBACK",
            Commit => "COMMIT",
        }
    }
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "NONE" => Some(Self::NoTransaction),
            "START TRANSACTION" => Some(Self::StartTransaction),
            "ROLLBACK" => Some(Self::RollBack),
            "COMMIT" => Some(Self::Commit),
            _ => None,
        }
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        TransactionId::NoTransaction
    }
}
