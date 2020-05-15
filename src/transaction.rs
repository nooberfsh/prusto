#[derive(Debug, Copy, Clone)]
pub enum TransactionId {
    NoTransaction,
    StartTransaction,
    RollBack,
    Commit
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
}
