use strum::{Display, EnumString, IntoStaticStr};

#[derive(Debug, Copy, Clone, Display, EnumString, IntoStaticStr)]
pub enum TransactionId {
    #[strum(serialize = "NONE")]
    NoTransaction,
    #[strum(serialize = "START TRANSACTION")]
    StartTransaction,
    #[strum(serialize = "ROLLBACK")]
    RollBack,
    #[strum(serialize = "COMMIT")]
    Commit,
}

impl Default for TransactionId {
    fn default() -> Self {
        TransactionId::NoTransaction
    }
}
