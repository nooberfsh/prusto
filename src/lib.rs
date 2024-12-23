#![allow(clippy::should_implement_trait)]
#![allow(clippy::derivable_impls)]

pub mod auth;
pub mod client;
pub mod error;

#[cfg(not(feature = "presto"))]
mod header;
pub mod models;
#[cfg(feature = "presto")]
mod presto_header;
mod proxy;
pub mod selected_role;
pub mod session;
pub mod ssl;
pub mod transaction;
pub mod tuples;
pub mod types;

pub use client::*;
pub use models::*;
pub use prusto_macros::*;
pub use types::*;
