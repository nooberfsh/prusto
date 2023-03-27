#![allow(clippy::should_implement_trait)]
#![allow(clippy::derivable_impls)]

pub mod auth;
pub mod client;
pub mod error;

#[cfg(not(feature = "presto"))]
mod header;

#[cfg(feature = "presto")]
mod presto_header;

pub mod selected_role;
pub mod session;
pub mod ssl;
pub mod transaction;

pub use client::*;
pub use prusto_macros::*;
pub use prusto_model::*;
