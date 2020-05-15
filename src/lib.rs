#![allow(incomplete_features)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]
#![feature(const_generics)]

pub mod client;
pub mod error;
pub mod models;
pub mod types;
pub mod transaction;
pub mod constants;

pub use client::*;
pub use error::*;
pub use macros::*;
pub use models::*;
pub use types::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
