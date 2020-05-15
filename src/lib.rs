#![allow(incomplete_features)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]
#![feature(const_generics)]

pub mod client;
pub mod constants;
pub mod error;
pub mod models;
pub mod transaction;
pub mod types;

pub use client::*;
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
