#![allow(incomplete_features)]
#![feature(type_alias_impl_trait)]
#![feature(generic_associated_types)]

pub mod error;
mod models;
pub mod types;

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
