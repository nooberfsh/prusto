#![feature(generic_associated_types)]

pub mod error;
mod models;
pub mod types;

pub use models::*;
pub use types::*;
pub use macros::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
