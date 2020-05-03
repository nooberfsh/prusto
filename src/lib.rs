#![feature(generic_associated_types)]

pub mod client;
mod constants;
pub mod error;
mod models;
pub mod types;

pub use models::*;

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
