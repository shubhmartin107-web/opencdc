#![allow(clippy::should_implement_trait)]

pub mod config;
pub mod r#trait;

pub use config::*;
pub use r#trait::*;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

#[cfg(feature = "mongodb")]
pub mod mongodb;
