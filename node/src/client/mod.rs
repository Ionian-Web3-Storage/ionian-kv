#![allow(dead_code)]

mod builder;
mod environment;

pub use builder::ClientBuilder;
pub use environment::{Environment, EnvironmentBuilder, RuntimeContext};

/// The core Ionian client.
///
/// Holds references to running services, cleanly shutting them down when dropped.
pub struct Client;
