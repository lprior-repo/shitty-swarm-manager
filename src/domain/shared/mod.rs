#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

mod error;
mod ids;
mod value_objects;

pub use error::{Result, RuntimeError};
pub use ids::{RuntimeAgentId, RuntimeBeadId, RuntimeRepoId};
pub use value_objects::{CastSigned, CastUnsigned};
