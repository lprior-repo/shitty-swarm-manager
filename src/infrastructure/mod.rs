#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![warn(clippy::pedantic)]
#![warn(clippy::nursery)]
#![forbid(unsafe_code)]

pub mod persistence;

pub use persistence::repositories::{
    AgentRepository, ArtifactRepository, AuditRepository, BeadRepository, LockRepository,
    StageRepository,
};
