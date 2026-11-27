//! Route handlers for the Unity Catalog facade.

pub mod catalogs;
pub mod common;
pub mod credentials;
pub mod delta;
pub mod delta_commits;
pub(crate) mod event_streams;
pub mod external_locations;
pub mod openapi;
pub mod permissions;
pub(crate) mod preview;
pub mod schemas;
pub mod storage_credentials;
pub mod table_reads;
pub mod tables;
pub(crate) mod usage;
