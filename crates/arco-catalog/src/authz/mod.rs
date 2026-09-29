//! Authorization domain.

pub mod compiler;
pub mod decision;
pub mod explain;
pub mod grants;
pub mod privileges;
#[cfg(feature = "test-utils")]
pub mod tenant_probe;
