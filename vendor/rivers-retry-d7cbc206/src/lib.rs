//! Pinned, unmodified Rivers retry and compute definitions.
mod retry;
pub use retry::*;
/// Original executor-neutral types referenced by retry metadata.
pub mod execution {
    /// Original Rivers compute declarations.
    pub mod compute;
}
