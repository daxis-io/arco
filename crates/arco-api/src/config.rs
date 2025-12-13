//! Server configuration.

use serde::{Deserialize, Serialize};

/// Configuration for the Arco API server.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// HTTP server port.
    pub http_port: u16,

    /// gRPC server port.
    pub grpc_port: u16,

    /// Enable debug endpoints.
    pub debug: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            http_port: 8080,
            grpc_port: 9090,
            debug: false,
        }
    }
}
