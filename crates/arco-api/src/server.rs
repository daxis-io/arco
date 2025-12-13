//! API server implementation.

use crate::config::Config;
use arco_core::Result;

/// The Arco API server.
///
/// Serves both HTTP and gRPC endpoints for catalog and orchestration.
#[derive(Debug)]
pub struct Server {
    config: Config,
}

impl Server {
    /// Creates a new server with the given configuration.
    #[must_use]
    pub fn new(config: Config) -> Self {
        Self { config }
    }

    /// Returns the server configuration.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Starts the server and blocks until shutdown.
    ///
    /// # Errors
    ///
    /// Returns an error if the server cannot start.
    pub async fn serve(&self) -> Result<()> {
        tracing::info!(
            http_port = self.config.http_port,
            grpc_port = self.config.grpc_port,
            "Starting Arco API server"
        );

        // TODO: Implement actual server
        Ok(())
    }
}

/// Builder for constructing a server.
#[derive(Debug, Default)]
pub struct ServerBuilder {
    config: Config,
}

impl ServerBuilder {
    /// Creates a new server builder.
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the HTTP port.
    #[must_use]
    pub fn http_port(mut self, port: u16) -> Self {
        self.config.http_port = port;
        self
    }

    /// Sets the gRPC port.
    #[must_use]
    pub fn grpc_port(mut self, port: u16) -> Self {
        self.config.grpc_port = port;
        self
    }

    /// Enables debug endpoints.
    #[must_use]
    pub fn debug(mut self, enabled: bool) -> Self {
        self.config.debug = enabled;
        self
    }

    /// Builds the server.
    #[must_use]
    pub fn build(self) -> Server {
        Server::new(self.config)
    }
}
