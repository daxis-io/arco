//! Canonical lake table-format contract.

use crate::error::{Error, Result};

/// Supported table formats across Arco control-plane APIs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TableFormat {
    /// Delta Lake table.
    Delta,
    /// Apache Iceberg table.
    Iceberg,
    /// Legacy/default parquet table surface.
    Parquet,
}

impl TableFormat {
    /// Parses a table format using case-insensitive matching.
    ///
    /// Accepted values: `delta`, `iceberg`, `parquet` (any casing).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when `raw` is unknown or empty.
    pub fn parse(raw: &str) -> Result<Self> {
        match raw.trim().to_ascii_lowercase().as_str() {
            "delta" => Ok(Self::Delta),
            "iceberg" => Ok(Self::Iceberg),
            "parquet" => Ok(Self::Parquet),
            other => Err(Error::Validation {
                message: format!(
                    "unknown table format '{other}'; expected one of: delta, iceberg, parquet"
                ),
            }),
        }
    }

    /// Returns the canonical lowercase string representation.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Delta => "delta",
            Self::Iceberg => "iceberg",
            Self::Parquet => "parquet",
        }
    }

    /// Parses and returns the canonical lowercase string representation.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when `raw` is unknown or empty.
    pub fn normalize(raw: &str) -> Result<String> {
        Ok(Self::parse(raw)?.as_str().to_string())
    }

    /// Resolves the effective table format from optional persisted metadata.
    ///
    /// Legacy rows without a stored format (`None`) are treated as `parquet`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Validation`] when the provided format value is unknown.
    pub fn effective(raw: Option<&str>) -> Result<Self> {
        raw.map_or_else(|| Ok(Self::Parquet), Self::parse)
    }
}

impl std::fmt::Display for TableFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}
