//! Catalog privilege definitions.

use std::fmt;
use std::str::FromStr;

/// Stable catalog privilege names used by authorization decisions.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Privilege {
    /// Read table, view, volume, function, or model metadata/data.
    Select,
    /// Modify data or metadata.
    Modify,
    /// Manage an object, including grants and ownership-sensitive updates.
    Manage,
    /// Use a catalog.
    UseCatalog,
    /// Use a schema.
    UseSchema,
    /// Create a table.
    CreateTable,
    /// Create a volume.
    CreateVolume,
    /// Create a function.
    CreateFunction,
    /// Create a registered model or model version.
    CreateModel,
    /// Mint scoped temporary credentials.
    CredentialMint,
    /// Read governed files.
    ReadFiles,
    /// Write governed files.
    WriteFiles,
}

impl Privilege {
    /// Returns the stable uppercase privilege name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Select => "SELECT",
            Self::Modify => "MODIFY",
            Self::Manage => "MANAGE",
            Self::UseCatalog => "USE_CATALOG",
            Self::UseSchema => "USE_SCHEMA",
            Self::CreateTable => "CREATE_TABLE",
            Self::CreateVolume => "CREATE_VOLUME",
            Self::CreateFunction => "CREATE_FUNCTION",
            Self::CreateModel => "CREATE_MODEL",
            Self::CredentialMint => "CREDENTIAL_MINT",
            Self::ReadFiles => "READ_FILES",
            Self::WriteFiles => "WRITE_FILES",
        }
    }

    /// Returns true when this privilege authorizes `requested`.
    #[must_use]
    pub const fn implies(self, requested: Self) -> bool {
        matches!(self, Self::Manage) || matches!((self, requested), (a, b) if a as u8 == b as u8)
    }
}

impl fmt::Display for Privilege {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for Privilege {
    type Err = String;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_uppercase().as_str() {
            "SELECT" => Ok(Self::Select),
            "MODIFY" => Ok(Self::Modify),
            "MANAGE" => Ok(Self::Manage),
            "USE_CATALOG" | "USE CATALOG" => Ok(Self::UseCatalog),
            "USE_SCHEMA" | "USE SCHEMA" => Ok(Self::UseSchema),
            "CREATE_TABLE" | "CREATE TABLE" => Ok(Self::CreateTable),
            "CREATE_VOLUME" | "CREATE VOLUME" => Ok(Self::CreateVolume),
            "CREATE_FUNCTION" | "CREATE FUNCTION" => Ok(Self::CreateFunction),
            "CREATE_MODEL" | "CREATE MODEL" => Ok(Self::CreateModel),
            "CREDENTIAL_MINT" | "CREDENTIAL MINT" => Ok(Self::CredentialMint),
            "READ_FILES" | "READ FILES" => Ok(Self::ReadFiles),
            "WRITE_FILES" | "WRITE FILES" => Ok(Self::WriteFiles),
            other => Err(format!("unknown privilege '{other}'")),
        }
    }
}
