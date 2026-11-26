//! Operator-approved schemas and table bindings for Arco event streams.

use std::collections::BTreeSet;
use std::sync::LazyLock;

use arco_core::{ControlPlaneScope, ScopedStorage};
use arrow::datatypes::DataType;
use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::state::ActivityTableBindings;

pub const LOGFOOD_STREAM: &str = "query-activity";

/// Checks whether a Delta table has durable event-stream ownership in this scope.
///
/// # Errors
/// Returns an error if the owner record cannot be checked.
pub async fn is_event_stream_table(
    storage: &ScopedStorage,
    table_id: Uuid,
) -> arco_core::Result<bool> {
    Ok(storage
        .head_raw(&event_stream_table_path(table_id))
        .await?
        .is_some())
}

pub fn event_stream_table_path(table_id: Uuid) -> String {
    format!("event-streams/v1/tables/{table_id}.sha256")
}

/// Operator-supplied event stream bindings. Each Workspace stream owns one table.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(transparent)]
pub struct EventStreamBindings(Vec<StreamBinding>);

impl<'de> Deserialize<'de> for EventStreamBindings {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Self::new(Vec::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }
}

impl EventStreamBindings {
    fn new(bindings: Vec<StreamBinding>) -> Result<Self, String> {
        let mut names = BTreeSet::new();
        let mut tables = BTreeSet::new();
        for binding in &bindings {
            let scope = (binding.tenant.as_str(), binding.workspace.as_str());
            if ControlPlaneScope::workspace_alias(scope.0, scope.1).is_err()
                || !valid_name(&binding.stream_id, true)
                || binding.table_id.is_nil()
                || (binding.stream_id == LOGFOOD_STREAM) != binding.schema.is_logfood()
                || !binding.schema.valid_definition()
            {
                return Err("invalid event stream binding or schema".to_string());
            }
            if !names.insert((scope, binding.stream_id.as_str()))
                || !tables.insert((scope, binding.table_id))
            {
                return Err(
                    "a Workspace event stream and its table must each be unique".to_string()
                );
            }
        }
        Ok(Self(bindings))
    }

    /// Includes the existing Logfood table binding as the `query-activity` stream.
    ///
    /// # Errors
    /// Returns an error when the scope or merged binding is invalid or ambiguous.
    pub fn with_activity_tables(&self, activity: &ActivityTableBindings) -> Result<Self, String> {
        let mut bindings = self.0.clone();
        for (scope, table_id) in &activity.0 {
            let (tenant, workspace) = scope
                .split_once('/')
                .ok_or_else(|| "activity table bindings use tenant/workspace keys".to_string())?;
            let binding = StreamBinding {
                tenant: tenant.to_string(),
                workspace: workspace.to_string(),
                stream_id: LOGFOOD_STREAM.to_string(),
                table_id: *table_id,
                schema: EventSchema::LogfoodV1,
            };
            if !bindings.contains(&binding) {
                bindings.push(binding);
            }
        }
        Self::new(bindings)
    }

    /// Returns whether any event streams have been approved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns whether the Logfood-compatible stream is bound in any Workspace.
    #[must_use]
    pub fn has_logfood(&self) -> bool {
        self.0.iter().any(|binding| binding.schema.is_logfood())
    }

    /// Returns the tables owned by event streams in this exact scope.
    pub fn table_ids<'a>(
        &'a self,
        tenant: &'a str,
        workspace: &'a str,
    ) -> impl Iterator<Item = Uuid> + 'a {
        self.0
            .iter()
            .filter(move |binding| binding.matches_scope(tenant, workspace))
            .map(|binding| binding.table_id)
    }

    pub(crate) fn resolve(
        &self,
        tenant: &str,
        workspace: &str,
        stream_id: &str,
    ) -> Option<&StreamBinding> {
        self.0.iter().find(|binding| {
            binding.stream_id == stream_id && binding.matches_scope(tenant, workspace)
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamBinding {
    tenant: String,
    workspace: String,
    pub(crate) stream_id: String,
    pub(crate) table_id: Uuid,
    pub(crate) schema: EventSchema,
}

impl StreamBinding {
    fn matches_scope(&self, tenant: &str, workspace: &str) -> bool {
        self.tenant == tenant && self.workspace == workspace
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "profile", rename_all = "snake_case", deny_unknown_fields)]
pub enum EventSchema {
    LogfoodV1,
    Custom {
        version: i32,
        fields: Vec<EventField>,
    },
}

impl EventSchema {
    pub(crate) const fn is_logfood(&self) -> bool {
        matches!(self, Self::LogfoodV1)
    }

    pub(crate) const fn version(&self) -> i32 {
        match self {
            Self::LogfoodV1 => 1,
            Self::Custom { version, .. } => *version,
        }
    }

    pub(crate) fn fields(&self) -> &[EventField] {
        match self {
            Self::LogfoodV1 => &LOGFOOD_FIELDS,
            Self::Custom { fields, .. } => fields,
        }
    }

    fn valid_definition(&self) -> bool {
        let mut names = BTreeSet::new();
        self.version() > 0
            && self.fields().len() <= 32
            && self.fields().iter().all(|field| {
                valid_name(&field.name, false)
                    && !matches!(
                        field.name.as_str(),
                        "source_event_id"
                            | "schema_version"
                            | "occurred_at_ms"
                            | "workspace_id"
                            | "commit_version"
                    )
                    && names.insert(field.name.as_str())
                    && field.valid_definition()
            })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EventField {
    pub(crate) name: String,
    #[serde(rename = "type")]
    pub(crate) kind: EventFieldType,
    #[serde(default)]
    pub(crate) nullable: bool,
    #[serde(default)]
    values: BTreeSet<String>,
}

impl EventField {
    fn valid_definition(&self) -> bool {
        match self.kind {
            EventFieldType::Enum => {
                !self.values.is_empty() && self.values.iter().all(|value| valid_id(value))
            }
            _ => self.values.is_empty(),
        }
    }

    pub(crate) fn admits(&self, value: &Value) -> bool {
        if value.is_null() {
            return self.nullable;
        }
        match self.kind {
            EventFieldType::Identifier => value.as_str().is_some_and(valid_id),
            EventFieldType::Enum => value
                .as_str()
                .is_some_and(|value| self.values.contains(value)),
            EventFieldType::Integer => value.as_i64().is_some(),
            EventFieldType::Boolean => value.is_boolean(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum EventFieldType {
    Identifier,
    Enum,
    Integer,
    Boolean,
}

impl EventFieldType {
    pub(crate) const fn column_types(self) -> (&'static str, DataType) {
        match self {
            Self::Identifier | Self::Enum => ("string", DataType::Utf8),
            Self::Integer => ("long", DataType::Int64),
            Self::Boolean => ("boolean", DataType::Boolean),
        }
    }
}

static LOGFOOD_FIELDS: LazyLock<Vec<EventField>> = LazyLock::new(|| {
    [
        "stage",
        "outcome",
        "surface_id",
        "release_id",
        "operation_id",
    ]
    .into_iter()
    .map(|name| EventField {
        name: name.to_string(),
        kind: EventFieldType::Identifier,
        nullable: name == "release_id",
        values: BTreeSet::new(),
    })
    .collect()
});

fn valid_name(name: &str, allow_dash: bool) -> bool {
    name.len() <= 64
        && name.as_bytes().first().is_some_and(u8::is_ascii_lowercase)
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase()
                || byte.is_ascii_digit()
                || byte == b'_'
                || (allow_dash && byte == b'-')
        })
}

fn valid_id(id: &str) -> bool {
    id.len() <= 128
        && id.as_bytes().first().is_some_and(u8::is_ascii_alphanumeric)
        && id
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}
