//! Immutable catalog read models pinned to published snapshot identity.

use std::collections::HashMap;

use arco_core::{CatalogDomain, CatalogPaths, ScopedStorage};

use crate::error::{CatalogError, Result};
use crate::manifest::CatalogDomainManifest;
use crate::parquet_util;
use crate::writer::{Catalog, Column, Schema, Table};

/// Stable identity for one published catalog snapshot.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct CatalogSnapshotIdentity {
    snapshot_version: u64,
    manifest_id: String,
    last_commit_id: Option<String>,
}

impl CatalogSnapshotIdentity {
    /// Builds a snapshot identity from the immutable catalog manifest metadata.
    #[must_use]
    pub fn from_manifest(manifest: &CatalogDomainManifest) -> Self {
        Self {
            snapshot_version: manifest.snapshot_version,
            manifest_id: manifest.manifest_id.clone(),
            last_commit_id: manifest.last_commit_id.clone(),
        }
    }
}

impl From<&CatalogDomainManifest> for CatalogSnapshotIdentity {
    fn from(manifest: &CatalogDomainManifest) -> Self {
        Self::from_manifest(manifest)
    }
}

/// Immutable, decoded catalog snapshot state for hot metadata reads.
#[derive(Debug, Clone)]
pub struct CatalogReadModel {
    snapshot_version: u64,
    catalogs: Vec<Catalog>,
    namespaces: Vec<Schema>,
    catalogs_by_name: HashMap<String, Catalog>,
    namespaces_by_name: HashMap<String, Schema>,
    tables_by_id: HashMap<String, Table>,
    tables_by_namespace_id: HashMap<String, Vec<Table>>,
    columns_by_table_id: HashMap<String, Vec<Column>>,
}

impl CatalogReadModel {
    /// Loads and decodes all catalog snapshot files for one manifest.
    pub async fn load(storage: &ScopedStorage, manifest: &CatalogDomainManifest) -> Result<Self> {
        if manifest.snapshot_version == 0 {
            return Ok(Self::from_parts(0, Vec::new(), Vec::new(), &[], Vec::new()));
        }

        let version = manifest.snapshot_version;
        let catalogs_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "catalogs.parquet");
        let namespaces_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "namespaces.parquet");
        let tables_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "tables.parquet");
        let columns_path =
            CatalogPaths::snapshot_file(CatalogDomain::Catalog, version, "columns.parquet");

        let (catalog_bytes, namespace_bytes, table_bytes, column_bytes) = tokio::try_join!(
            storage.get_raw(&catalogs_path),
            storage.get_raw(&namespaces_path),
            storage.get_raw(&tables_path),
            storage.get_raw(&columns_path),
        )?;

        let catalogs = parquet_util::read_catalogs(&catalog_bytes)?
            .into_iter()
            .map(Catalog::try_from)
            .collect::<Result<Vec<_>>>()?;

        let namespaces = parquet_util::read_namespaces(&namespace_bytes)?
            .into_iter()
            .map(Schema::try_from)
            .collect::<Result<Vec<_>>>()?;

        let tables = parquet_util::read_tables(&table_bytes)?
            .into_iter()
            .map(Table::try_from)
            .collect::<Result<Vec<_>>>()?;

        let columns = parquet_util::read_columns(&column_bytes)?
            .into_iter()
            .map(Column::from)
            .collect::<Vec<_>>();

        Ok(Self::from_parts(
            version, catalogs, namespaces, &tables, columns,
        ))
    }

    /// Builds an immutable read model from already-decoded snapshot parts.
    #[must_use]
    pub fn from_parts(
        snapshot_version: u64,
        catalogs: Vec<Catalog>,
        namespaces: Vec<Schema>,
        tables: &[Table],
        columns: Vec<Column>,
    ) -> Self {
        let catalogs_by_name = catalogs
            .iter()
            .map(|catalog| (catalog.name.clone(), catalog.clone()))
            .collect::<HashMap<_, _>>();

        let mut namespaces_by_name = HashMap::new();
        for namespace in &namespaces {
            namespaces_by_name
                .entry(namespace.name.clone())
                .or_insert_with(|| namespace.clone());
        }

        let tables_by_id = tables
            .iter()
            .map(|table| (table.id.clone(), table.clone()))
            .collect::<HashMap<_, _>>();

        let mut tables_by_namespace_id: HashMap<String, Vec<Table>> = HashMap::new();
        for table in tables {
            tables_by_namespace_id
                .entry(table.namespace_id.clone())
                .or_default()
                .push(table.clone());
        }

        let mut columns_by_table_id: HashMap<String, Vec<Column>> = HashMap::new();
        for column in columns {
            columns_by_table_id
                .entry(column.table_id.clone())
                .or_default()
                .push(column);
        }
        for columns in columns_by_table_id.values_mut() {
            columns.sort_by_key(|column| column.ordinal);
        }

        Self {
            snapshot_version,
            catalogs,
            namespaces,
            catalogs_by_name,
            namespaces_by_name,
            tables_by_id,
            tables_by_namespace_id,
            columns_by_table_id,
        }
    }

    /// Lists catalogs from this snapshot.
    #[must_use]
    pub fn list_catalogs(&self) -> Vec<Catalog> {
        self.catalogs.clone()
    }

    /// Gets a catalog by name from this snapshot.
    #[must_use]
    pub fn get_catalog(&self, name: &str) -> Option<Catalog> {
        self.catalogs_by_name.get(name).cloned()
    }

    /// Lists all namespaces from this snapshot.
    #[must_use]
    pub fn list_namespaces(&self) -> Vec<Schema> {
        self.namespaces.clone()
    }

    /// Gets a namespace by name from this snapshot.
    #[must_use]
    pub fn get_namespace(&self, name: &str) -> Option<Schema> {
        self.namespaces_by_name.get(name).cloned()
    }

    /// Lists schemas within a catalog while preserving legacy default-catalog behavior.
    pub fn list_schemas(&self, catalog: &str) -> Result<Vec<Schema>> {
        if self.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        }

        let default_catalog_id = self
            .catalogs_by_name
            .get("default")
            .map(|catalog| catalog.id.as_str());
        let requested_catalog_id = self
            .catalogs_by_name
            .get(catalog)
            .map(|catalog| catalog.id.as_str());

        let effective_requested_catalog_id = if let Some(id) = requested_catalog_id {
            Some(id)
        } else if catalog == "default" {
            default_catalog_id
        } else {
            return Err(CatalogError::NotFound {
                entity: "catalog".into(),
                name: catalog.to_string(),
            });
        };

        Ok(self
            .namespaces
            .iter()
            .filter(|namespace| {
                effective_requested_catalog_id.map_or_else(
                    || namespace.catalog_id.is_none(),
                    |requested| {
                        namespace.catalog_id.as_deref().or(default_catalog_id) == Some(requested)
                    },
                )
            })
            .cloned()
            .collect())
    }

    /// Lists tables by namespace identifier.
    #[must_use]
    pub fn list_tables_for_namespace_id(&self, namespace_id: &str) -> Vec<Table> {
        self.tables_by_namespace_id
            .get(namespace_id)
            .cloned()
            .unwrap_or_default()
    }

    /// Lists tables by legacy namespace name.
    pub fn list_tables(&self, namespace: &str) -> Result<Vec<Table>> {
        if self.snapshot_version == 0 {
            return Err(CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            });
        }

        let namespace_id = self
            .namespaces_by_name
            .get(namespace)
            .map(|namespace| namespace.id.as_str())
            .ok_or_else(|| CatalogError::NotFound {
                entity: "namespace".into(),
                name: namespace.to_string(),
            })?;

        Ok(self.list_tables_for_namespace_id(namespace_id))
    }

    /// Lists tables within a schema in a catalog.
    pub fn list_tables_in_schema(&self, catalog: &str, schema: &str) -> Result<Vec<Table>> {
        let namespace_id = self
            .list_schemas(catalog)?
            .iter()
            .find(|namespace| namespace.name == schema)
            .map(|namespace| namespace.id.clone())
            .ok_or_else(|| CatalogError::NotFound {
                entity: "schema".into(),
                name: format!("{catalog}.{schema}"),
            })?;
        Ok(self.list_tables_for_namespace_id(namespace_id.as_str()))
    }

    /// Gets a table by legacy namespace name and table name.
    pub fn get_table(&self, namespace: &str, name: &str) -> Result<Option<Table>> {
        let tables = self.list_tables(namespace)?;
        Ok(tables.into_iter().find(|table| table.name == name))
    }

    /// Gets a table by catalog, schema, and table name.
    pub fn get_table_in_schema(
        &self,
        catalog: &str,
        schema: &str,
        table: &str,
    ) -> Result<Option<Table>> {
        let tables = self.list_tables_in_schema(catalog, schema)?;
        Ok(tables.into_iter().find(|candidate| candidate.name == table))
    }

    /// Gets a table by stable table identifier.
    #[must_use]
    pub fn get_table_by_id(&self, table_id: &str) -> Option<Table> {
        self.tables_by_id.get(table_id).cloned()
    }

    /// Lists columns for a table by stable table identifier.
    #[must_use]
    pub fn get_columns(&self, table_id: &str) -> Vec<Column> {
        self.columns_by_table_id
            .get(table_id)
            .cloned()
            .unwrap_or_default()
    }
}
