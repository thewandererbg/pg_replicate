use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::{
    clients::databricks::{DatabricksClient, DatabricksError},
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
};
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use postgres_replication::protocol::RelationBody;

use super::{BatchDestination, DestinationError};

#[derive(Debug, Error)]
pub enum DatabricksDestinationError {
    #[error("databricks error: {0}")]
    Databricks(#[from] DatabricksError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0} (expected: {1})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,

    #[error("schema change detected: {0}")]
    SchemaChangeRestart(String),

    #[error("merge operation failed: {0}")]
    MergeOperationFailed(String),

    #[error("missing table clustering columns")]
    MissingTableClusteringColumns,
}

impl DestinationError for DatabricksDestinationError {}

pub struct DatabricksBatchDestination {
    client: DatabricksClient,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
    table_clustering_columns: Option<HashMap<String, Vec<String>>>,
}

impl DatabricksBatchDestination {
    /// Creates a new Databricks batch destination with access token authentication
    pub async fn new_with_access_token(
        workspace_url: String,
        warehouse_id: String,
        access_token: String,
        catalog: String,
        schema: String,
        s3_access_key: Option<String>,
        s3_secret_key: Option<String>,
        s3_region: Option<String>,
        s3_bucket: Option<String>,
    ) -> Result<Self, DatabricksError> {
        let client = DatabricksClient::new_with_access_token(
            workspace_url,
            warehouse_id,
            access_token,
            catalog,
            schema,
            s3_access_key,
            s3_secret_key,
            s3_region,
            s3_bucket,
        )
        .await?;

        Ok(Self {
            client,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
            table_clustering_columns: None,
        })
    }

    /// Gets table schema by table ID with proper error handling
    fn get_table_schema(
        &self,
        table_id: TableId,
    ) -> Result<&TableSchema, DatabricksDestinationError> {
        self.table_schemas
            .as_ref()
            .ok_or(DatabricksDestinationError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(DatabricksDestinationError::MissingTableId(table_id))
    }

    /// Gets table clustering columns by table name
    fn get_clustering_columns(&self, table_name: &str) -> Vec<&str> {
        self.table_clustering_columns
            .as_ref()
            .and_then(|map| map.get(table_name))
            .map(|vec| vec.iter().map(|s| s.as_str()).collect())
            .unwrap_or_else(Vec::new)
    }

    /// Returns the table name in databricks
    fn table_name(table_name: &TableName) -> String {
        if table_name.schema == "public" {
            table_name.name.clone()
        } else {
            format!("{}_{}", table_name.schema, table_name.name)
        }
    }

    /// Creates the CDC columns
    fn create_cdc_columns_schemas(
        &self,
        table_id: TableId,
    ) -> Result<Vec<ColumnSchema>, DatabricksDestinationError> {
        let table_schema = self.get_table_schema(table_id)?;
        let mut column_schemas = table_schema.column_schemas.clone();

        column_schemas.push(ColumnSchema {
            name: "_operation".to_string(),
            typ: Type::TEXT,
            modifier: 0,
            nullable: false,
            primary: false,
        });

        column_schemas.push(ColumnSchema {
            name: "_version".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            primary: false,
        });

        Ok(column_schemas)
    }

    /// Add CDC metadata columns to a table row
    fn add_cdc_metadata(&self, table_row: &mut TableRow, operation: &str, _version: i64) {
        table_row.values.push(Cell::String(operation.to_string()));
        table_row.values.push(Cell::I64(_version));
    }

    async fn merge_rows(
        &mut self,
        table_id: TableId,
        table_rows: Vec<TableRow>,
    ) -> Result<(), DatabricksDestinationError> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name(&table_schema.name);
        let columns_schemas = self.create_cdc_columns_schemas(table_id)?;
        let clustering_columns = self.get_clustering_columns(&table_name);

        self.client
            .merge_rows_to_table(
                table_schema,
                &columns_schemas,
                table_rows,
                &table_name,
                &clustering_columns,
            )
            .await?;
        Ok(())
    }

    /// Handle schema changes (detect new columns and request restart)
    fn handle_schema_change(
        &self,
        relation: &RelationBody,
    ) -> Result<(), DatabricksDestinationError> {
        // info!("Handling schema change for {:?}", relation.name().ok());

        let relation_columns: HashSet<String> = relation
            .columns()
            .iter()
            .filter_map(|col| col.name().ok())
            .map(String::from)
            .collect();

        if let Ok(table_schema) = self.get_table_schema(relation.rel_id()) {
            let existing_columns: HashSet<String> = table_schema
                .column_schemas
                .iter()
                .map(|col| col.name.clone())
                .collect();

            let new_columns: Vec<_> = relation_columns.difference(&existing_columns).collect();

            if !new_columns.is_empty() {
                let table_name = Self::table_name(&table_schema.name);
                let message = format!("new columns in {}: {:?}", table_name, new_columns);
                info!("Schema change detected: {}", message);
                return Err(DatabricksDestinationError::SchemaChangeRestart(message));
            }
        }

        Ok(())
    }
}

#[async_trait]
impl BatchDestination for DatabricksBatchDestination {
    type Error = DatabricksDestinationError;

    /// Initialize and get the current state of the CDC pipeline
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        info!("Getting resumption state from Databricks");

        // Create CDC tracking tables (_last_lsn and _copied_tables)
        self.client.create_cdc_tracking_tables().await?;

        let copied_tables = self.client.get_copied_table_ids().await?;

        let last_lsn = self
            .client
            .get_last_lsn()
            .await
            .unwrap_or_else(|_| PgLsn::from(0));

        self.committed_lsn = Some(last_lsn);

        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    /// Create both bronze and silver tables for all schemas
    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        info!("Creating {} tables in databricks", table_schemas.len());
        self.table_schemas = Some(table_schemas.clone());

        let mut table_clustering_columns: HashMap<String, Vec<String>> = HashMap::new();

        for (table_id, table_schema) in table_schemas.clone() {
            let table_name = Self::table_name(&table_schema.name);
            let table_schema = self.get_table_schema(table_id)?;
            let mut column_schemas = vec![ColumnSchema {
                name: "_version".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            }];

            column_schemas.extend(table_schema.column_schemas.clone());

            let created = self
                .client
                .create_or_update_table(&table_name, &column_schemas)
                .await?;

            if created {
                info!("Created table {}", table_name);
            }

            let cluster_columns = self.client.get_cluster_columns(&table_name).await?;

            table_clustering_columns.insert(table_name, cluster_columns);
        }

        info!(
            "Retrieved clustering columns for tables: {:?}",
            table_clustering_columns
        );
        self.table_clustering_columns = Some(table_clustering_columns);

        Ok(())
    }

    /// Write initial table data directly to table (clean production data)
    async fn write_table_rows(
        &mut self,
        mut table_rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name(&table_schema.name);
        let mut column_schemas = table_schema.column_schemas.clone();

        column_schemas.push(ColumnSchema {
            name: "_version".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            primary: false,
        });

        info!("Writing {} rows to table {}", table_rows.len(), table_name);

        let _version = Utc::now().timestamp_micros();
        for table_row in table_rows.iter_mut() {
            table_row.values.push(Cell::I64(_version));
        }

        self.client
            .insert_rows(&table_name, &column_schemas, &table_rows)
            .await?;

        Ok(())
    }

    /// Process CDC events and write to bronze tables
    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        if events.is_empty() {
            return Ok(self.committed_lsn.unwrap_or_else(|| PgLsn::from(0)));
        }

        let mut table_events: HashMap<TableId, Vec<TableRow>> = HashMap::new();
        let mut new_last_lsn = PgLsn::from(0);
        let mut _version: i64 = 0;

        // Process all CDC events and group by table
        for event in events {
            match event {
                CdcEvent::Begin(begin_body) => {
                    let final_lsn_u64 = begin_body.final_lsn();
                    self.final_lsn = Some(final_lsn_u64.into());
                    _version = begin_body.timestamp() + 946684800000000; // postgre microseconds to unix microseconds
                }

                CdcEvent::Commit(commit_body) => {
                    let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                    match self.final_lsn {
                        Some(final_lsn) if commit_lsn == final_lsn => {
                            new_last_lsn = commit_lsn;
                        }
                        Some(final_lsn) => {
                            return Err(DatabricksDestinationError::IncorrectCommitLsn(
                                commit_lsn, final_lsn,
                            ));
                        }
                        None => {
                            return Err(DatabricksDestinationError::CommitWithoutBegin);
                        }
                    }
                }

                CdcEvent::Insert((table_id, mut table_row)) => {
                    self.add_cdc_metadata(&mut table_row, "INSERT", _version);
                    table_events.entry(table_id).or_default().push(table_row);
                }

                CdcEvent::Update((table_id, mut table_row)) => {
                    self.add_cdc_metadata(&mut table_row, "UPDATE", _version);
                    table_events.entry(table_id).or_default().push(table_row);
                }

                CdcEvent::Delete((table_id, mut table_row)) => {
                    self.add_cdc_metadata(&mut table_row, "DELETE", _version);
                    table_events.entry(table_id).or_default().push(table_row);
                }

                CdcEvent::Truncate(truncate_body) => {
                    for &table_id in truncate_body.rel_ids() {
                        table_events.remove(&table_id);
                        self.truncate_table(table_id).await?;
                    }
                }

                CdcEvent::Relation(relation) => {
                    self.handle_schema_change(&relation)?;
                }

                CdcEvent::KeepAliveRequested(keep_alive) => {
                    new_last_lsn = keep_alive.wal_end().into();
                    self.final_lsn = Some(new_last_lsn);
                }

                // These events don't require specific handling in our CDC pipeline
                CdcEvent::Origin(_) | CdcEvent::Type(_) => {}
            }
        }

        // Batch insert all events into bronze tables
        for (table_id, table_rows) in table_events {
            if !table_rows.is_empty() {
                self.merge_rows(table_id, table_rows).await?;
            }
        }

        // Update the last processed LSN if we have a new one
        if new_last_lsn != PgLsn::from(0) {
            self.client.set_last_lsn(new_last_lsn).await?;
            self.committed_lsn = Some(new_last_lsn);
        }

        Ok(self
            .committed_lsn
            .expect("committed_lsn should always be set"))
    }

    /// Mark a table as fully copied
    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client.insert_into_copied_tables(table_id).await?;
        Ok(())
    }

    /// Truncate tables
    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name(&table_schema.name);

        self.client.truncate_table(&table_name).await?;

        Ok(())
    }
}
