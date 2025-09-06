use futures::future::join_all;
use gcp_bigquery_client::storage::TableDescriptor;
use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use gcp_bigquery_client::error::BQError;
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use super::{BatchDestination, DestinationError};
use crate::clients::bigquery::table_schema_to_descriptor;
use crate::{
    clients::bigquery::BigQueryClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
};

#[derive(Debug, Error)]
pub enum BigQueryDestinationError {
    #[error("big query error: {0}")]
    BigQuery(#[from] BQError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,

    #[error("schema change detected: {0}")]
    SchemaChangeRestart(String),
}

impl DestinationError for BigQueryDestinationError {}

pub struct BigQueryBatchDestination {
    client: BigQueryClient,
    dataset_id: String,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
    max_staleness_mins: u16,
}

impl BigQueryBatchDestination {
    pub async fn new_with_key_path(
        project_id: String,
        dataset_id: String,
        gcp_sa_key_path: &str,
        max_staleness_mins: u16,
    ) -> Result<BigQueryBatchDestination, BQError> {
        let client = BigQueryClient::new_with_key_path(project_id, gcp_sa_key_path).await?;
        Ok(BigQueryBatchDestination {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
            max_staleness_mins,
        })
    }

    pub async fn new_with_key(
        project_id: String,
        dataset_id: String,
        gcp_sa_key: &str,
        max_staleness_mins: u16,
    ) -> Result<BigQueryBatchDestination, BQError> {
        let client = BigQueryClient::new_with_key(project_id, gcp_sa_key).await?;
        Ok(BigQueryBatchDestination {
            client,
            dataset_id,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
            max_staleness_mins,
        })
    }

    #[expect(clippy::result_large_err)]
    fn get_table_schema(
        &self,
        table_id: TableId,
    ) -> Result<&TableSchema, BigQueryDestinationError> {
        self.table_schemas
            .as_ref()
            .ok_or(BigQueryDestinationError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(BigQueryDestinationError::MissingTableId(table_id))
    }

    fn table_name_in_bq(table_name: &TableName) -> String {
        if table_name.schema == "public" {
            format!("{}", table_name.name)
        } else {
            format!("{}_{}", table_name.schema, table_name.name)
        }
    }
}

#[async_trait]
impl BatchDestination for BigQueryBatchDestination {
    type Error = BigQueryDestinationError;
    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        info!("getting resumption state from bigquery");
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            primary: true,
        }];

        self.client
            .create_table_if_missing(
                &self.dataset_id,
                "_copied_tables",
                &copied_table_column_schemas,
                0,
            )
            .await?;

        let last_lsn_column_schemas = [
            ColumnSchema {
                name: "id".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: true,
            },
            ColumnSchema {
                name: "lsn".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            },
        ];
        if self
            .client
            .create_table_if_missing(&self.dataset_id, "_last_lsn", &last_lsn_column_schemas, 0)
            .await?
        {
            self.client.insert_last_lsn_row(&self.dataset_id).await?;
        }

        let copied_tables = self.client.get_copied_table_ids(&self.dataset_id).await?;
        let last_lsn = self.client.get_last_lsn(&self.dataset_id).await?;

        self.committed_lsn = Some(last_lsn);

        Ok(PipelineResumptionState {
            copied_tables,
            last_lsn,
        })
    }

    async fn write_table_schemas(
        &mut self,
        table_schemas: HashMap<TableId, TableSchema>,
    ) -> Result<(), Self::Error> {
        for table_schema in table_schemas.values() {
            let table_name = Self::table_name_in_bq(&table_schema.name);
            self.client
                .create_or_update_table(
                    &self.dataset_id,
                    &table_name,
                    &table_schema.column_schemas,
                    self.max_staleness_mins,
                )
                .await?;
        }

        self.table_schemas = Some(table_schemas);

        Ok(())
    }

    async fn write_table_rows(
        &mut self,
        mut table_rows: Vec<TableRow>,
        table_id: TableId,
    ) -> Result<(), Self::Error> {
        let table_schema = self.get_table_schema(table_id)?;
        let table_name = Self::table_name_in_bq(&table_schema.name);
        let table_descriptor = table_schema_to_descriptor(table_schema);

        for table_row in &mut table_rows {
            table_row.values.push(Cell::String("UPSERT".to_string()));
            table_row.values.push(Cell::String("0/1".to_string()));
        }

        self.client
            .stream_rows(
                &self.dataset_id,
                &table_name,
                &table_descriptor,
                &table_rows,
            )
            .await?;

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut table_name_to_table_rows = HashMap::new();
        let mut tables_with_deletes = HashSet::new();
        let mut new_last_lsn = PgLsn::from(0);
        for (i, event) in events.into_iter().enumerate() {
            // info!("{event:?}");
            match event {
                CdcEvent::Begin(begin_body) => {
                    let final_lsn_u64 = begin_body.final_lsn();
                    self.final_lsn = Some(final_lsn_u64.into());
                }
                CdcEvent::Commit(commit_body) => {
                    let commit_lsn: PgLsn = commit_body.commit_lsn().into();
                    if let Some(final_lsn) = self.final_lsn {
                        if commit_lsn == final_lsn {
                            new_last_lsn = commit_lsn;
                        } else {
                            Err(BigQueryDestinationError::IncorrectCommitLsn(
                                commit_lsn, final_lsn,
                            ))?
                        }
                    } else {
                        Err(BigQueryDestinationError::CommitWithoutBegin)?
                    }
                }
                CdcEvent::Insert((table_id, mut table_row)) => {
                    table_row.values.push(Cell::String("UPSERT".to_string()));
                    table_row.values.push(Cell::String(format!(
                        "{}/{:X}",
                        self.final_lsn.unwrap().to_string(),
                        i
                    )));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Update((table_id, mut table_row)) => {
                    table_row.values.push(Cell::String("UPSERT".to_string()));
                    table_row.values.push(Cell::String(format!(
                        "{}/{:X}",
                        self.final_lsn.unwrap().to_string(),
                        i
                    )));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Delete((table_id, mut table_row)) => {
                    tables_with_deletes.insert(table_id);
                    table_row.values.push(Cell::String("DELETE".to_string()));
                    table_row.values.push(Cell::String(format!(
                        "{}/{:X}",
                        self.final_lsn.unwrap().to_string(),
                        i
                    )));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Origin(_) => {}
                CdcEvent::Truncate(truncate_body) => {
                    for table_id in truncate_body.rel_ids() {
                        table_name_to_table_rows.remove(table_id);
                        self.truncate_table(*table_id).await?;
                    }
                }
                CdcEvent::Relation(r) => {
                    let relation_column_names: HashSet<_> = r
                        .columns()
                        .iter()
                        .filter_map(|col| col.name().ok())
                        .map(String::from)
                        .collect();

                    let table_name =
                        Self::table_name_in_bq(&self.get_table_schema(r.rel_id())?.name);
                    let existing_columns = self
                        .client
                        .get_table_columns(&self.dataset_id, &table_name)
                        .await?;

                    let new_columns: Vec<_> = relation_column_names
                        .difference(&existing_columns)
                        .collect();

                    if !new_columns.is_empty() {
                        let message = format!("new columns in {}: {:?}", table_name, new_columns);
                        info!("Detected {}, requesting restart", message);
                        return Err(BigQueryDestinationError::SchemaChangeRestart(message));
                    }
                }
                CdcEvent::KeepAliveRequested(keep_alive) => {
                    new_last_lsn = keep_alive.wal_end().into();
                    self.final_lsn = Some(new_last_lsn);
                }
                CdcEvent::Type(_) => {}
            }
        }

        // Gather all the preparation data first
        let prepared_data: Result<
            Vec<(u32, String, TableDescriptor, Vec<TableRow>)>,
            BigQueryDestinationError,
        > = table_name_to_table_rows
            .into_iter()
            .map(
                |(table_id, table_rows)| -> Result<
                    (u32, String, TableDescriptor, Vec<TableRow>),
                    BigQueryDestinationError,
                > {
                    let table_schema = self.get_table_schema(table_id)?;
                    let table_name = Self::table_name_in_bq(&table_schema.name);
                    let table_descriptor = table_schema_to_descriptor(table_schema);
                    Ok((table_id, table_name, table_descriptor, table_rows))
                },
            )
            .collect();

        let prepared_data = prepared_data?;

        // Now run all upsert operations concurrently
        let tasks: Vec<_> = prepared_data
            .into_iter()
            .map(|(table_id, table_name, table_descriptor, table_rows)| {
                let dataset_id = self.dataset_id.clone();
                let mut client = self.client.clone();
                let has_deletes = tables_with_deletes.contains(&table_id);
                async move {
                    client
                        .upsert_rows(
                            &dataset_id,
                            &table_name,
                            &table_descriptor,
                            &table_rows,
                            has_deletes,
                        )
                        .await
                }
            })
            .collect();

        join_all(tasks)
            .await
            .into_iter()
            .collect::<Result<Vec<_>, _>>()?;

        if new_last_lsn != PgLsn::from(0) {
            self.client
                .set_last_lsn(&self.dataset_id, new_last_lsn)
                .await?;
            self.committed_lsn = Some(new_last_lsn);
        }

        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
        Ok(committed_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client
            .insert_into_copied_tables(&self.dataset_id, table_id)
            .await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        if let Some(table_schema) = self.table_schemas.as_ref().and_then(|ts| ts.get(&table_id)) {
            let table_name = Self::table_name_in_bq(&table_schema.name);
            self.client
                .truncate_table(&self.dataset_id, &table_name)
                .await?;
        }
        Ok(())
    }
}
