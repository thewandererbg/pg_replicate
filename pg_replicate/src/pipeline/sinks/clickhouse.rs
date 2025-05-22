use std::collections::{HashMap, HashSet};

use async_trait::async_trait;
use chrono::Utc;
use clickhouse::error::Error as CHError;
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::{
    clients::clickhouse::ClickHouseClient,
    conversions::{cdc_event::CdcEvent, table_row::TableRow, Cell},
    pipeline::PipelineResumptionState,
};
use postgres::schema::{ColumnSchema, TableId, TableName, TableSchema};

use super::{BatchSink, SinkError};

#[derive(Debug, Error)]
pub enum ClickHouseSinkError {
    #[error("clickhouse error: {0}")]
    ClickHouse(#[from] CHError),

    #[error("missing table schemas")]
    MissingTableSchemas,

    #[error("missing table id: {0}")]
    MissingTableId(TableId),

    #[error("incorrect commit lsn: {0}(expected: {0})")]
    IncorrectCommitLsn(PgLsn, PgLsn),

    #[error("commit message without begin message")]
    CommitWithoutBegin,
}

impl SinkError for ClickHouseSinkError {}

pub struct ClickHouseBatchSink {
    client: ClickHouseClient,
    table_schemas: Option<HashMap<TableId, TableSchema>>,
    committed_lsn: Option<PgLsn>,
    final_lsn: Option<PgLsn>,
}

impl ClickHouseBatchSink {
    pub async fn new_with_credentials(
        url: String,
        database: String,
        username: String,
        password: String,
    ) -> Result<ClickHouseBatchSink, CHError> {
        let client =
            ClickHouseClient::new_with_credentials(&url, database.clone(), &username, &password)
                .await?;

        Ok(ClickHouseBatchSink {
            client,
            table_schemas: None,
            committed_lsn: None,
            final_lsn: None,
        })
    }

    fn get_table_schema(&self, table_id: TableId) -> Result<&TableSchema, ClickHouseSinkError> {
        self.table_schemas
            .as_ref()
            .ok_or(ClickHouseSinkError::MissingTableSchemas)?
            .get(&table_id)
            .ok_or(ClickHouseSinkError::MissingTableId(table_id))
    }

    fn table_name_in_ch(table_name: &TableName) -> String {
        if table_name.schema == "public" {
            format!("{}", table_name.name)
        } else {
            format!("{}_{}", table_name.schema, table_name.name)
        }
    }
}

#[async_trait]
impl BatchSink for ClickHouseBatchSink {
    type Error = ClickHouseSinkError;

    async fn get_resumption_state(&mut self) -> Result<PipelineResumptionState, Self::Error> {
        info!("getting resumption state from clickhouse");
        let copied_table_column_schemas = [ColumnSchema {
            name: "table_id".to_string(),
            typ: Type::INT4,
            modifier: 0,
            nullable: false,
            primary: true,
        }];

        self.client
            .create_table_if_missing("_copied_tables", &copied_table_column_schemas, "MergeTree")
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
            .create_table_if_missing("_last_lsn", &last_lsn_column_schemas, "MergeTree")
            .await?
        {
            self.client.insert_last_lsn_row().await?;
        }

        let copied_tables = self.client.get_copied_table_ids().await?;
        let last_lsn = self.client.get_last_lsn().await?;

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
            let table_name = Self::table_name_in_ch(&table_schema.table_name);
            self.client
                .create_or_update_table(
                    &table_name,
                    &table_schema.column_schemas,
                    "ReplacingMergeTree",
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
        let table_name = Self::table_name_in_ch(&table_schema.table_name);
        let mut column_schemas = table_schema.column_schemas.clone();

        column_schemas.push(ColumnSchema {
            name: "_version".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            primary: false,
        });

        column_schemas.push(ColumnSchema {
            name: "_is_deleted".to_string(),
            typ: Type::INT8,
            modifier: 0,
            nullable: false,
            primary: false,
        });

        // Add version and tombstone marker to each row
        let _version = Utc::now().timestamp_millis();
        for table_row in table_rows.iter_mut() {
            table_row.values.push(Cell::I64(_version));
            table_row.values.push(Cell::I64(0));
        }

        self.client
            .insert_rows(&table_name, &column_schemas, &table_rows)
            .await?;

        Ok(())
    }

    async fn write_cdc_events(&mut self, events: Vec<CdcEvent>) -> Result<PgLsn, Self::Error> {
        let mut table_name_to_table_rows = HashMap::new();
        let mut new_last_lsn = PgLsn::from(0);

        let _version = Utc::now().timestamp_millis();
        for event in events {
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
                            Err(ClickHouseSinkError::IncorrectCommitLsn(
                                commit_lsn, final_lsn,
                            ))?
                        }
                    } else {
                        Err(ClickHouseSinkError::CommitWithoutBegin)?
                    }
                }
                CdcEvent::Insert((table_id, mut table_row)) => {
                    table_row.values.push(Cell::I64(_version));
                    table_row.values.push(Cell::I64(0));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Update((table_id, mut table_row)) => {
                    table_row.values.push(Cell::I64(_version));
                    table_row.values.push(Cell::I64(0));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Delete((table_id, mut table_row)) => {
                    table_row.values.push(Cell::I64(_version));
                    table_row.values.push(Cell::I64(1));
                    let table_rows: &mut Vec<TableRow> =
                        table_name_to_table_rows.entry(table_id).or_default();
                    table_rows.push(table_row);
                }
                CdcEvent::Origin(_) => {}
                CdcEvent::Truncate(truncate_body) => {
                    for table_id in truncate_body.rel_ids() {
                        let table_schema = self.get_table_schema(*table_id)?;
                        let table_name = Self::table_name_in_ch(&table_schema.table_name);
                        self.client.truncate_table(&table_name).await?;
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
                        Self::table_name_in_ch(&self.get_table_schema(r.rel_id())?.table_name);
                    let existing_columns = self.client.get_table_columns(&table_name).await?;

                    let new_columns: Vec<_> = relation_column_names
                        .difference(&existing_columns)
                        .collect();

                    if !new_columns.is_empty() {
                        info!(
                            "Detected new columns in {}: {:?}, restarting the replication",
                            table_name, new_columns
                        );
                        std::process::exit(0);
                    }
                }

                CdcEvent::KeepAliveRequested { reply: _ } => {}
                CdcEvent::Type(_) => {}
            }
        }

        // Process batched operations for each table
        for (table_id, table_rows) in table_name_to_table_rows {
            let table_schema = self.get_table_schema(table_id)?;
            let table_name = Self::table_name_in_ch(&table_schema.table_name);
            let mut column_schemas = table_schema.column_schemas.clone();

            column_schemas.push(ColumnSchema {
                name: "_version".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            });

            column_schemas.push(ColumnSchema {
                name: "_is_deleted".to_string(),
                typ: Type::INT8,
                modifier: 0,
                nullable: false,
                primary: false,
            });

            self.client
                .insert_rows(&table_name, &column_schemas, &table_rows)
                .await?
        }

        // Update LSN if needed
        if new_last_lsn != PgLsn::from(0) {
            self.client.set_last_lsn(new_last_lsn).await?;
            self.committed_lsn = Some(new_last_lsn);
        }

        let committed_lsn = self.committed_lsn.expect("committed lsn is none");
        Ok(committed_lsn)
    }

    async fn table_copied(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        self.client.insert_into_copied_tables(table_id).await?;
        Ok(())
    }

    async fn truncate_table(&mut self, table_id: TableId) -> Result<(), Self::Error> {
        if let Some(table_schema) = self.table_schemas.as_ref().and_then(|ts| ts.get(&table_id)) {
            let table_name = Self::table_name_in_ch(&table_schema.table_name);
            self.client.truncate_table(&table_name).await?;
        }
        Ok(())
    }
}
