use std::{collections::HashSet, fs};

use bytes::{Buf, BufMut};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use futures::StreamExt;
use gcp_bigquery_client::error::NestedResponseError;
use gcp_bigquery_client::model::table::Table;
use gcp_bigquery_client::storage::{ColumnMode, StorageApi};
use gcp_bigquery_client::yup_oauth2::parse_service_account_key;
use gcp_bigquery_client::{
    error::BQError,
    google::cloud::bigquery::storage::v1::{WriteStream, WriteStreamView},
    model::{
        query_request::QueryRequest, query_response::ResultSet,
        table_data_insert_all_request::TableDataInsertAllRequest,
    },
    storage::{ColumnType, FieldDescriptor, StreamName, TableDescriptor},
    Client,
};
use postgres::schema::{ColumnSchema, TableId, TableSchema};
use prost::Message;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;
use uuid::Uuid;

use crate::conversions::numeric::PgNumeric;
use crate::conversions::table_row::TableRow;
use crate::conversions::{ArrayCell, Cell};

#[derive(Clone)]
pub struct BigQueryClient {
    project_id: String,
    client: Client,
}

//TODO: fix all SQL injections
impl BigQueryClient {
    pub async fn new_with_key_path(
        project_id: String,
        gcp_sa_key_path: &str,
    ) -> Result<BigQueryClient, BQError> {
        let gcp_sa_key = fs::read_to_string(gcp_sa_key_path)?;
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn new_with_key(
        project_id: String,
        gcp_sa_key: &str,
    ) -> Result<BigQueryClient, BQError> {
        let service_account_key = parse_service_account_key(gcp_sa_key)?;
        let client = Client::from_service_account_key(service_account_key, false).await?;

        Ok(BigQueryClient { project_id, client })
    }

    pub async fn create_table_if_missing(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<bool, BQError> {
        if self.table_exists(dataset_id, table_name).await? {
            Ok(false)
        } else {
            self.create_table(dataset_id, table_name, column_schemas, max_staleness_mins)
                .await?;
            Ok(true)
        }
    }

    pub async fn create_or_update_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<bool, BQError> {
        if self.table_exists(dataset_id, table_name).await? {
            // If table exists, check for new columns
            let existing_columns = self.get_table_columns(dataset_id, table_name).await?;

            // Find columns that exist in the new schema but not in the table
            let new_columns: Vec<&ColumnSchema> = column_schemas
                .iter()
                .filter(|col| !existing_columns.contains(&col.name))
                .collect();

            // If there are new columns, add them to the table
            if !new_columns.is_empty() {
                for column in &new_columns {
                    self.add_column_to_table(dataset_id, table_name, column)
                        .await?;
                }
            }
            Ok(false)
        } else {
            self.create_table(dataset_id, table_name, column_schemas, max_staleness_mins)
                .await?;
            Ok(true)
        }
    }

    fn postgres_to_bigquery_type(typ: &Type) -> &'static str {
        match typ {
            &Type::BOOL => "bool",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "string",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "int64",
            &Type::FLOAT4 | &Type::FLOAT8 => "float64",
            &Type::NUMERIC => "float64",
            &Type::DATE => "date",
            &Type::TIME => "time",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "timestamp",
            &Type::UUID => "string",
            &Type::JSON | &Type::JSONB => "json",
            &Type::OID => "int64",
            &Type::BYTEA => "bytes",
            &Type::BOOL_ARRAY => "array<bool>",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "array<string>",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "array<int64>",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY => "array<float64>",
            &Type::NUMERIC_ARRAY => "array<float64>",
            &Type::DATE_ARRAY => "array<date>",
            &Type::TIME_ARRAY => "array<time>",
            &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "array<timestamp>",
            &Type::UUID_ARRAY => "array<string>",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "array<json>",
            &Type::OID_ARRAY => "array<int64>",
            &Type::BYTEA_ARRAY => "array<bytes>",
            _ => "string",
        }
    }

    // fn is_array_type(typ: &Type) -> bool {
    //     matches!(
    //         typ,
    //         &Type::BOOL_ARRAY
    //             | &Type::CHAR_ARRAY
    //             | &Type::BPCHAR_ARRAY
    //             | &Type::VARCHAR_ARRAY
    //             | &Type::NAME_ARRAY
    //             | &Type::TEXT_ARRAY
    //             | &Type::INT2_ARRAY
    //             | &Type::INT4_ARRAY
    //             | &Type::INT8_ARRAY
    //             | &Type::FLOAT4_ARRAY
    //             | &Type::FLOAT8_ARRAY
    //             | &Type::NUMERIC_ARRAY
    //             | &Type::DATE_ARRAY
    //             | &Type::TIME_ARRAY
    //             | &Type::TIMESTAMP_ARRAY
    //             | &Type::TIMESTAMPTZ_ARRAY
    //             | &Type::UUID_ARRAY
    //             | &Type::JSON_ARRAY
    //             | &Type::JSONB_ARRAY
    //             | &Type::OID_ARRAY
    //             | &Type::BYTEA_ARRAY
    //     )
    // }

    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push('`');
        s.push_str(&column_schema.name);
        s.push('`');
        s.push(' ');
        let typ = Self::postgres_to_bigquery_type(&column_schema.typ);
        s.push_str(typ);
        // if !column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
        //     s.push_str(" not null");
        // };
        if !column_schema.nullable {
            s.push_str(" not null");
        };
    }

    fn add_primary_key_clause(column_schemas: &[ColumnSchema], s: &mut String) {
        let identity_columns = column_schemas.iter().filter(|s| s.primary);

        s.push_str("primary key (");

        for column in identity_columns {
            s.push('`');
            s.push_str(&column.name);
            s.push('`');
            s.push(',');
        }

        s.pop(); //','
        s.push_str(") not enforced");
    }

    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut s = String::new();
        s.push('(');

        for column_schema in column_schemas.iter() {
            Self::column_spec(column_schema, &mut s);
            s.push(',');
        }

        let has_identity_cols = column_schemas.iter().any(|s| s.primary);
        if has_identity_cols {
            Self::add_primary_key_clause(column_schemas, &mut s);
        } else {
            s.pop(); //','
        }

        s.push(')');

        s
    }

    fn max_staleness_option(max_staleness_mins: u16) -> String {
        if max_staleness_mins == 0 {
            "".to_string()
        } else {
            format!("options (max_staleness = interval {max_staleness_mins} minute)")
        }
    }

    fn partition_option(column_schemas: &[ColumnSchema]) -> String {
        if column_schemas.iter().any(|col| col.name == "created_at") {
            "partition by date(created_at)".to_string()
        } else {
            "".to_string()
        }
    }

    pub async fn create_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        max_staleness_mins: u16,
    ) -> Result<(), BQError> {
        let columns_spec = Self::create_columns_spec(column_schemas);
        let max_staleness_option = Self::max_staleness_option(max_staleness_mins);
        let partition_option = Self::partition_option(column_schemas);
        let project_id = &self.project_id;
        info!("creating table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query =
            format!("create table `{project_id}.{dataset_id}.{table_name}` {columns_spec} {partition_option} {max_staleness_option}",);

        let _ = self.query(query).await?;
        Ok(())
    }

    pub async fn get_default_stream(
        &mut self,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<WriteStream, BQError> {
        let stream_name = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );
        let write_stream = self
            .client
            .storage_mut()
            .get_write_stream(&stream_name, WriteStreamView::Full)
            .await?;
        Ok(write_stream)
    }

    pub async fn table_exists(&self, dataset_id: &str, table_name: &str) -> Result<bool, BQError> {
        match self
            .client
            .table()
            .get(&self.project_id, dataset_id, table_name, None)
            .await
        {
            Ok(_) => Ok(true),
            Err(e) if e.to_string().contains("Not found") || e.to_string().contains("404") => {
                Ok(false)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_table_info(
        &self,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<Option<Table>, BQError> {
        match self
            .client
            .table()
            .get(&self.project_id, dataset_id, table_name, None)
            .await
        {
            Ok(table) => Ok(Some(table)),
            Err(e) if e.to_string().contains("Not found") || e.to_string().contains("404") => {
                Ok(None)
            }
            Err(e) => Err(e),
        }
    }

    pub async fn get_last_lsn(&self, dataset_id: &str) -> Result<PgLsn, BQError> {
        let project_id = &self.project_id;
        let query = format!("select lsn from `{project_id}.{dataset_id}._last_lsn`",);

        let mut rs = self.query(query).await?;

        let lsn: i64 = if rs.next_row() {
            rs.get_i64_by_name("lsn")?
                .expect("no column named `lsn` found in query result")
        } else {
            //TODO: return error instead of panicking
            panic!("failed to get lsn");
        };

        Ok((lsn as u64).into())
    }

    pub async fn set_last_lsn(&self, dataset_id: &str, lsn: PgLsn) -> Result<(), BQError> {
        let lsn: u64 = lsn.into();

        let project_id = &self.project_id;
        let query =
            format!("update `{project_id}.{dataset_id}._last_lsn` set lsn = {lsn} where id = 1",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self, dataset_id: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query =
            format!("insert into `{project_id}.{dataset_id}._last_lsn` (id, lsn) values (1, 0)",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn get_copied_table_ids(
        &self,
        dataset_id: &str,
    ) -> Result<HashSet<TableId>, BQError> {
        let project_id = &self.project_id;
        let query = format!("select table_id from `{project_id}.{dataset_id}._copied_tables`",);

        let mut rs = self.query(query).await?;
        let mut table_ids = HashSet::new();
        while rs.next_row() {
            let table_id = rs
                .get_i64_by_name("table_id")?
                .expect("no column named `table_id` found in query result");
            table_ids.insert(table_id as TableId);
        }

        Ok(table_ids)
    }

    pub async fn insert_into_copied_tables(
        &self,
        dataset_id: &str,
        table_id: TableId,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let query = format!(
            "insert into `{project_id}.{dataset_id}._copied_tables` (table_id) values ({table_id})",
        );

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn insert_row(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let query = self.create_insert_row_query(dataset_id, table_name, table_row);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn stream_rows(
        &mut self,
        dataset_id: &str,
        table_name: &str,
        table_descriptor: &TableDescriptor,
        mut table_rows: &[TableRow],
    ) -> Result<(), BQError> {
        let default_stream = StreamName::new_default(
            self.project_id.clone(),
            dataset_id.to_string(),
            table_name.to_string(),
        );

        const MAX_SIZE_BYTES: usize = 9 * 1024 * 1024; // 9 MB

        loop {
            let (rows, num_processed_rows) =
                StorageApi::create_rows(table_descriptor, table_rows, MAX_SIZE_BYTES);
            let trace_id = "etl bigquery client".to_string();
            let mut response_stream = self
                .client
                .storage_mut()
                .append_rows(&default_stream, rows, trace_id)
                .await?;

            if let Some(r) = response_stream.next().await {
                let _ = r?;
            }

            table_rows = &table_rows[num_processed_rows..];
            if table_rows.is_empty() {
                break;
            }
        }

        Ok(())
    }

    pub async fn insert_rows(
        &self,
        dataset_id: &str,
        table_name: &str,
        insert_request: TableDataInsertAllRequest,
    ) -> Result<(), BQError> {
        self.client
            .tabledata()
            .insert_all(&self.project_id, dataset_id, table_name, insert_request)
            .await?;
        Ok(())
    }

    fn create_insert_row_query(
        &self,
        dataset_id: &str,
        table_name: &str,
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        let project_id = &self.project_id;
        s.push_str(&format!(
            "insert into `{project_id}.{dataset_id}.{table_name}`"
        ));
        s.push_str(" values(");

        for (i, value) in table_row.values.iter().enumerate() {
            Self::cell_to_query_value(value, &mut s);

            if i < table_row.values.len() - 1 {
                s.push(',');
            }
        }

        s.push(')');

        s
    }

    fn cell_to_query_value(cell: &Cell, s: &mut String) {
        match cell {
            Cell::Null => s.push_str("null"),
            Cell::Bool(b) => s.push_str(&format!("{b}")),
            Cell::String(str) => s.push_str(&format!("'{str}'")),
            Cell::I16(i) => s.push_str(&format!("{i}")),
            Cell::I32(i) => s.push_str(&format!("{i}")),
            Cell::I64(i) => s.push_str(&format!("{i}")),
            Cell::F32(i) => s.push_str(&format!("{i}")),
            Cell::F64(i) => s.push_str(&format!("{i}")),
            Cell::Numeric(n) => s.push_str(&format!("{n}")),
            Cell::Date(t) => s.push_str(&format!("'{t}'")),
            Cell::Time(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStamp(t) => s.push_str(&format!("'{t}'")),
            Cell::TimeStampTz(t) => s.push_str(&format!("'{t}'")),
            Cell::Uuid(t) => s.push_str(&format!("'{t}'")),
            Cell::Json(j) => s.push_str(&format!("'{j}'")),
            Cell::U32(u) => s.push_str(&format!("{u}")),
            Cell::Bytes(b) => {
                let bytes: String = b.iter().map(|b| *b as char).collect();
                s.push_str(&format!("b'{bytes}'"))
            }
            Cell::Array(_) => unreachable!(),
        }
    }

    pub async fn update_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_update_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;
        Ok(())
    }

    fn create_update_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("update ");
        s.push_str(table_name);
        s.push_str(" set ");

        let mut remove_comma = false;

        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if !column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, &mut s);
                s.push(',');
                remove_comma = true;
            }
        }

        if remove_comma {
            s.pop();
        }

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    /// Adds a where clause for the identity columns
    fn add_identities_where_clause(
        s: &mut String,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) {
        s.push_str(" where ");

        let mut remove_and = false;
        for (cell, column) in table_row.values.iter().zip(column_schemas) {
            if column.primary {
                s.push_str(&column.name);
                s.push_str(" = ");
                Self::cell_to_query_value(cell, s);
                s.push_str(" and ");
                remove_and = true;
            }
        }

        if remove_and {
            s.pop(); //' '
            s.pop(); //'d'
            s.pop(); //'n'
            s.pop(); //'a'
            s.pop(); //' '
        }
    }

    pub async fn delete_row(
        &self,
        dataset_id: &str,
        table_schema: &TableSchema,
        table_row: &TableRow,
    ) -> Result<(), BQError> {
        let project_id = &self.project_id;
        let table_name = &table_schema.name.name;
        let table_name = &format!("`{project_id}.{dataset_id}.{table_name}`");
        let column_schemas = &table_schema.column_schemas;
        let query = Self::create_delete_row_query(table_name, column_schemas, table_row);
        let _ = self.query(query).await?;

        Ok(())
    }

    fn create_delete_row_query(
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_row: &TableRow,
    ) -> String {
        let mut s = String::new();

        s.push_str("delete from ");
        s.push_str(table_name);

        Self::add_identities_where_clause(&mut s, column_schemas, table_row);

        s
    }

    pub async fn drop_table(&self, dataset_id: &str, table_name: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        info!("dropping table {project_id}.{dataset_id}.{table_name} in bigquery");
        let query = format!("drop table `{project_id}.{dataset_id}.{table_name}`",);

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn truncate_table(&self, dataset_id: &str, table_name: &str) -> Result<(), BQError> {
        let project_id = &self.project_id;
        info!("truncating table {project_id}.{dataset_id}.{table_name} in BigQuery");
        let query = format!("truncate table `{project_id}.{dataset_id}.{table_name}`");

        let _ = self.query(query).await?;

        Ok(())
    }

    pub async fn begin_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("begin transaction".to_string()).await?;

        Ok(())
    }

    pub async fn commit_transaction(&self) -> Result<(), BQError> {
        let _ = self.query("commit".to_string()).await?;

        Ok(())
    }

    pub async fn set_last_lsn_and_commit_transaction(
        &self,
        dataset_id: &str,
        last_lsn: PgLsn,
    ) -> Result<(), BQError> {
        self.set_last_lsn(dataset_id, last_lsn).await?;
        self.commit_transaction().await?;
        Ok(())
    }

    async fn query(&self, query: String) -> Result<ResultSet, BQError> {
        info!("Query: {}", query);
        let query_response = self
            .client
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;

        // Check if query completed
        if !query_response.job_complete.unwrap_or(false) {
            return Err(BQError::ResponseError {
                error: gcp_bigquery_client::error::ResponseError {
                    error: NestedResponseError {
                        code: 0,
                        message: "Query did not complete".to_string(),
                        status: "INCOMPLETE".to_string(),
                        errors: vec![],
                    },
                },
            });
        }

        // Log cost information
        if let Some(bytes_str) = &query_response.total_bytes_processed {
            if let Ok(bytes_processed) = bytes_str.parse::<u64>() {
                let cost_usd = (bytes_processed as f64 / 1_000_000_000_000.0) * 6.25;
                info!(
                    "Query processed {:.0} MB, estimated cost: ${:.4}",
                    bytes_processed as f64 / 1_000_000.0,
                    cost_usd
                );
            }
        }

        Ok(ResultSet::new_from_query_response(query_response))
    }

    // Add a new column to an existing table
    pub async fn add_column_to_table(
        &self,
        dataset_id: &str,
        table_name: &str,
        column: &ColumnSchema,
    ) -> Result<(), BQError> {
        let column_type = Self::postgres_to_bigquery_type(&column.typ);
        let nullable = if column.nullable { "" } else { " NOT NULL" };

        let query = format!(
            "ALTER TABLE `{}.{}.{}` ADD COLUMN {} {}{}",
            self.project_id, dataset_id, table_name, column.name, column_type, nullable
        );

        self.query(query).await?;
        Ok(())
    }

    pub async fn get_table_columns(
        &self,
        dataset_id: &str,
        table_name: &str,
    ) -> Result<HashSet<String>, BQError> {
        let table = self
            .client
            .table()
            .get(&self.project_id, dataset_id, table_name, None)
            .await?;

        let mut columns = HashSet::new();

        if let Some(fields) = table.schema.fields {
            for field in fields {
                columns.insert(field.name);
            }
        }

        Ok(columns)
    }

    pub async fn upsert_rows(
        &mut self,
        dataset_id: &str,
        base_table: &str,
        table_descriptor: &TableDescriptor,
        update_rows: &[TableRow],
    ) -> Result<(), BQError> {
        let table_info = self.get_table_info(dataset_id, base_table).await?;
        let table_info = match table_info {
            Some(info) => info,
            None => {
                return Err(BQError::NoDataAvailable);
            }
        };

        let partition_type = self.get_partition_type(&table_info);
        let partition_column = self.get_partition_column(&table_info);

        let num_bytes: u64 = table_info
            .num_bytes
            .as_deref()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0);
        let streaming_buffer = table_info.streaming_buffer.is_some();

        info!("{} size: {} bytes", base_table, num_bytes);
        info!("{} has streaming buffer: {}", base_table, streaming_buffer);
        info!("{} has partition column: {}", base_table, partition_column);

        // 40MB is the minimum bytes billed for any partition replacement.
        // Stream directly into table if it's lower than 40MB
        if partition_type == "NONE"
            || partition_column == ""
            || streaming_buffer
            || num_bytes < 40 * 1024 * 1024
        {
            info!("Streaming rows into {}", base_table);
            self.stream_rows(dataset_id, base_table, table_descriptor, update_rows)
                .await?;
        } else {
            info!(
                "Inserting rows into {} by partition replacement",
                base_table
            );
            self.insert_rows_by_partition_replacement(
                dataset_id,
                base_table,
                table_descriptor,
                update_rows,
                &partition_column,
            )
            .await?;
        }
        Ok(())
    }

    pub async fn insert_rows_by_partition_replacement(
        &mut self,
        dataset_id: &str,
        base_table: &str,
        table_descriptor: &TableDescriptor,
        update_rows: &[TableRow],
        partition_column: &str,
    ) -> Result<(), BQError> {
        let temp_table = format!("{}_temp_{}", base_table, Utc::now().timestamp());

        let affected_partitions = self
            .get_affected_partitions(
                update_rows,
                table_descriptor,
                dataset_id,
                base_table,
                partition_column,
            )
            .await?;

        info!("Affected partitions: {:?}", affected_partitions);

        // 1. Create temp table with final desired state
        self.create_final_state_table(
            dataset_id,
            &temp_table,
            base_table,
            table_descriptor,
            update_rows,
            &affected_partitions,
            partition_column,
        )
        .await?;

        // 2. Replace partitions
        self.execute_partition_replacement(
            dataset_id,
            base_table,
            &temp_table,
            &affected_partitions,
            partition_column,
        )
        .await?;

        // 3. Cleanup
        self.drop_table(dataset_id, &temp_table).await?;

        Ok(())
    }

    async fn create_final_state_table(
        &mut self,
        dataset_id: &str,
        temp_table: &str,
        base_table: &str,
        table_descriptor: &TableDescriptor,
        update_rows: &[TableRow],
        affected_partitions: &[String],
        partition_column: &str,
    ) -> Result<(), BQError> {
        // 1. Create empty temp table with same schema as base table
        self.create_temp_table(dataset_id, temp_table, base_table)
            .await?;

        // 2. Copy existing data from unaffected partitions to temp table
        self.copy_existing_partitions(
            dataset_id,
            temp_table,
            base_table,
            &affected_partitions,
            partition_column,
        )
        .await?;

        // 3. Add the new/updated records via streaming
        self.stream_rows(dataset_id, temp_table, table_descriptor, update_rows)
            .await?;

        Ok(())
    }

    async fn create_temp_table(
        &mut self,
        dataset_id: &str,
        temp_table: &str,
        base_table: &str,
    ) -> Result<(), BQError> {
        let query = format!(
            "create table `{}.{}.{}`
             like `{}.{}.{}`
             options(
               expiration_timestamp=timestamp_add(current_timestamp(), interval 10 minute)
             )",
            self.project_id, dataset_id, temp_table, self.project_id, dataset_id, base_table
        );

        self.query(query).await?;
        Ok(())
    }

    async fn copy_existing_partitions(
        &mut self,
        dataset_id: &str,
        temp_table: &str,
        base_table: &str,
        affected_partitions: &[String],
        partition_column: &str,
    ) -> Result<(), BQError> {
        let partition_filter = affected_partitions
            .iter()
            .map(|p| format!("'{}'", p))
            .collect::<Vec<_>>()
            .join(",");

        let query = format!(
            "insert into `{}.{}.{}` select * from `{}.{}.{}` where date({}) in ({})",
            self.project_id,
            dataset_id,
            temp_table,
            self.project_id,
            dataset_id,
            base_table,
            partition_column,
            partition_filter
        );

        self.query(query).await?;
        Ok(())
    }

    async fn get_affected_partitions(
        &self,
        update_rows: &[TableRow],
        table_descriptor: &TableDescriptor,
        dataset_id: &str,
        table_name: &str,
        partition_column: &str,
    ) -> Result<Vec<String>, BQError> {
        let mut partitions = HashSet::new();
        let mut delete_ids = Vec::new();

        // Find the id field index once
        let id_field_index = table_descriptor
            .field_descriptors
            .iter()
            .position(|field| field.name == "id")
            .ok_or_else(|| BQError::InvalidColumnName {
                col_name: "id".to_string(),
            })?;

        // Process rows
        for row in update_rows {
            if row.is_delete() {
                // Extract id for delete rows
                let id_cell = &row.values[id_field_index];
                let mut id_value = String::new();
                Self::cell_to_query_value(id_cell, &mut id_value);
                delete_ids.push(id_value);
            } else {
                // Get partition date for non-delete rows
                let partition_date = row
                    .get_partition_date(table_descriptor, partition_column)
                    .map_err(|e| BQError::InvalidColumnName {
                        col_name: format!("{}: {}", partition_column, e),
                    })?;
                partitions.insert(partition_date);
            }
        }

        // Query partitions for delete rows if any exist
        if !delete_ids.is_empty() {
            let query = format!(
                "select distinct format_date('%Y-%m-%d', {}) as partition_date from `{}.{}.{}` where id in ({})",
                partition_column, self.project_id, dataset_id, table_name, delete_ids.join(", ")
            );

            let mut result_set = self.query(query).await?;
            while result_set.next_row() {
                let partition_date = result_set
                    .get_string_by_name("partition_date")?
                    .ok_or(BQError::NoDataAvailable)?;
                partitions.insert(partition_date);
            }
        }

        let mut partitions_vec: Vec<String> = partitions.into_iter().collect();
        partitions_vec.sort();
        Ok(partitions_vec)
    }

    async fn execute_partition_replacement(
        &mut self,
        dataset_id: &str,
        base_table: &str,
        temp_table: &str,
        affected_partitions: &[String],
        partition_column: &str,
    ) -> Result<(), BQError> {
        let partition_filter = affected_partitions
            .iter()
            .map(|p| format!("'{}'", p))
            .collect::<Vec<_>>()
            .join(",");

        let merge_query = format!(
            "merge `{}.{}.{}` t using `{}.{}.{}` s on false when not matched by source and date(t.`{}`) in ({}) then delete when not matched then insert row",
            self.project_id,
            dataset_id,
            base_table,
            self.project_id,
            dataset_id,
            temp_table,
            partition_column,
            partition_filter
        );

        self.query(merge_query).await?;
        Ok(())
    }

    fn get_partition_type(&mut self, table: &Table) -> String {
        table
            .time_partitioning
            .as_ref()
            .map(|tp| tp.r#type.clone())
            .unwrap_or("NONE".to_string())
    }

    fn get_partition_column(&mut self, table: &Table) -> String {
        table
            .time_partitioning
            .as_ref()
            .and_then(|tp| tp.field.clone())
            .unwrap_or("".to_string())
    }
}

impl Message for TableRow {
    fn encode_raw(&self, buf: &mut impl BufMut)
    where
        Self: Sized,
    {
        let mut tag = 1;
        for cell in &self.values {
            cell.encode_raw(tag, buf);
            tag += 1;
        }
    }

    fn merge_field(
        &mut self,
        _tag: u32,
        _wire_type: prost::encoding::WireType,
        _buf: &mut impl Buf,
        _ctx: prost::encoding::DecodeContext,
    ) -> Result<(), prost::DecodeError>
    where
        Self: Sized,
    {
        unimplemented!("merge_field not implemented yet");
    }

    fn encoded_len(&self) -> usize {
        let mut len = 0;
        let mut tag = 1;
        for cell in &self.values {
            len += cell.encoded_len(tag);
            tag += 1;
        }
        len
    }

    fn clear(&mut self) {
        for cell in &mut self.values {
            cell.clear();
        }
    }
}

impl Cell {
    fn encode_raw(&self, tag: u32, buf: &mut impl BufMut) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => {
                ::prost::encoding::bool::encode(tag, b, buf);
            }
            Cell::String(s) => {
                ::prost::encoding::string::encode(tag, s, buf);
            }
            Cell::I16(i) => {
                let val = *i as i32;
                ::prost::encoding::int32::encode(tag, &val, buf);
            }
            Cell::I32(i) => {
                ::prost::encoding::int32::encode(tag, i, buf);
            }
            Cell::I64(i) => {
                ::prost::encoding::int64::encode(tag, i, buf);
            }
            Cell::F32(i) => {
                ::prost::encoding::float::encode(tag, i, buf);
            }
            Cell::F64(i) => {
                ::prost::encoding::double::encode(tag, i, buf);
            }
            Cell::Numeric(n) => {
                let f = n.to_f64();
                ::prost::encoding::double::encode(tag, &f, buf);
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                ::prost::encoding::string::encode(tag, &s, buf);
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                ::prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                ::prost::encoding::string::encode(tag, &s, buf)
            }
            Cell::U32(i) => {
                ::prost::encoding::uint32::encode(tag, i, buf);
            }
            Cell::Bytes(b) => {
                ::prost::encoding::bytes::encode(tag, b, buf);
            }
            Cell::Array(a) => {
                a.clone().encode_raw(tag, buf);
            }
        }
    }

    fn encoded_len(&self, tag: u32) -> usize {
        match self {
            Cell::Null => 0,
            Cell::Bool(b) => ::prost::encoding::bool::encoded_len(tag, b),
            Cell::String(s) => ::prost::encoding::string::encoded_len(tag, s),
            Cell::I16(i) => {
                let val = *i as i32;
                ::prost::encoding::int32::encoded_len(tag, &val)
            }
            Cell::I32(i) => ::prost::encoding::int32::encoded_len(tag, i),
            Cell::I64(i) => ::prost::encoding::int64::encoded_len(tag, i),
            Cell::F32(i) => ::prost::encoding::float::encoded_len(tag, i),
            Cell::F64(i) => ::prost::encoding::double::encoded_len(tag, i),
            Cell::Numeric(n) => {
                let f = n.to_f64();
                ::prost::encoding::double::encoded_len(tag, &f)
            }
            Cell::Date(t) => {
                let s = t.format("%Y-%m-%d").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Time(t) => {
                let s = t.format("%H:%M:%S%.f").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStamp(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::TimeStampTz(t) => {
                let s = t.format("%Y-%m-%d %H:%M:%S%.f%:z").to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Uuid(u) => {
                let s = u.to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::Json(j) => {
                let s = j.to_string();
                ::prost::encoding::string::encoded_len(tag, &s)
            }
            Cell::U32(i) => ::prost::encoding::uint32::encoded_len(tag, i),
            Cell::Bytes(b) => ::prost::encoding::bytes::encoded_len(tag, b),
            Cell::Array(array_cell) => array_cell.clone().encoded_len(tag),
        }
    }

    fn clear(&mut self) {
        match self {
            Cell::Null => {}
            Cell::Bool(b) => *b = false,
            Cell::String(s) => s.clear(),
            Cell::I16(i) => *i = 0,
            Cell::I32(i) => *i = 0,
            Cell::I64(i) => *i = 0,
            Cell::F32(i) => *i = 0.,
            Cell::F64(i) => *i = 0.,
            Cell::Numeric(n) => *n = PgNumeric::default(),
            Cell::Date(t) => *t = NaiveDate::default(),
            Cell::Time(t) => *t = NaiveTime::default(),
            Cell::TimeStamp(t) => *t = NaiveDateTime::default(),
            Cell::TimeStampTz(t) => *t = DateTime::<Utc>::default(),
            Cell::Uuid(u) => *u = Uuid::default(),
            Cell::Json(j) => *j = serde_json::Value::default(),
            Cell::U32(u) => *u = 0,
            Cell::Bytes(b) => b.clear(),
            Cell::Array(vec) => {
                vec.clear();
            }
        }
    }
}

impl ArrayCell {
    fn encode_raw(self, tag: u32, buf: &mut impl BufMut) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                ::prost::encoding::bool::encode_packed(tag, &vec, buf);
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                ::prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                ::prost::encoding::int32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                ::prost::encoding::uint32::encode_packed(tag, &vec, buf);
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                ::prost::encoding::int64::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                ::prost::encoding::float::encode_packed(tag, &vec, buf);
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                ::prost::encoding::double::encode_packed(tag, &vec, buf);
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<f64> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_f64())
                    .collect();
                ::prost::encoding::double::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encode_repeated(tag, &vec, buf);
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                ::prost::encoding::bytes::encode_repeated(tag, &vec, buf);
            }
        }
    }

    fn encoded_len(self, tag: u32) -> usize {
        match self {
            ArrayCell::Null => 0,
            ArrayCell::Bool(mut vec) => {
                let vec: Vec<bool> = vec.drain(..).flatten().collect();
                ::prost::encoding::bool::encoded_len_packed(tag, &vec)
            }
            ArrayCell::String(mut vec) => {
                let vec: Vec<String> = vec.drain(..).flatten().collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::I16(mut vec) => {
                let vec: Vec<i32> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap() as i32)
                    .collect();
                ::prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I32(mut vec) => {
                let vec: Vec<i32> = vec.drain(..).flatten().collect();
                ::prost::encoding::int32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::U32(mut vec) => {
                let vec: Vec<u32> = vec.drain(..).flatten().collect();
                ::prost::encoding::uint32::encoded_len_packed(tag, &vec)
            }
            ArrayCell::I64(mut vec) => {
                let vec: Vec<i64> = vec.drain(..).flatten().collect();
                ::prost::encoding::int64::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F32(mut vec) => {
                let vec: Vec<f32> = vec.drain(..).flatten().collect();
                ::prost::encoding::float::encoded_len_packed(tag, &vec)
            }
            ArrayCell::F64(mut vec) => {
                let vec: Vec<f64> = vec.drain(..).flatten().collect();
                ::prost::encoding::double::encoded_len_packed(tag, &vec)
            }
            ArrayCell::Numeric(mut vec) => {
                let vec: Vec<f64> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_f64())
                    .collect();
                ::prost::encoding::double::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Date(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Time(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStamp(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::TimeStampTz(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().format("%Y-%m-%d %H:%M:%S%.f%:z").to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Uuid(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Json(mut vec) => {
                let vec: Vec<String> = vec
                    .drain(..)
                    .filter(|v| v.is_some())
                    .map(|v| v.unwrap().to_string())
                    .collect();
                ::prost::encoding::string::encoded_len_repeated(tag, &vec)
            }
            ArrayCell::Bytes(mut vec) => {
                let vec: Vec<Vec<u8>> = vec.drain(..).flatten().collect();
                ::prost::encoding::bytes::encoded_len_repeated(tag, &vec)
            }
        }
    }

    fn clear(&mut self) {
        match self {
            ArrayCell::Null => {}
            ArrayCell::Bool(vec) => vec.clear(),
            ArrayCell::String(vec) => vec.clear(),
            ArrayCell::I16(vec) => vec.clear(),
            ArrayCell::I32(vec) => vec.clear(),
            ArrayCell::U32(vec) => vec.clear(),
            ArrayCell::I64(vec) => vec.clear(),
            ArrayCell::F32(vec) => vec.clear(),
            ArrayCell::F64(vec) => vec.clear(),
            ArrayCell::Numeric(vec) => vec.clear(),
            ArrayCell::Date(vec) => vec.clear(),
            ArrayCell::Time(vec) => vec.clear(),
            ArrayCell::TimeStamp(vec) => vec.clear(),
            ArrayCell::TimeStampTz(vec) => vec.clear(),
            ArrayCell::Uuid(vec) => vec.clear(),
            ArrayCell::Json(vec) => vec.clear(),
            ArrayCell::Bytes(vec) => vec.clear(),
        }
    }
}

/// Converts a [`TableSchema`] to [`TableDescriptor`].
///
/// This function is defined here and doesn't use the [`From`] trait because it's not possible since
/// [`TableSchema`] is in another crate and we don't want to pollute the `postgres` crate with destination
/// specific internals.
pub fn table_schema_to_descriptor(table_schema: &TableSchema) -> TableDescriptor {
    let mut field_descriptors = Vec::with_capacity(table_schema.column_schemas.len());
    let mut number = 1;
    for column_schema in &table_schema.column_schemas {
        let typ = match column_schema.typ {
            Type::BOOL => ColumnType::Bool,
            Type::CHAR | Type::BPCHAR | Type::VARCHAR | Type::NAME | Type::TEXT => {
                ColumnType::String
            }
            Type::INT2 => ColumnType::Int32,
            Type::INT4 => ColumnType::Int32,
            Type::INT8 => ColumnType::Int64,
            Type::FLOAT4 => ColumnType::Float,
            Type::FLOAT8 => ColumnType::Double,
            Type::NUMERIC => ColumnType::Double,
            Type::DATE => ColumnType::String,
            Type::TIME => ColumnType::String,
            Type::TIMESTAMP => ColumnType::String,
            Type::TIMESTAMPTZ => ColumnType::String,
            Type::UUID => ColumnType::String,
            Type::JSON => ColumnType::String,
            Type::JSONB => ColumnType::String,
            Type::OID => ColumnType::Int32,
            Type::BYTEA => ColumnType::Bytes,
            Type::BOOL_ARRAY => ColumnType::Bool,
            Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY => ColumnType::String,
            Type::INT2_ARRAY => ColumnType::Int32,
            Type::INT4_ARRAY => ColumnType::Int32,
            Type::INT8_ARRAY => ColumnType::Int64,
            Type::FLOAT4_ARRAY => ColumnType::Float,
            Type::FLOAT8_ARRAY => ColumnType::Double,
            Type::NUMERIC_ARRAY => ColumnType::Double,
            Type::DATE_ARRAY => ColumnType::String,
            Type::TIME_ARRAY => ColumnType::String,
            Type::TIMESTAMP_ARRAY => ColumnType::String,
            Type::TIMESTAMPTZ_ARRAY => ColumnType::String,
            Type::UUID_ARRAY => ColumnType::String,
            Type::JSON_ARRAY => ColumnType::String,
            Type::JSONB_ARRAY => ColumnType::String,
            Type::OID_ARRAY => ColumnType::Int32,
            Type::BYTEA_ARRAY => ColumnType::Bytes,
            _ => ColumnType::String,
        };

        let mode = match column_schema.typ {
            Type::BOOL_ARRAY
            | Type::CHAR_ARRAY
            | Type::BPCHAR_ARRAY
            | Type::VARCHAR_ARRAY
            | Type::NAME_ARRAY
            | Type::TEXT_ARRAY
            | Type::INT2_ARRAY
            | Type::INT4_ARRAY
            | Type::INT8_ARRAY
            | Type::FLOAT4_ARRAY
            | Type::FLOAT8_ARRAY
            | Type::NUMERIC_ARRAY
            | Type::DATE_ARRAY
            | Type::TIME_ARRAY
            | Type::TIMESTAMP_ARRAY
            | Type::TIMESTAMPTZ_ARRAY
            | Type::UUID_ARRAY
            | Type::JSON_ARRAY
            | Type::JSONB_ARRAY
            | Type::OID_ARRAY
            | Type::BYTEA_ARRAY => ColumnMode::Nullable,
            _ => ColumnMode::Nullable,
        };

        field_descriptors.push(FieldDescriptor {
            number,
            name: column_schema.name.clone(),
            typ,
            mode,
        });
        number += 1;
    }

    field_descriptors.push(FieldDescriptor {
        number,
        name: "_CHANGE_TYPE".to_string(),
        typ: ColumnType::String,
        mode: ColumnMode::Required,
    });

    field_descriptors.push(FieldDescriptor {
        number: number + 1,
        name: "_CHANGE_SEQUENCE_NUMBER".to_string(),
        typ: ColumnType::String,
        mode: ColumnMode::Required,
    });

    TableDescriptor { field_descriptors }
}

impl TableRow {
    pub fn get_partition_date(
        &self,
        table_descriptor: &TableDescriptor,
        partition_column: &str,
    ) -> Result<String, Box<dyn std::error::Error>> {
        // Find the "created_at" column position in the table descriptor
        let created_at_index = table_descriptor
            .field_descriptors
            .iter()
            .position(|field| field.name == partition_column)
            .ok_or(format!(
                "Partition column '{}' not found in table descriptor",
                partition_column,
            ))?;

        match self.values.get(created_at_index) {
            Some(Cell::Date(date)) => Ok(date.format("%Y-%m-%d").to_string()),
            Some(Cell::TimeStamp(datetime)) => Ok(datetime.date().format("%Y-%m-%d").to_string()),
            Some(Cell::TimeStampTz(datetime)) => {
                Ok(datetime.date_naive().format("%Y-%m-%d").to_string())
            }
            Some(Cell::String(date_str)) => {
                // Try to parse string as date
                if let Ok(parsed_date) = NaiveDate::parse_from_str(date_str, "%Y-%m-%d") {
                    Ok(parsed_date.format("%Y-%m-%d").to_string())
                } else if let Ok(parsed_datetime) =
                    NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S")
                {
                    Ok(parsed_datetime.date().format("%Y-%m-%d").to_string())
                } else if let Ok(parsed_datetime) =
                    NaiveDateTime::parse_from_str(date_str, "%Y-%m-%dT%H:%M:%S")
                {
                    Ok(parsed_datetime.date().format("%Y-%m-%d").to_string())
                } else {
                    Err(format!("Could not parse date from string: {}", date_str).into())
                }
            }
            Some(Cell::Null) => Err("Partition date cannot be null".into()),
            Some(other) => Err(format!("Unexpected date type: {:?}", other).into()),
            None => Err("Created_at column index out of bounds".into()),
        }
    }

    pub fn is_delete(&self) -> bool {
        self.values[self.values.len() - 2] == Cell::String("DELETE".to_string())
    }
}

impl PgNumeric {
    pub fn to_f64(&self) -> f64 {
        match self {
            PgNumeric::NaN => f64::NAN,
            PgNumeric::PositiveInf => f64::INFINITY,
            PgNumeric::NegativeInf => f64::NEG_INFINITY,
            PgNumeric::Value(bd) => bd.to_string().parse::<f64>().unwrap_or(f64::NAN),
        }
    }
}
