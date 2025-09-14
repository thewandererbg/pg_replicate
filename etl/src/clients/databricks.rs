use crate::clients::s3::{S3Client, S3Error};
use crate::utils::deduplication::TableRowDeduplicator;
use chrono::NaiveDateTime;
use postgres::schema::{ColumnSchema, TableId, TableSchema};
use regex::Regex;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::from_str;
use std::collections::HashSet;
use std::str::FromStr;
use thiserror::Error;
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;

#[derive(Debug, Error)]
pub enum DatabricksError {
    #[error("Request failed: {0}")]
    RequestFailed(String),

    #[error("Authentication failed: {0}")]
    AuthenticationFailed(String),

    #[error("Statement execution failed: {0}")]
    StatementExecutionFailed(String),

    #[error("Table not found: {0}")]
    TableNotFound(String),

    #[error("Schema evolution failed: {0}")]
    SchemaEvolutionFailed(String),

    #[error("Table creation failed: {0}")]
    TableCreationFailed(String),

    #[error("Missing primary key columns: {0}")]
    MissingPrimaryKeyColumns(String),

    #[error("Invalid column name: {col_name}")]
    InvalidColumnName { col_name: String },

    #[error("No data available")]
    NoDataAvailable,

    #[error("Invalid cell type: expected {expected}, got {got}")]
    InvalidCellType { expected: String, got: String },

    #[error("HTTP error: {0}")]
    HttpError(#[from] reqwest::Error),

    #[error("JSON error: {0}")]
    JsonError(#[from] serde_json::Error),

    #[error("S3 error: {0}")]
    S3Error(#[from] S3Error),

    #[error("S3 not configured")]
    S3NotConfigured,
}

/// Databricks client for CDC operations using HTTP API
#[derive(Clone)]
pub struct DatabricksClient {
    client: Client,
    workspace_url: String,
    warehouse_id: String,
    access_token: String,
    catalog: String,
    schema: String,
    pub s3_client: Option<S3Client>,
}

#[derive(Debug, Serialize)]
struct ExecuteStatementRequest {
    statement: String,
    warehouse_id: String,
    catalog: String,
    schema: String,
    wait_timeout: Option<String>,
    on_wait_timeout: Option<String>,
    format: Option<String>,
    disposition: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ExecuteStatementResponse {
    #[allow(dead_code)]
    statement_id: String,
    status: StatementStatus,
    result: Option<StatementResult>,
}

#[derive(Debug, Deserialize)]
struct StatementStatus {
    state: String,
    error: Option<ErrorInfo>,
}

#[derive(Debug, Deserialize)]
struct ErrorInfo {
    message: String,
    // #[serde(rename = "type")]
    // error_type: Option<String>,
    // details: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct StatementResult {
    #[allow(dead_code)]
    row_count: Option<i64>,
    data_array: Option<Vec<Vec<serde_json::Value>>>,
}

impl DatabricksClient {
    /// Creates a new Databricks client with HTTP API access
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
    ) -> Result<DatabricksClient, DatabricksError> {
        let client = Client::new();

        let s3_client = match (s3_bucket, s3_region, s3_access_key, s3_secret_key) {
            (Some(bucket), Some(region), Some(access_key), Some(secret_key)) => {
                let prefix = "cdc-data".to_string(); // or make this configurable too
                S3Client::new(bucket, region, prefix, access_key, secret_key)
                    .await
                    .ok()
            }
            _ => None,
        };

        let databricks_client = DatabricksClient {
            client,
            workspace_url,
            warehouse_id,
            access_token,
            catalog,
            schema,
            s3_client,
        };

        // Test connection with simple query
        databricks_client.execute_query("SELECT 1").await?;

        Ok(databricks_client)
    }

    /// Executes SQL and returns full response (core implementation)
    async fn execute_sql(&self, sql: &str) -> Result<ExecuteStatementResponse, DatabricksError> {
        let re = Regex::new(r"VALUES\s*\([^)]*\)(?:\s*,\s*\([^)]*\))*").unwrap();
        let clean_sql = re.replace(&sql, "VALUES (...)");
        info!("Executing SQL: {}", clean_sql);

        let request = ExecuteStatementRequest {
            statement: sql.to_string(),
            warehouse_id: self.warehouse_id.clone(),
            catalog: self.catalog.clone(),
            schema: self.schema.clone(),
            wait_timeout: Some("50s".to_string()),
            on_wait_timeout: Some("CANCEL".to_string()),
            format: Some("JSON_ARRAY".to_string()),
            disposition: Some("INLINE".to_string()),
        };

        let url = format!("{}/api/2.0/sql/statements", self.workspace_url);
        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", self.access_token))
            .json(&request)
            .send()
            .await?;

        if !response.status().is_success() {
            let error_text = response.text().await?;
            return Err(DatabricksError::StatementExecutionFailed(error_text));
        }

        let result: ExecuteStatementResponse = response.json().await?;

        if result.status.state != "SUCCEEDED" {
            let error_detail = result
                .status
                .error
                .as_ref()
                .map(|e| format!("Error: {}", e.message))
                .unwrap_or("No error details".to_string());
            return Err(DatabricksError::StatementExecutionFailed(format!(
                "Statement failed with status: {}, Details: {}",
                result.status.state, error_detail
            )));
        }

        Ok(result)
    }

    /// Executes a SQL statement and waits for completion (DDL/DML operations)
    pub async fn execute_statement(&self, sql: &str) -> Result<(), DatabricksError> {
        self.execute_sql(sql).await.map(|_| ())
    }

    /// Executes a query and returns results (SELECT operations)
    pub async fn execute_query(&self, sql: &str) -> Result<StatementResult, DatabricksError> {
        let response = self.execute_sql(sql).await?;
        response.result.ok_or(DatabricksError::NoDataAvailable)
    }

    /// Inserts rows into a table
    pub async fn insert_rows(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<(), DatabricksError> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);
        let insert_sql = self.build_insert_sql(&full_table_name, column_schemas, table_rows)?;

        info!(
            "Inserting {} rows into {}",
            table_rows.len(),
            full_table_name
        );
        self.execute_statement(&insert_sql).await?;

        Ok(())
    }

    /// Builds INSERT statement for any table
    /// TODO: This method is vulnerable to SQL injection through row data
    /// Consider using parameterized queries when Databricks API supports it
    fn build_insert_sql(
        &self,
        full_table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<String, DatabricksError> {
        if table_rows.is_empty() {
            return Err(DatabricksError::NoDataAvailable);
        }

        // Build column names list
        let column_names: Vec<String> = column_schemas
            .iter()
            .map(|col| Self::escape_identifier(&col.name))
            .collect();

        let mut values_clauses = Vec::new();
        for row in table_rows {
            let mut values = Vec::new();

            for cell in &row.values {
                values.push(Self::cell_to_sql_value(cell)?);
            }

            values_clauses.push(format!("({})", values.join(", ")));
        }

        let insert_sql = format!(
            "INSERT INTO {} ({}) VALUES {}",
            full_table_name,
            column_names.join(", "),
            values_clauses.join(", ")
        );

        Ok(insert_sql)
    }

    /// Create or update table with the specified schema
    pub async fn create_or_update_table(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, DatabricksError> {
        if self.table_exists(table_name).await? {
            // If table exists, check for new columns
            let existing_columns = self.get_table_columns(&table_name).await?;

            // Find columns that exist in the new schema but not in the table
            let new_columns: Vec<&ColumnSchema> = column_schemas
                .iter()
                .filter(|col| !existing_columns.contains(&col.name))
                .collect();

            // If there are new columns, add them to the table
            if !new_columns.is_empty() {
                for column in &new_columns {
                    self.add_column_to_table(table_name, column).await?;
                }
            }
            Ok(false)
        } else {
            self.create_table_if_missing(table_name, column_schemas)
                .await?;
            Ok(true)
        }
    }

    /// Creates a table with the specified schema
    pub async fn create_table(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<(), DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);

        // Validate primary key columns exist for CDC operations
        let primary_key_columns: Vec<&ColumnSchema> =
            column_schemas.iter().filter(|col| col.primary).collect();

        if primary_key_columns.is_empty() {
            return Err(DatabricksError::SchemaEvolutionFailed(
                "Table requires at least one primary key column for CDC operations".to_string(),
            ));
        }

        // Filter out _operation column and create columns spec
        let filtered_columns: Vec<ColumnSchema> = column_schemas
            .iter()
            .filter(|col| col.name != "_operation")
            .cloned()
            .collect();

        let columns_spec = Self::create_columns_spec(&filtered_columns);

        // Find optimal partitioning column (prioritize created_at/date columns)
        let created_column = column_schemas
            .iter()
            .find(|col| {
                matches!(col.typ, Type::DATE | Type::TIMESTAMP | Type::TIMESTAMPTZ)
                    && col.name.to_lowercase().contains("created_at")
            })
            .map(|col| Self::escape_identifier(&col.name));

        let id_column = column_schemas
            .iter()
            .find(|col| col.name == "id")
            .map(|col| Self::escape_identifier(&col.name));

        let cluster_clause = match (created_column, id_column) {
            (Some(created), Some(id)) => format!("CLUSTER BY ({}, {})", created, id),
            (Some(created), None) => format!("CLUSTER BY ({})", created),
            (None, Some(id)) => format!("CLUSTER BY ({})", id),
            (None, None) => String::new(),
        };

        // Create table
        let create_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} {} \
             USING DELTA \
             {} \
             TBLPROPERTIES (\
               'delta.autoOptimize.optimizeWrite' = 'true', \
               'delta.autoOptimize.autoCompact' = 'true'\
             )",
            full_table_name, columns_spec, cluster_clause
        );

        self.execute_statement(&create_sql).await?;

        // Log creation summary
        let pk_columns: Vec<String> = primary_key_columns
            .iter()
            .map(|col| Self::escape_identifier(&col.name))
            .collect();

        info!(
            "Table {} created for CDC operations. \
             Clean data with _version tracking. Primary keys: {}. {}",
            full_table_name,
            pk_columns.join(", "),
            cluster_clause
        );

        Ok(())
    }

    /// Creates table if missing
    pub async fn create_table_if_missing(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
    ) -> Result<bool, DatabricksError> {
        let table_exists = self.table_exists(table_name).await?;

        if table_exists {
            return Ok(false); // Table already exists
        }

        info!("Creating table: {}", table_name);

        self.create_table(table_name, column_schemas).await?;

        Ok(true)
    }

    /// Checks if a table exists in the specified catalog and schema
    pub async fn table_exists(&self, table_name: &str) -> Result<bool, DatabricksError> {
        let sql = format!(
            "SELECT 1 FROM information_schema.tables WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}' LIMIT 1",
            self.catalog, self.schema, table_name
        );

        let result = self.execute_query(&sql).await?;

        let exists = result
            .data_array
            .map(|rows| !rows.is_empty())
            .unwrap_or(false);

        Ok(exists)
    }

    /// Drops a table if it exists
    pub async fn drop_table(&self, table_name: &str) -> Result<(), DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);
        let sql = format!("DROP TABLE IF EXISTS {}", full_table_name);
        self.execute_statement(&sql).await
    }

    /// Truncates a table
    pub async fn truncate_table(&self, table_name: &str) -> Result<(), DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);
        let sql = format!("TRUNCATE TABLE {}", full_table_name);
        self.execute_statement(&sql).await
    }

    /// Gets the last processed LSN from CDC tracking table
    pub async fn get_last_lsn(&self) -> Result<PgLsn, DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, "_last_lsn");
        let sql = format!("SELECT lsn FROM {} WHERE id = 1", full_table_name);

        let result = self.execute_query(&sql).await?;

        if let Some(rows) = result.data_array {
            if let Some(first_row) = rows.first() {
                if let Some(lsn_value) = first_row.first() {
                    if let Some(lsn_str) = lsn_value.as_str() {
                        return lsn_str.parse().map_err(|_| {
                            DatabricksError::StatementExecutionFailed(
                                "Invalid LSN format".to_string(),
                            )
                        });
                    }
                }
            }
        }

        Err(DatabricksError::NoDataAvailable)
    }

    /// Updates the last processed LSN using MERGE operation
    pub async fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, "_last_lsn");
        let sql = format!(
            "MERGE INTO {} AS target \
             USING (SELECT 1 as id, '{}' as lsn) AS source \
             ON target.id = source.id \
             WHEN MATCHED THEN UPDATE SET lsn = source.lsn \
             WHEN NOT MATCHED THEN INSERT (id, lsn) VALUES (source.id, source.lsn)",
            full_table_name, lsn
        );

        self.execute_statement(&sql).await
    }

    /// Gets table IDs that completed initial copy
    pub async fn get_copied_table_ids(&self) -> Result<HashSet<TableId>, DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, "_copied_tables");
        let sql = format!("SELECT table_id FROM {}", full_table_name);

        let result = self.execute_query(&sql).await?;

        let table_ids: Vec<u32> = Self::result_column_as(&result, 0);

        Ok(table_ids.into_iter().collect())
    }

    /// Marks table as copied in tracking table
    pub async fn insert_into_copied_tables(
        &self,
        table_id: TableId,
    ) -> Result<(), DatabricksError> {
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, "_copied_tables");
        let sql = format!(
            "MERGE INTO {} AS target \
             USING (SELECT {} as table_id) AS source \
             ON target.table_id = source.table_id \
             WHEN NOT MATCHED THEN INSERT (table_id) VALUES (source.table_id)",
            full_table_name, table_id
        );

        self.execute_statement(&sql).await
    }

    /// Initializes CDC tracking tables (_last_lsn and _copied_tables)
    pub async fn create_cdc_tracking_tables(&self) -> Result<(), DatabricksError> {
        info!("Creating CDC tracking tables");
        // Create _last_lsn table
        let last_lsn_table = Self::build_table_name(&self.catalog, &self.schema, "_last_lsn");
        let last_lsn_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT, lsn STRING) USING DELTA",
            last_lsn_table
        );
        self.execute_statement(&last_lsn_sql).await?;

        // Create _copied_tables table
        let copied_tables_table =
            Self::build_table_name(&self.catalog, &self.schema, "_copied_tables");
        let copied_tables_sql = format!(
            "CREATE TABLE IF NOT EXISTS {} (table_id BIGINT) USING DELTA",
            copied_tables_table
        );
        self.execute_statement(&copied_tables_sql).await?;

        Ok(())
    }

    /// Maps PostgreSQL types to Databricks types
    fn postgres_to_databricks_type(typ: &Type) -> &'static str {
        match typ {
            // Scalars
            &Type::BOOL => "BOOLEAN",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "STRING",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "BIGINT",
            &Type::FLOAT4 | &Type::FLOAT8 => "DOUBLE",
            &Type::NUMERIC => "DOUBLE", // use DOUBLE for simplicity
            &Type::DATE => "DATE",
            &Type::TIME => "STRING",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "TIMESTAMP",
            &Type::UUID => "STRING",
            &Type::JSON | &Type::JSONB => "STRING",
            &Type::OID => "BIGINT",
            &Type::BYTEA => "BINARY",

            // Arrays
            &Type::BOOL_ARRAY => "ARRAY<BOOLEAN>",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "ARRAY<STRING>",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "ARRAY<BIGINT>",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY | &Type::NUMERIC_ARRAY => "ARRAY<DOUBLE>",
            &Type::DATE_ARRAY => "ARRAY<DATE>",
            &Type::TIME_ARRAY => "ARRAY<STRING>",
            &Type::TIMESTAMP_ARRAY | &Type::TIMESTAMPTZ_ARRAY => "ARRAY<TIMESTAMP>",
            &Type::UUID_ARRAY => "ARRAY<STRING>",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "ARRAY<STRING>",
            &Type::OID_ARRAY => "ARRAY<BIGINT>",
            &Type::BYTEA_ARRAY => "ARRAY<BINARY>",

            _ => "STRING",
        }
    }

    /// Generates CREATE TABLE column specifications
    fn create_columns_spec(column_schemas: &[ColumnSchema]) -> String {
        let mut specs = Vec::new();

        for column in column_schemas {
            let mut spec = format!(
                "{} {}",
                Self::escape_identifier(&column.name),
                Self::postgres_to_databricks_type(&column.typ)
            );

            if !column.nullable {
                spec.push_str(" NOT NULL");
            }

            specs.push(spec);
        }

        format!("({})", specs.join(", "))
    }

    /// Converts Cell to SQL value string
    /// TODO: This method is vulnerable to SQL injection through string values
    /// Consider using parameterized queries when Databricks API supports it
    fn cell_to_sql_value(cell: &Cell) -> Result<String, DatabricksError> {
        let value = match cell {
            Cell::Null => "NULL".to_string(),
            Cell::Bool(b) => b.to_string(),
            Cell::String(s) => format!("'{}'", s.replace("'", "''")),
            Cell::I16(n) => n.to_string(),
            Cell::I32(n) => n.to_string(),
            Cell::U32(n) => n.to_string(),
            Cell::I64(n) => n.to_string(),
            Cell::F32(f) => {
                if f.is_nan() || f.is_infinite() {
                    "NULL".to_string() // Databricks doesn't handle NaN/Infinity well
                } else {
                    f.to_string()
                }
            }
            Cell::F64(f) => {
                if f.is_nan() || f.is_infinite() {
                    "NULL".to_string()
                } else {
                    f.to_string()
                }
            }
            Cell::Numeric(n) => n.to_string(),
            Cell::Date(d) => format!("DATE '{}'", d.format("%Y-%m-%d")),
            Cell::Time(t) => format!("'{}'", t.format("%H:%M:%S%.6f")),
            Cell::TimeStamp(ts) => format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f")),
            Cell::TimeStampTz(ts) => {
                format!("TIMESTAMP '{}'", ts.format("%Y-%m-%d %H:%M:%S%.6f %z"))
            }
            Cell::Uuid(u) => format!("'{}'", u),
            Cell::Json(j) => format!("'{}'", j.to_string().replace("'", "''")),
            Cell::Bytes(b) => {
                let hex_string = b
                    .iter()
                    .map(|byte| format!("{:02x}", byte))
                    .collect::<String>();
                format!("X'{}'", hex_string.to_uppercase())
            }
            Cell::Array(a) => {
                let elems = a.to_cells();
                if elems.is_empty() {
                    "array()".to_string()
                } else {
                    let mut parts = Vec::new();
                    for elem in elems.iter() {
                        parts.push(Self::cell_to_sql_value(elem)?);
                    }
                    format!("array({})", parts.join(", "))
                }
            }
        };
        Ok(value)
    }

    /// Builds fully qualified table name with proper escaping
    fn build_table_name(catalog: &str, schema: &str, table_name: &str) -> String {
        format!(
            "{}.{}.{}",
            Self::escape_identifier(catalog),
            Self::escape_identifier(schema),
            Self::escape_identifier(table_name)
        )
    }

    /// Escapes SQL identifiers by wrapping in backticks
    fn escape_identifier(identifier: &str) -> String {
        format!("`{}`", identifier.replace("`", "``"))
    }

    /// Extracts a column from the StatementResult at the specified column index,
    /// attempting to convert each value to type T. Supports both numeric (u64) and string parsing.
    fn result_column_as<T>(result: &StatementResult, col: usize) -> Vec<T>
    where
        T: FromStr + TryFrom<u64>,
    {
        result
            .data_array
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter_map(|row| row.get(col))
            .filter_map(|v| {
                // numeric path
                if let Some(n) = v.as_u64() {
                    if let Ok(val) = T::try_from(n) {
                        return Some(val);
                    }
                }
                // string path
                if let Some(s) = v.as_str() {
                    if let Ok(val) = s.parse::<T>() {
                        return Some(val);
                    }
                }
                None
            })
            .collect()
    }

    /// Extracts a column as Vec<String> from the StatementResult at the specified column index.
    fn result_column_as_string(result: &StatementResult, col: usize) -> Vec<String> {
        result
            .data_array
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .filter_map(|row| row.get(col))
            .filter_map(|v| v.as_str())
            .map(String::from)
            .collect()
    }

    /// Add a new column to an existing table
    pub async fn add_column_to_table(
        &self,
        table_name: &str,
        column: &ColumnSchema,
    ) -> Result<(), DatabricksError> {
        let column_type = Self::postgres_to_databricks_type(&column.typ);

        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);
        let query = format!(
            "ALTER TABLE {} ADD COLUMN {} {}",
            full_table_name, column.name, column_type
        );

        self.execute_statement(&query).await?;
        Ok(())
    }

    /// Get the columns of a table
    pub async fn get_table_columns(
        &self,
        table_name: &str,
    ) -> Result<HashSet<String>, DatabricksError> {
        let sql = format!(
            "SELECT column_name FROM information_schema.columns WHERE table_catalog = '{}' AND table_schema = '{}' AND table_name = '{}'",
            self.catalog, self.schema, table_name
        );

        let response = self.execute_sql(&sql).await?;

        if let Some(result) = response.result {
            let column_names = Self::result_column_as_string(&result, 0);
            let columns: HashSet<String> = column_names.into_iter().collect();
            Ok(columns)
        } else {
            Ok(HashSet::new())
        }
    }

    fn get_primary_key_columns(
        table_schema: &TableSchema,
        table_name: &str,
    ) -> Result<Vec<ColumnSchema>, DatabricksError> {
        let primary_key_columns: Vec<ColumnSchema> = table_schema
            .column_schemas
            .iter()
            .filter(|col| col.primary)
            .cloned()
            .collect();

        if primary_key_columns.is_empty() {
            return Err(DatabricksError::MissingPrimaryKeyColumns(
                table_name.to_string(),
            ));
        }
        Ok(primary_key_columns)
    }

    /// Merge rows into a table in databricks
    pub async fn merge_rows_to_table_direct(
        &self,
        table_schema: &TableSchema,
        column_schemas: &[ColumnSchema],
        table_rows: Vec<TableRow>,
        table_name: &str,
        clustering_columns: &[&str],
    ) -> Result<(), DatabricksError> {
        if table_rows.is_empty() {
            return Ok(());
        }

        let primary_key_columns = Self::get_primary_key_columns(table_schema, table_name)?;

        // Deduplicate rows in client by primary key, keeping latest version
        let deduplicated_rows = TableRowDeduplicator::deduplicate_by_primary_key(
            &table_rows,
            &primary_key_columns,
            column_schemas,
        )
        .map_err(|e| DatabricksError::StatementExecutionFailed(e.to_string()))?;

        info!(
            "Deduplicating {} rows to {} for table: {}",
            table_rows.len(),
            deduplicated_rows.len(),
            table_name
        );

        // Build the merge SQL
        let merge_sql = self.build_merge_sql(
            column_schemas,
            &primary_key_columns,
            &deduplicated_rows,
            table_name,
            clustering_columns,
        )?;

        info!("Executing merge to table: {}", table_name);

        self.execute_statement(&merge_sql).await.map_err(|e| {
            DatabricksError::StatementExecutionFailed(format!(
                "Failed to merge to table {}: {}",
                table_name, e
            ))
        })?;

        info!(
            "Successfully merged {} rows to table: {}",
            deduplicated_rows.len(),
            table_name
        );

        Ok(())
    }

    /// Build merge SQL statement
    fn build_merge_sql(
        &self,
        column_schemas: &[ColumnSchema],
        primary_key_columns: &[ColumnSchema],
        deduplicated_rows: &[TableRow],
        table_name: &str,
        clustering_columns: &[&str],
    ) -> Result<String, DatabricksError> {
        // Build primary key join conditions
        let pk_join = Self::build_merge_primary_key_join(
            column_schemas,
            primary_key_columns,
            deduplicated_rows,
            table_name,
            clustering_columns,
        );

        // Build column aliases for CTE SELECT
        let cte_select_columns = column_schemas
            .iter()
            .enumerate()
            .map(|(i, col)| format!("col{} as `{}`", i + 1, col.name))
            .collect::<Vec<_>>()
            .join(", ");

        // Filtered columns (excluding _operation)
        let insert_columns_str = column_schemas
            .iter()
            .filter(|col| col.name != "_operation")
            .map(|col| format!("`{}`", col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_values_list = column_schemas
            .iter()
            .filter(|col| col.name != "_operation")
            .map(|col| format!("`source_data`.`{}`", col.name))
            .collect::<Vec<_>>()
            .join(", ");

        // Build SET clause for UPDATE (exclude _operation from updates)
        let update_set: String = column_schemas
            .iter()
            .filter(|col| col.name != "_operation") // Don't update _operation
            .map(|col| format!("`{}` = `source_data`.`{}`", col.name, col.name))
            .collect::<Vec<_>>()
            .join(", ");

        // Build VALUES clause
        let values_rows: Result<Vec<String>, DatabricksError> = deduplicated_rows
            .iter()
            .map(|row| {
                let values: Result<Vec<String>, _> = row
                    .values
                    .iter()
                    .map(|cell| Self::cell_to_sql_value(cell))
                    .collect();

                values.map(|v| format!("({})", v.join(", ")))
            })
            .collect();

        let values_clause = values_rows?.join(",\n        ");
        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);

        let merge_sql = format!(
            r#"
            WITH source_data AS (
                SELECT
                    {}
                FROM VALUES
                    {}
            )
            MERGE INTO {}
            USING source_data
            ON {}
            WHEN MATCHED AND `source_data`.`_operation` = 'DELETE' THEN DELETE
            WHEN MATCHED AND `source_data`.`_operation` IN ('INSERT', 'UPDATE') THEN
                UPDATE SET {}
            WHEN NOT MATCHED AND `source_data`.`_operation` IN ('INSERT', 'UPDATE') THEN
                INSERT ({}) VALUES ({})
            "#,
            cte_select_columns,
            values_clause,
            full_table_name,
            pk_join,
            update_set,
            insert_columns_str,
            insert_values_list
        );

        Ok(merge_sql)
    }

    pub fn build_merge_primary_key_join(
        column_schemas: &[ColumnSchema],
        primary_key_columns: &[ColumnSchema],
        deduplicated_rows: &[TableRow],
        table_name: &str,
        clustering_columns: &[&str],
    ) -> String {
        // Build primary key join conditions
        let pk_join_conditions: Vec<String> = primary_key_columns
            .iter()
            .map(|col| {
                format!(
                    "`{}`.`{}` = `source_data`.`{}`",
                    table_name, col.name, col.name
                )
            })
            .collect();

        let mut join_conditions = pk_join_conditions;

        if clustering_columns.contains(&"created_at") {
            if let Some(col_idx) = column_schemas
                .iter()
                .position(|col| col.name == "created_at")
            {
                let mut min_datetime: Option<NaiveDateTime> = None;
                let mut max_datetime: Option<NaiveDateTime> = None;
                let mut all_have_created_at = true;

                for row in deduplicated_rows.iter() {
                    let datetime = match row.values.get(col_idx) {
                        Some(Cell::Date(date)) => Some(date.and_hms_opt(0, 0, 0).unwrap()),
                        Some(Cell::TimeStamp(datetime)) => Some(*datetime),
                        Some(Cell::TimeStampTz(datetime_tz)) => Some(datetime_tz.naive_utc()),
                        Some(Cell::Null) | None => {
                            all_have_created_at = false;
                            break;
                        }
                        _ => {
                            all_have_created_at = false;
                            break;
                        }
                    };

                    if let Some(dt) = datetime {
                        min_datetime = Some(min_datetime.map_or(dt, |min| min.min(dt)));
                        max_datetime = Some(max_datetime.map_or(dt, |max| max.max(dt)));
                    }
                }

                if all_have_created_at {
                    if let (Some(min_dt), Some(max_dt)) = (min_datetime, max_datetime) {
                        let date_filter = if min_dt.date() == max_dt.date() {
                            // Same date, use equality
                            format!(
                                "date(`{}`.`created_at`) = '{}'",
                                table_name,
                                min_dt.date().format("%Y-%m-%d")
                            )
                        } else {
                            // Date range
                            format!(
                                "`{}`.`created_at` >= '{}' AND `{}`.`created_at` <= '{}'",
                                table_name,
                                min_dt.format("%Y-%m-%d %H:%M:%S%.6f"),
                                table_name,
                                max_dt.format("%Y-%m-%d %H:%M:%S%.6f")
                            )
                        };
                        join_conditions.push(date_filter);
                    }
                }
            }
        }

        let pk_join = join_conditions.join(" AND ");

        return pk_join;
    }

    /// Get cluster columns for a table in databricks
    pub async fn get_cluster_columns(
        &self,
        table_name: &str,
    ) -> Result<Vec<String>, DatabricksError> {
        let full = Self::build_table_name(&self.catalog, &self.schema, table_name);
        let q = format!("SHOW TBLPROPERTIES {} ('clusteringColumns')", full);

        let result = match self.execute_query(&q).await {
            Ok(r) => r,
            Err(_) => return Ok(vec![]),
        };

        // Expect one row: [key, value]; value is like: [["created_at"],["id"]]
        let raw = result
            .data_array
            .as_deref()
            .and_then(|rows| rows.first())
            .and_then(|row| row.get(1)) // value column
            .and_then(|v| v.as_str());

        if let Some(s) = raw {
            // Parse JSON array-of-arrays and flatten
            if let Ok(v) = from_str::<Vec<Vec<String>>>(s) {
                return Ok(v.into_iter().flatten().collect());
            }
        }

        Ok(vec![])
    }

    /// Upload rows to s3
    pub async fn upload_rows_to_s3(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<String, DatabricksError> {
        if let Some(s3_client) = &self.s3_client {
            let s3_path = s3_client
                .upload_rows(table_name, column_schemas, table_rows)
                .await
                .map_err(|e| DatabricksError::S3Error(e))?;
            Ok(s3_path)
        } else {
            Err(DatabricksError::S3NotConfigured)
        }
    }

    /// Build merge SQL statement
    fn build_merge_from_s3_sql(
        &self,
        column_schemas: &[ColumnSchema],
        primary_key_columns: &[ColumnSchema],
        deduplicated_rows: &[TableRow],
        table_name: &str,
        clustering_columns: &[&str],
        s3_path: &str,
    ) -> Result<String, DatabricksError> {
        // Build primary key join conditions
        let pk_join = Self::build_merge_primary_key_join(
            column_schemas,
            primary_key_columns,
            deduplicated_rows,
            table_name,
            clustering_columns,
        );

        // Filtered columns (excluding _operation)
        let insert_columns_str = column_schemas
            .iter()
            .filter(|col| col.name != "_operation")
            .map(|col| format!("`{}`", col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let insert_values_list = column_schemas
            .iter()
            .filter(|col| col.name != "_operation")
            .map(|col| format!("`source_data`.`{}`", col.name))
            .collect::<Vec<_>>()
            .join(", ");

        // Build SET clause for UPDATE (exclude _operation from updates)
        let update_set: String = column_schemas
            .iter()
            .filter(|col| col.name != "_operation") // Don't update _operation
            .map(|col| format!("`{}` = `source_data`.`{}`", col.name, col.name))
            .collect::<Vec<_>>()
            .join(", ");

        let full_table_name = Self::build_table_name(&self.catalog, &self.schema, table_name);

        let merge_sql = format!(
            r#"
            MERGE INTO {}
            USING (
                SELECT *
                FROM read_files(
                    '{}',
                    format => 'parquet'
                )
            ) AS source_data
            ON {}
            WHEN MATCHED AND `source_data`.`_operation` = 'DELETE' THEN DELETE
            WHEN MATCHED AND `source_data`.`_operation` IN ('INSERT', 'UPDATE') THEN
                UPDATE SET {}
            WHEN NOT MATCHED AND `source_data`.`_operation` IN ('INSERT', 'UPDATE') THEN
                INSERT ({}) VALUES ({})
            "#,
            full_table_name, s3_path, pk_join, update_set, insert_columns_str, insert_values_list
        );

        Ok(merge_sql)
    }

    pub async fn merge_rows_to_table_using_s3(
        &self,
        table_schema: &TableSchema,
        column_schemas: &[ColumnSchema],
        table_rows: Vec<TableRow>,
        table_name: &str,
        clustering_columns: &[&str],
    ) -> Result<(), DatabricksError> {
        // Step 1: Get primary key columns from table schema
        let primary_key_columns = Self::get_primary_key_columns(table_schema, table_name)?;

        // Step 1: Upload rows to S3
        let s3_path = self
            .upload_rows_to_s3(table_name, column_schemas, &table_rows)
            .await?;

        info!("Uploaded rows to S3: {}", s3_path);

        // Step 3: Build merge SQL using S3 data
        let merge_sql = self.build_merge_from_s3_sql(
            column_schemas,
            &primary_key_columns,
            &table_rows,
            table_name,
            clustering_columns,
            &s3_path,
        )?;

        // Step 4: Execute merge query
        self.execute_statement(&merge_sql).await?;

        Ok(())
    }

    pub async fn merge_rows_to_table(
        &self,
        table_schema: &TableSchema,
        column_schemas: &[ColumnSchema],
        table_rows: Vec<TableRow>,
        table_name: &str,
        clustering_columns: &[&str],
    ) -> Result<(), DatabricksError> {
        if self.s3_client.is_some() {
            self.merge_rows_to_table_using_s3(
                table_schema,
                column_schemas,
                table_rows,
                table_name,
                clustering_columns,
            )
            .await?;
        } else {
            self.merge_rows_to_table_direct(
                table_schema,
                column_schemas,
                table_rows,
                table_name,
                clustering_columns,
            )
            .await?;
        }

        Ok(())
    }
}
