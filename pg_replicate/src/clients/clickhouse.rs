use std::collections::HashSet;
use std::fmt::Write;

use clickhouse::{error::Error as CHError, sql::Identifier, Client, Row};
use tokio_postgres::types::{PgLsn, Type};
use tracing::info;

use crate::conversions::{ArrayCell, Cell};
use crate::{
    conversions::table_row::TableRow,
    table::{ColumnSchema, TableId},
};
use serde::Deserialize;

// Define row structs with proper lifetime parameter
#[derive(Row, Deserialize)]
struct CountRow {
    count: u64,
}

#[derive(Row, Deserialize)]
struct LsnRow {
    lsn: i64,
}

#[derive(Row, Deserialize)]
struct TableIdRow {
    table_id: i64,
}

pub struct ClickHouseClient {
    client: Client,
    database: String,
}

impl ClickHouseClient {
    pub async fn new_with_credentials(
        url: &str,
        database: String,
        username: &str,
        password: &str,
    ) -> Result<ClickHouseClient, CHError> {
        let client = Client::default()
            .with_url(url)
            .with_user(username)
            .with_password(password)
            .with_database(&database);

        Ok(ClickHouseClient { client, database })
    }

    pub async fn create_or_update_table(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        engine: &str,
    ) -> Result<bool, CHError> {
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
                    self.add_column_to_table(&table_name, column).await?;
                }
            }
            Ok(false)
        } else {
            self.create_table(table_name, column_schemas, engine)
                .await?;
            Ok(true)
        }
    }

    pub async fn create_table_if_missing(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        engine: &str,
    ) -> Result<bool, CHError> {
        if self.table_exists(table_name).await? {
            Ok(false)
        } else {
            self.create_table(table_name, column_schemas, engine)
                .await?;
            Ok(true)
        }
    }

    fn postgres_to_clickhouse_type(typ: &Type) -> &'static str {
        match typ {
            &Type::BOOL => "Bool",
            &Type::CHAR | &Type::BPCHAR | &Type::VARCHAR | &Type::NAME | &Type::TEXT => "String",
            &Type::INT2 | &Type::INT4 | &Type::INT8 => "Int64",
            &Type::FLOAT4 | &Type::FLOAT8 | &Type::NUMERIC => "Float64",
            &Type::DATE => "Date",
            &Type::TIME => "String",
            &Type::TIMESTAMP | &Type::TIMESTAMPTZ => "DateTime64(6)",
            &Type::UUID => "UUID",
            &Type::JSON | &Type::JSONB => "String",
            &Type::OID => "UInt64",
            &Type::BYTEA => "String",
            &Type::BOOL_ARRAY => "Array(UInt8)",
            &Type::CHAR_ARRAY
            | &Type::BPCHAR_ARRAY
            | &Type::VARCHAR_ARRAY
            | &Type::NAME_ARRAY
            | &Type::TEXT_ARRAY => "Array(String)",
            &Type::INT2_ARRAY | &Type::INT4_ARRAY | &Type::INT8_ARRAY => "Array(Int64)",
            &Type::FLOAT4_ARRAY | &Type::FLOAT8_ARRAY | &Type::NUMERIC_ARRAY => "Array(Float64)",
            &Type::DATE_ARRAY => "Array(Date)",
            &Type::TIME_ARRAY => "Array(String)",
            &Type::TIMESTAMP_ARRAY => "Array(DateTime64(6))",
            &Type::TIMESTAMPTZ_ARRAY => "Array(DateTime64(6))",
            &Type::UUID_ARRAY => "Array(UUID)",
            &Type::JSON_ARRAY | &Type::JSONB_ARRAY => "Array(String)",
            &Type::OID_ARRAY => "Array(UInt64)",
            &Type::BYTEA_ARRAY => "Array(String)",
            _ => "String", // Default to String for unknown types
        }
    }

    fn is_array_type(typ: &Type) -> bool {
        matches!(
            typ,
            &Type::BOOL_ARRAY
                | &Type::CHAR_ARRAY
                | &Type::BPCHAR_ARRAY
                | &Type::VARCHAR_ARRAY
                | &Type::NAME_ARRAY
                | &Type::TEXT_ARRAY
                | &Type::INT2_ARRAY
                | &Type::INT4_ARRAY
                | &Type::INT8_ARRAY
                | &Type::FLOAT4_ARRAY
                | &Type::FLOAT8_ARRAY
                | &Type::NUMERIC_ARRAY
                | &Type::DATE_ARRAY
                | &Type::TIME_ARRAY
                | &Type::TIMESTAMP_ARRAY
                | &Type::TIMESTAMPTZ_ARRAY
                | &Type::UUID_ARRAY
                | &Type::JSON_ARRAY
                | &Type::JSONB_ARRAY
                | &Type::OID_ARRAY
                | &Type::BYTEA_ARRAY
        )
    }

    fn column_spec(column_schema: &ColumnSchema, s: &mut String) {
        s.push_str(&column_schema.name);
        s.push(' ');
        let typ = Self::postgres_to_clickhouse_type(&column_schema.typ);

        if column_schema.nullable && !Self::is_array_type(&column_schema.typ) {
            s.push_str(&format!("Nullable({})", typ));
        } else {
            s.push_str(typ);
        }
    }

    fn create_columns_spec(column_schemas: &[ColumnSchema], engine: &str) -> String {
        let mut s = String::new();
        s.push('(');

        for column_schema in column_schemas.iter() {
            Self::column_spec(column_schema, &mut s);
            s.push(',');
        }

        // _version and _is_deleted is meta column of replacingmergetree engine
        // https://clickhouse.com/docs/engines/table-engines/mergetree-family/replacingmergetree
        if engine == "ReplacingMergeTree" {
            s.push_str("_version UInt64 DEFAULT toUInt64(now64(9))*1000,");
            s.push_str("_is_deleted UInt8 DEFAULT 0");
        } else {
            s.pop(); // Remove the trailing comma
        }

        s.push(')');

        let primary_columns: Vec<&str> = column_schemas
            .iter()
            .filter(|s| s.primary)
            .map(|s| s.name.as_str())
            .collect();

        if engine == "ReplacingMergeTree" {
            s.push_str("ENGINE = ReplacingMergeTree(_version, _is_deleted) ");
        } else {
            s.push_str("ENGINE = MergeTree() ");
        }

        // https://clickhouse.com/docs/engines/table-engines/mergetree-family/mergetree#selecting-a-primary-key
        if primary_columns.is_empty() {
            s.push_str("ORDER BY tuple()");
        } else {
            s.push_str(&format!(" ORDER BY ({})", primary_columns.join(", ")));
        }

        s
    }

    pub async fn create_table(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        engine: &str,
    ) -> Result<(), CHError> {
        let columns_spec = Self::create_columns_spec(column_schemas, engine);

        info!(
            "creating table {}.{} in clickhouse",
            self.database, table_name
        );

        let query = format!(
            "CREATE TABLE IF NOT EXISTS {}.{} {}",
            &self.database, table_name, columns_spec
        );

        self.client.query(&query).execute().await?;
        Ok(())
    }

    pub async fn table_exists(&self, table_name: &str) -> Result<bool, CHError> {
        let row: CountRow = self
            .client
            .query("SELECT count() AS count FROM system.tables WHERE database = ? AND name = ?")
            .bind(&self.database)
            .bind(table_name)
            .fetch_one()
            .await?;

        Ok(row.count > 0)
    }

    pub async fn get_last_lsn(&self) -> Result<PgLsn, CHError> {
        // Use fetch_one directly with the LsnRow type
        let row: LsnRow = self
            .client
            .query("SELECT ?fields FROM ?._last_lsn")
            .bind(Identifier(&self.database))
            .fetch_one()
            .await?;

        Ok((row.lsn as u64).into())
    }

    pub async fn set_last_lsn(&self, lsn: PgLsn) -> Result<(), CHError> {
        let lsn: u64 = lsn.into();

        let database = &self.database;
        let query = format!("ALTER TABLE {database}._last_lsn UPDATE lsn = {lsn} WHERE id = 1",);

        let _ = self.client.query(&query).execute().await?;

        Ok(())
    }

    pub async fn insert_last_lsn_row(&self) -> Result<(), CHError> {
        let database = &self.database;
        let query = format!("INSERT INTO {database}._last_lsn (id, lsn) VALUES (1, 0)",);

        let _ = self.client.query(&query).execute().await?;

        Ok(())
    }

    pub async fn get_copied_table_ids(&self) -> Result<HashSet<TableId>, CHError> {
        let rows: Vec<TableIdRow> = self
            .client
            .query("SELECT table_id FROM ?._copied_tables")
            .bind(Identifier(&self.database))
            .fetch_all()
            .await?;

        Ok(rows
            .into_iter()
            .map(|row| row.table_id as TableId)
            .collect())
    }

    pub async fn insert_into_copied_tables(&self, table_id: TableId) -> Result<(), CHError> {
        let database = &self.database;
        let query = format!("INSERT INTO {database}._copied_tables (table_id) VALUES ({table_id})");

        let _ = self.client.query(&query).execute().await?;

        Ok(())
    }

    pub async fn insert_rows(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<(), CHError> {
        let batch_queries: Vec<String> =
            self.create_insert_batch_query(table_name, column_schemas, table_rows);
        for query in batch_queries {
            let _ = self.client.query(&query).execute().await?;
        }
        Ok(())
    }

    pub async fn drop_table(&self, table_name: &str) -> Result<(), CHError> {
        let database = &self.database;
        info!("dropping table {database}.{table_name} ClickHouse");
        let query = format!("drop table if exists {database}.{table_name}",);

        let _ = self.client.query(&query).execute().await?;

        Ok(())
    }

    pub async fn truncate_table(&self, table_name: &str) -> Result<(), CHError> {
        let database = &self.database;
        info!("truncating table {database}.{table_name} ClickHouse");
        let query = format!("truncate table if exists {database}.{table_name}",);

        let _ = self.client.query(&query).execute().await?;

        Ok(())
    }

    fn create_insert_batch_query(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Vec<String> {
        let database = &self.database;
        let mut batch_queries = Vec::new();
        let batch_size = 10000;

        // Process rows in batches
        for chunk in table_rows.chunks(batch_size) {
            let mut query = format!("INSERT INTO {database}.{table_name} (");

            for (i, column) in column_schemas.iter().enumerate() {
                query.push_str(&column.name);
                if i < column_schemas.len() - 1 {
                    query.push(',');
                }
            }

            query.push_str(") VALUES");

            // Add each row in the current batch
            for (i, table_row) in chunk.iter().enumerate() {
                if i > 0 {
                    query.push_str(", ");
                }

                query.push('(');

                // Add each value in the row
                for (j, value) in table_row.values.iter().enumerate() {
                    Self::cell_to_query_value(value, &mut query);
                    if j < table_row.values.len() - 1 {
                        query.push(',');
                    }
                }
                query.push(')');
            }

            batch_queries.push(query);
        }

        batch_queries
    }

    fn cell_to_query_value(cell: &Cell, s: &mut String) {
        match cell {
            Cell::Null => s.push_str("NULL"),
            Cell::Bool(b) => write!(s, "{b}").unwrap(),
            Cell::String(str) => {
                let escaped = str.replace('\'', "\\'").replace("?", "??");
                write!(s, "'{escaped}'").unwrap();
            }
            Cell::I16(i) => write!(s, "{i}").unwrap(),
            Cell::I32(i) => write!(s, "{i}").unwrap(),
            Cell::I64(i) => write!(s, "{i}").unwrap(),
            Cell::U32(i) => write!(s, "{i}").unwrap(),
            Cell::F32(f) => write!(s, "{f}").unwrap(),
            Cell::F64(f) => write!(s, "{f}").unwrap(),
            Cell::Numeric(n) => write!(s, "{}", n).unwrap(),
            Cell::Date(d) => write!(s, "'{}'", d.format("%Y-%m-%d")).unwrap(),
            Cell::Time(t) => write!(s, "'{}'", t.format("%H:%M:%S")).unwrap(),
            Cell::TimeStamp(t) => write!(s, "'{}'", t.format("%Y-%m-%dT%H:%M:%S.%6f")).unwrap(),
            Cell::TimeStampTz(t) => write!(s, "'{}'", t.format("%Y-%m-%dT%H:%M:%S.%6f")).unwrap(),
            Cell::Uuid(u) => write!(s, "'{u}'").unwrap(),
            Cell::Json(j) => {
                let escaped = j.to_string().replace('\'', "\\'").replace("?", "??");
                write!(s, "'{}'", escaped).unwrap();
            }
            Cell::Bytes(b) => {
                let hex: String = b.iter().map(|byte| format!("{:02x}", byte)).collect();
                write!(s, "unhex('{hex}')").unwrap();
            }
            Cell::Array(a) => {
                s.push('[');
                let mut first = true;
                for item in a.to_cells() {
                    if !first {
                        s.push_str(", ");
                    }
                    first = false;
                    Self::cell_to_query_value(&item, s);
                }
                s.push(']');
            }
        }
    }

    // Get existing columns for a table
    pub async fn get_table_columns(&self, table_name: &str) -> Result<HashSet<String>, CHError> {
        let query = format!(
            "SELECT name FROM system.columns WHERE table = '{}'",
            table_name
        );
        let result: Vec<String> = self.client.query(&query).fetch_all().await?;

        Ok(result.into_iter().collect())
    }

    // Add a new column to an existing table
    pub async fn add_column_to_table(
        &self,
        table_name: &str,
        column: &ColumnSchema,
    ) -> Result<(), CHError> {
        let mut column_type = String::new();
        Self::column_spec(column, &mut column_type);

        let query = format!("ALTER TABLE {} ADD COLUMN {}", table_name, column_type);

        self.client.query(&query).execute().await?;

        Ok(())
    }
}

impl ArrayCell {
    pub fn to_cells(&self) -> Vec<Cell> {
        match self {
            ArrayCell::Null => vec![],
            ArrayCell::Bool(vec) => vec
                .iter()
                .map(|v| v.map(Cell::Bool).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::String(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::String).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::I16(vec) => vec
                .iter()
                .map(|v| v.map(Cell::I16).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::I32(vec) => vec
                .iter()
                .map(|v| v.map(Cell::I32).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::U32(vec) => vec
                .iter()
                .map(|v| v.map(Cell::U32).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::I64(vec) => vec
                .iter()
                .map(|v| v.map(Cell::I64).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::F32(vec) => vec
                .iter()
                .map(|v| v.map(Cell::F32).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::F64(vec) => vec
                .iter()
                .map(|v| v.map(Cell::F64).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Numeric(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::Numeric).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Date(vec) => vec
                .iter()
                .map(|v| v.map(Cell::Date).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Time(vec) => vec
                .iter()
                .map(|v| v.map(Cell::Time).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::TimeStamp(vec) => vec
                .iter()
                .map(|v| v.map(Cell::TimeStamp).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::TimeStampTz(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::TimeStampTz).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Uuid(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::Uuid).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Json(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::Json).unwrap_or(Cell::Null))
                .collect(),
            ArrayCell::Bytes(vec) => vec
                .iter()
                .map(|v| v.clone().map(Cell::Bytes).unwrap_or(Cell::Null))
                .collect(),
        }
    }
}
