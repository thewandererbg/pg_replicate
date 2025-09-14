use crate::conversions::{table_row::TableRow, Cell};
use postgres::schema::ColumnSchema;

#[derive(Debug, thiserror::Error)]
pub enum DeduplicationError {
    #[error("_version column not found in schema")]
    VersionColumnNotFound,
    #[error("Column '{0}' not found in schema")]
    ColumnNotFound(String),
    #[error("Row has insufficient columns for '{0}'")]
    InsufficientColumns(String),
}

pub struct TableRowDeduplicator;

impl TableRowDeduplicator {
    pub fn deduplicate_by_primary_key(
        table_rows: &[TableRow],
        primary_key_columns: &[ColumnSchema],
        column_schemas: &[ColumnSchema],
    ) -> Result<Vec<TableRow>, DeduplicationError> {
        use std::collections::HashMap;

        let mut dedup_map: HashMap<String, TableRow> = HashMap::new();

        // Create column index mapping for efficient lookups
        let mut column_indices: HashMap<&str, usize> = HashMap::new();
        for (idx, col) in column_schemas.iter().enumerate() {
            column_indices.insert(&col.name, idx);
        }

        // Find _version column index
        let version_index = column_schemas
            .iter()
            .position(|col| col.name == "_version")
            .ok_or_else(|| DeduplicationError::VersionColumnNotFound)?;

        for row in table_rows {
            // Build primary key string for this row
            let pk_parts: Result<Vec<String>, _> = primary_key_columns
                .iter()
                .map(|col| {
                    let col_idx = column_indices
                        .get(col.name.as_str())
                        .ok_or_else(|| DeduplicationError::ColumnNotFound(col.name.clone()))?;

                    if *col_idx >= row.values.len() {
                        return Err(DeduplicationError::InsufficientColumns(col.name.clone()));
                    }

                    Ok(Self::cell_to_string(&row.values[*col_idx]))
                })
                .collect();

            let primary_key = pk_parts?.join("|");

            // Get version for comparison - _version is always i64
            let current_version = match &row.values[version_index] {
                Cell::I64(v) => v,
                _ => unreachable!("_version should always be i64"),
            };

            // Keep row if it's the first with this PK or has higher version
            match dedup_map.get(&primary_key) {
                None => {
                    dedup_map.insert(primary_key, row.clone());
                }
                Some(existing_row) => {
                    let existing_version = match &existing_row.values[version_index] {
                        Cell::I64(v) => v,
                        _ => unreachable!("_version should always be i64"),
                    };

                    if current_version >= existing_version {
                        dedup_map.insert(primary_key, row.clone());
                    }
                }
            }
        }

        Ok(dedup_map.into_values().collect())
    }

    /// Helper function to convert Cell to string for primary key building
    fn cell_to_string(cell: &Cell) -> String {
        match cell {
            Cell::Null => "NULL".to_string(),
            Cell::Bool(b) => b.to_string(),
            Cell::String(s) => s.clone(),
            Cell::I16(i) => i.to_string(),
            Cell::I32(i) => i.to_string(),
            Cell::U32(i) => i.to_string(),
            Cell::I64(i) => i.to_string(),
            Cell::F32(f) => f.to_string(),
            Cell::F64(d) => d.to_string(),
            Cell::Numeric(n) => n.to_string(),
            Cell::Date(d) => d.to_string(),
            Cell::Time(t) => t.to_string(),
            Cell::TimeStamp(ts) => ts.to_string(),
            Cell::TimeStampTz(ts) => ts.to_string(),
            Cell::Uuid(u) => u.to_string(),
            Cell::Json(j) => j.to_string(),
            Cell::Bytes(b) => format!("{:?}", b), // Simple debug representation
            Cell::Array(_) => "ARRAY".to_string(), // Arrays not typically used in primary keys
        }
    }
}
