use crate::conversions::table_row::TableRow;
use crate::conversions::Cell;
use crate::utils::parquet::postgres_to_parquet_type;
use arrow::array::*;
use arrow::datatypes::{Field, Schema};
use arrow::record_batch::RecordBatch;
use aws_config::{BehaviorVersion, Region};
use aws_sdk_s3::{primitives::ByteStream, Client};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use postgres::schema::ColumnSchema;
use std::sync::Arc;
use tracing::{error, info};

/// Type alias for common S3 errors - using AWS SDK's native error types
pub type S3Error = Box<dyn std::error::Error + Send + Sync>;

/// S3 client for CDC operations
#[derive(Clone)]
pub struct S3Client {
    client: Client,
    bucket: String,
    prefix: String,
}

impl S3Client {
    /// Creates a new S3 client with AWS SDK
    pub async fn new(
        bucket: String,
        region: String,
        prefix: String,
        access_key: String,
        secret_key: String,
    ) -> Result<S3Client, S3Error> {
        let credentials =
            aws_sdk_s3::config::Credentials::new(access_key, secret_key, None, None, "static");

        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(Region::new(region))
            .credentials_provider(credentials)
            .load()
            .await;

        let client = Client::new(&config);

        let s3_client = S3Client {
            client,
            bucket: bucket.clone(),
            prefix,
        };

        // Test connection with simple head bucket operation
        s3_client.test_connection().await?;

        Ok(s3_client)
    }

    /// Tests S3 connection
    async fn test_connection(&self) -> Result<(), S3Error> {
        info!("Testing S3 connection to bucket: {}", self.bucket);

        match self.client.head_bucket().bucket(&self.bucket).send().await {
            Ok(_) => {
                info!("S3 connection successful");
                Ok(())
            }
            Err(e) => {
                error!("S3 connection failed: {:?}", e);
                Err(Box::new(e))
            }
        }
    }

    /// Lists objects with given prefix
    pub async fn list_objects(&self, prefix: &str) -> Result<Vec<String>, S3Error> {
        let full_prefix = if self.prefix.is_empty() {
            prefix.to_string()
        } else {
            format!("{}/{}", self.prefix, prefix)
        };

        info!("Listing objects with prefix: {}", full_prefix);

        let response = self
            .client
            .list_objects_v2()
            .bucket(&self.bucket)
            .prefix(&full_prefix)
            .send()
            .await?;

        let keys: Vec<String> = response
            .contents()
            .iter()
            .filter_map(|obj| obj.key())
            .map(|key| key.to_string())
            .collect();

        info!("Found {} objects", keys.len());
        Ok(keys)
    }

    pub async fn upload_parquet(
        &self,
        key: &str,
        parquet_data: Vec<u8>,
    ) -> Result<String, S3Error> {
        let parquet_key = if key.ends_with(".parquet") {
            key.to_string()
        } else {
            format!("{}.parquet", key)
        };

        info!(
            "Uploading {} bytes as Parquet to s3://{}/{}",
            parquet_data.len(),
            self.bucket,
            parquet_key
        );

        let byte_stream = ByteStream::from(parquet_data);

        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(&parquet_key)
            .body(byte_stream)
            .content_type("application/octet-stream")
            .send()
            .await?;

        let s3_path = format!("s3://{}/{}", self.bucket, parquet_key);
        info!("Successfully uploaded Parquet to: {}", s3_path);
        Ok(s3_path)
    }

    /// Uploads table rows to s3 as Parquet with schema information
    pub async fn upload_rows(
        &self,
        table_name: &str,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<String, S3Error> {
        // Generate timestamp-based key for partitioning
        let timestamp = chrono::Utc::now();
        let date_partition = timestamp.format("%Y%m%d").to_string();
        let time_str = timestamp.format("%H%M%S").to_string();
        let timestamp_micros = timestamp.timestamp_nanos_opt().unwrap_or(0) / 1000;

        let key = if self.prefix.is_empty() {
            format!(
                "{}/{}/data_{}_{}",
                table_name, date_partition, time_str, timestamp_micros
            )
        } else {
            format!(
                "{}/{}/{}/data_{}_{}",
                self.prefix, table_name, date_partition, time_str, timestamp_micros
            )
        };

        // Convert to Parquet format
        let parquet_data = self.convert_to_parquet(column_schemas, table_rows)?;

        info!("Uploading table rows for '{}' as Parquet", table_name);
        self.upload_parquet(&key, parquet_data).await
    }

    /// Convert table rows to Parquet with explicit type mapping
    pub fn convert_to_parquet(
        &self,
        column_schemas: &[ColumnSchema],
        table_rows: &[TableRow],
    ) -> Result<Vec<u8>, S3Error> {
        if table_rows.is_empty() {
            return Ok(Vec::new());
        }

        // Create Arrow schema with exact type mapping
        let fields: Vec<Field> = column_schemas
            .iter()
            .map(|col| Field::new(&col.name, postgres_to_parquet_type(&col.typ), col.nullable))
            .collect();
        let schema = Arc::new(Schema::new(fields));

        // Build typed columns using your parquet.rs function
        let mut columns: Vec<Arc<dyn Array>> = Vec::new();

        for (col_idx, col_schema) in column_schemas.iter().enumerate() {
            // Extract cells for this column
            let cells: Vec<Cell> = table_rows
                .iter()
                .map(|row| row.get(col_idx).cloned().unwrap_or(Cell::Null))
                .collect();

            // Convert using your existing parquet.rs function
            let parquet_type = postgres_to_parquet_type(&col_schema.typ);
            let array = crate::utils::parquet::cell_to_parquet_value(&cells, &parquet_type)
                .map_err(|e| Box::new(e) as S3Error)?;
            columns.push(array);
        }

        let batch = RecordBatch::try_new(schema.clone(), columns)?;

        // Write with optimal settings for Databricks
        let mut buffer = Vec::new();
        let props = WriterProperties::builder()
            .set_compression(Compression::SNAPPY)
            .set_max_row_group_size(128 * 1024 * 1024)
            .build();

        let mut writer = ArrowWriter::try_new(&mut buffer, schema, Some(props))?;
        writer.write(&batch)?;
        writer.close()?;

        Ok(buffer)
    }
}
