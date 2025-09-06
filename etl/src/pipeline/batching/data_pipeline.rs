use futures::{future::try_join_all, StreamExt};
use postgres::schema::TableId;
use std::sync::Arc;
use std::{collections::HashSet, time::Instant};
use tokio::pin;
use tokio::sync::Notify;
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::{
    conversions::cdc_event::CdcEventConversionError,
    pipeline::{
        batching::stream::BatchTimeoutStream,
        destinations::BatchDestination,
        sources::{postgres::CdcStreamError, CommonSourceError, Source},
        PipelineAction, PipelineError,
    },
};

use super::BatchConfig;

#[derive(Debug, Clone)]
pub struct BatchDataPipelineHandle {
    copy_tables_stream_stop: Arc<Notify>,
    cdc_stream_stop: Arc<Notify>,
}

impl BatchDataPipelineHandle {
    pub fn stop(&self) {
        self.copy_tables_stream_stop.notify_one();
        self.cdc_stream_stop.notify_one();
    }
}

pub struct BatchDataPipeline<Src: Source, Dst: BatchDestination> {
    source: Src,
    destinations: Vec<Dst>,
    action: PipelineAction,
    batch_config: BatchConfig,
    copy_tables_stream_stop: Arc<Notify>,
    cdc_stream_stop: Arc<Notify>,
}

impl<Src: Source, Dst: BatchDestination> BatchDataPipeline<Src, Dst> {
    pub fn new(
        source: Src,
        destinations: Vec<Dst>,
        action: PipelineAction,
        batch_config: BatchConfig,
    ) -> Self {
        BatchDataPipeline {
            source,
            destinations,
            action,
            batch_config,
            copy_tables_stream_stop: Arc::new(Notify::new()),
            cdc_stream_stop: Arc::new(Notify::new()),
        }
    }

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let table_schemas = self.source.get_table_schemas();
        let table_schemas = table_schemas.clone();

        if !table_schemas.is_empty() {
            // Write table schemas to all destinations in parallel
            let schema_futures: Vec<_> = self
                .destinations
                .iter_mut()
                .map(|dest| dest.write_table_schemas(table_schemas.clone()))
                .collect();

            try_join_all(schema_futures)
                .await
                .map_err(PipelineError::Destination)?;
        }

        Ok(())
    }

    async fn copy_tables(
        &mut self,
        copied_tables: &HashSet<TableId>,
    ) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let start = Instant::now();
        let table_schemas = self.source.get_table_schemas();

        let mut keys: Vec<u32> = table_schemas.keys().copied().collect();
        keys.sort();

        for key in keys {
            let table_schema = table_schemas.get(&key).expect("failed to get table key");
            if copied_tables.contains(&table_schema.id) {
                info!("table {} already copied.", table_schema.name);
                continue;
            }

            // Truncate table on all destinations in parallel
            let truncate_futures: Vec<_> = self
                .destinations
                .iter_mut()
                .map(|dest| dest.truncate_table(table_schema.id))
                .collect();

            try_join_all(truncate_futures)
                .await
                .map_err(PipelineError::Destination)?;

            let table_rows = self
                .source
                .get_table_copy_stream(&table_schema.name, &table_schema.column_schemas)
                .await
                .map_err(PipelineError::Source)?;

            let batch_timeout_stream = BatchTimeoutStream::new(
                table_rows,
                self.batch_config.clone(),
                self.copy_tables_stream_stop.notified(),
            );

            pin!(batch_timeout_stream);

            while let Some(batch) = batch_timeout_stream.next().await {
                info!("got {} table copy events in a batch", batch.len());
                //TODO: Avoid a vec copy
                let mut rows = Vec::with_capacity(batch.len());
                for row in batch {
                    rows.push(row.map_err(CommonSourceError::TableCopyStream)?);
                }

                // Write rows to all destinations in parallel
                let write_futures: Vec<_> = self
                    .destinations
                    .iter_mut()
                    .map(|dest| dest.write_table_rows(rows.clone(), table_schema.id))
                    .collect();

                try_join_all(write_futures)
                    .await
                    .map_err(PipelineError::Destination)?;
            }

            // Mark table as copied on all destinations in parallel
            let copied_futures: Vec<_> = self
                .destinations
                .iter_mut()
                .map(|dest| dest.table_copied(table_schema.id))
                .collect();

            try_join_all(copied_futures)
                .await
                .map_err(PipelineError::Destination)?;
        }

        self.source
            .commit_transaction()
            .await
            .map_err(PipelineError::Source)?;

        let end = Instant::now();
        let seconds = (end - start).as_secs();
        debug!("took {seconds} seconds to copy tables");

        Ok(())
    }

    async fn copy_cdc_events(
        &mut self,
        last_lsn: PgLsn,
    ) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        self.source
            .commit_transaction()
            .await
            .map_err(PipelineError::Source)?;

        let mut last_lsn: u64 = last_lsn.into();
        last_lsn += 1;

        let cdc_events = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .map_err(PipelineError::Source)?;
        pin!(cdc_events);

        let batch_timeout_stream = BatchTimeoutStream::new(
            cdc_events,
            self.batch_config.clone(),
            self.cdc_stream_stop.notified(),
        );
        pin!(batch_timeout_stream);

        // Ping the postgresql database each 10s to keep connection alive
        // in case wal_sender_timeout < max_batch_fill_time
        let mut ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut current_lsn = last_lsn.into();

        loop {
            tokio::select! {
                batch_opt = batch_timeout_stream.next() => {
                    match batch_opt {
                        Some(batch) => {
                            info!("got {} cdc events in a batch", batch.len());
                            let mut events = Vec::with_capacity(batch.len());
                            for event in batch {
                                if let Err(CdcStreamError::CdcEventConversion(
                                    CdcEventConversionError::MissingSchema(_),
                                )) = event
                                {
                                    continue;
                                }
                                let event = event.map_err(CommonSourceError::CdcStream)?;
                                events.push(event);
                            }

                            // Write to all destinations in parallel with ping mechanism
                            let write_futures: Vec<_> = self.destinations
                                .iter_mut()
                                .map(|dest| dest.write_cdc_events(events.clone()))
                                .collect();

                            let write_future = try_join_all(write_futures);
                            pin!(write_future);

                            let mut write_ping_interval = tokio::time::interval(std::time::Duration::from_secs(10));
                            write_ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                            let lsn_results = loop {
                                tokio::select! {
                                    result = &mut write_future => {
                                        break result.map_err(PipelineError::Destination)?;
                                    }
                                    _ = write_ping_interval.tick() => {
                                        // Send ping with current LSN to keep connection alive during write
                                        let inner = unsafe {
                                            batch_timeout_stream
                                                .as_mut()
                                                .get_unchecked_mut()
                                                .get_inner_mut()
                                        };
                                        let _ = inner.as_mut().send_status_update(current_lsn).await;
                                    }
                                }
                            };

                            // Get minimum LSN across all destinations
                            let min_lsn = lsn_results.iter().min().unwrap();
                            current_lsn = *min_lsn;

                            let inner = unsafe {
                                batch_timeout_stream
                                    .as_mut()
                                    .get_unchecked_mut()
                                    .get_inner_mut()
                            };
                            inner
                                .as_mut()
                                .send_status_update(*min_lsn)
                                .await
                                .map_err(CommonSourceError::StatusUpdate)?;
                        }
                        None => {
                            info!("CDC stream unexpected error");
                            return Err(PipelineError::CommonSource(
                                CommonSourceError::CdcStream(
                                    CdcStreamError::CdcEventConversion(
                                        CdcEventConversionError::UnknownReplicationMessage
                                    )
                                )
                            ));
                        }
                    }
                }
                _ = ping_interval.tick() => {
                    // Send ping with current LSN to keep connection alive during waiting
                    let inner = unsafe {
                        batch_timeout_stream
                            .as_mut()
                            .get_unchecked_mut()
                            .get_inner_mut()
                    };
                    let _ = inner.as_mut().send_status_update(current_lsn).await;
                }
                else => break
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        // Get resumption state from all destinations and use the minimum LSN
        let resumption_futures: Vec<_> = self
            .destinations
            .iter_mut()
            .map(|dest| dest.get_resumption_state())
            .collect();

        let resumption_states = try_join_all(resumption_futures)
            .await
            .map_err(PipelineError::Destination)?;

        // Find minimum LSN and intersection of copied tables
        let min_lsn = resumption_states
            .iter()
            .map(|state| state.last_lsn)
            .min()
            .unwrap_or_else(|| PgLsn::from(0));

        // Only consider tables copied if ALL destinations have them copied
        let mut copied_tables = resumption_states[0].copied_tables.clone();
        for state in &resumption_states[1..] {
            copied_tables = copied_tables
                .intersection(&state.copied_tables)
                .cloned()
                .collect();
        }

        match self.action {
            PipelineAction::TableCopiesOnly => {
                self.copy_table_schemas().await?;
                self.copy_tables(&copied_tables).await?;
            }
            PipelineAction::CdcOnly => {
                self.copy_table_schemas().await?;
                self.copy_cdc_events(min_lsn).await?;
            }
            PipelineAction::Both => {
                self.copy_table_schemas().await?;
                self.copy_tables(&copied_tables).await?;
                self.copy_cdc_events(min_lsn).await?;
            }
        }

        Ok(())
    }

    pub fn handle(&self) -> BatchDataPipelineHandle {
        BatchDataPipelineHandle {
            copy_tables_stream_stop: self.copy_tables_stream_stop.clone(),
            cdc_stream_stop: self.cdc_stream_stop.clone(),
        }
    }

    pub fn source(&self) -> &Src {
        &self.source
    }

    pub fn destinations(&self) -> &[Dst] {
        &self.destinations
    }
}
