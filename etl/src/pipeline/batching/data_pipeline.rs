use futures::Stream;
use futures::{future::try_join_all, StreamExt};
use postgres::schema::TableId;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{collections::HashSet, time::Instant};
use tokio::pin;
use tokio::sync::{mpsc, Notify};
use tokio_postgres::types::PgLsn;
use tracing::{debug, info};

use crate::{
    conversions::cdc_event::{CdcEvent, CdcEventConversionError},
    pipeline::{
        batching::stream::BatchTimeoutStream,
        destinations::BatchDestination,
        sources::{postgres::CdcStreamError, CommonSourceError, Source},
        PipelineAction, PipelineError,
    },
};

use super::BatchConfig;

// Simple receiver-to-stream wrapper
struct ReceiverStream<T> {
    receiver: mpsc::UnboundedReceiver<T>,
}

impl<T> ReceiverStream<T> {
    fn new(receiver: mpsc::UnboundedReceiver<T>) -> Self {
        Self { receiver }
    }
}

impl<T> Stream for ReceiverStream<T> {
    type Item = T;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

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
    batch_configs: Vec<BatchConfig>,
    destination_ids: Vec<String>,
    action: PipelineAction,
    copy_tables_stream_stop: Arc<Notify>,
    cdc_stream_stop: Arc<Notify>,
}

impl<Src: Source, Dst: BatchDestination> BatchDataPipeline<Src, Dst> {
    pub fn new(
        source: Src,
        destinations_with_configs: Vec<(Dst, BatchConfig, String)>,
        action: PipelineAction,
    ) -> Self {
        let mut destinations = Vec::new();
        let mut batch_configs = Vec::new();
        let mut destination_ids = Vec::new();

        for (destination, batch_config, destination_id) in destinations_with_configs {
            destinations.push(destination);
            batch_configs.push(batch_config);
            destination_ids.push(destination_id);
        }

        BatchDataPipeline {
            source,
            destinations,
            batch_configs,
            destination_ids,
            action,
            copy_tables_stream_stop: Arc::new(Notify::new()),
            cdc_stream_stop: Arc::new(Notify::new()),
        }
    }

    async fn copy_table_schemas(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        let table_schemas = self.source.get_table_schemas();

        if !table_schemas.is_empty() {
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

            // Use single batch stream for table copying (same as original)
            let batch_timeout_stream = BatchTimeoutStream::new(
                table_rows,
                BatchConfig::new(10000, Duration::from_millis(5000)),
                self.copy_tables_stream_stop.notified(),
            );

            pin!(batch_timeout_stream);

            while let Some(batch) = batch_timeout_stream.next().await {
                info!("got {} table copy events in a batch", batch.len());
                let mut rows = Vec::with_capacity(batch.len());
                for row in batch {
                    rows.push(row.map_err(CommonSourceError::TableCopyStream)?);
                }

                // Write to all destinations in parallel
                let write_futures: Vec<_> = self
                    .destinations
                    .iter_mut()
                    .map(|dest| dest.write_table_rows(rows.clone(), table_schema.id))
                    .collect();

                try_join_all(write_futures)
                    .await
                    .map_err(PipelineError::Destination)?;
            }

            // Mark table as copied on all destinations
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

        // Single CDC stream (the source)
        let cdc_events = self
            .source
            .get_cdc_stream(last_lsn.into())
            .await
            .map_err(PipelineError::Source)?;
        pin!(cdc_events);

        // Hard-coded for 2 destinations - change this assertion for different counts
        assert_eq!(
            self.destinations.len(),
            2,
            "This implementation assumes exactly 2 destinations"
        );

        // Create channels for both destinations
        let (sender_0, receiver_0) = mpsc::unbounded_channel::<CdcEvent>();
        let (sender_1, receiver_1) = mpsc::unbounded_channel::<CdcEvent>();
        let destination_senders = vec![sender_0, sender_1];

        // Create batch streams for each destination with their individual configs
        let receiver_stream_0 = ReceiverStream::new(receiver_0);
        let batch_stream_0 = BatchTimeoutStream::new(
            receiver_stream_0,
            self.batch_configs[0].clone(),
            self.cdc_stream_stop.notified(),
        );
        pin!(batch_stream_0);

        let receiver_stream_1 = ReceiverStream::new(receiver_1);
        let batch_stream_1 = BatchTimeoutStream::new(
            receiver_stream_1,
            self.batch_configs[1].clone(),
            self.cdc_stream_stop.notified(),
        );
        pin!(batch_stream_1);

        // Main event processing loop
        let mut ping_interval = tokio::time::interval(Duration::from_secs(10));
        ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut stream0_lsn = last_lsn.into();
        let mut stream1_lsn = last_lsn.into();
        let mut current_lsn = std::cmp::min(stream0_lsn, stream1_lsn);

        loop {
            tokio::select! {
                // Read from source stream
                event_opt = cdc_events.next() => {
                    match event_opt {
                        Some(event) => {
                            // Handle conversion errors
                            if let Err(CdcStreamError::CdcEventConversion(
                                CdcEventConversionError::MissingSchema(_),
                            )) = event
                            {
                                continue;
                            }
                            let event = event.map_err(CommonSourceError::CdcStream)?;

                            // Broadcast to all destinations
                            for sender in &destination_senders {
                                if let Err(_) = sender.send(event.clone()) {
                                    return Err(PipelineError::Unexpected());
                                }
                            }
                        }
                        None => {
                            info!("CDC stream ended");
                            break;
                        }
                    }
                }

                // Poll destination 0's batch stream
                batch_opt = batch_stream_0.next() => {
                    match batch_opt {
                        Some(batch) => {
                            info!("got {} cdc events in batch for destination {}",
                                  batch.len(), self.destination_ids[0]);

                            if !batch.is_empty() {
                                // Write with ping mechanism inline
                                let write_future = self.destinations[0].write_cdc_events(batch);
                                pin!(write_future);

                                let mut write_ping_interval = tokio::time::interval(Duration::from_secs(10));
                                write_ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                                let lsn = loop {
                                    tokio::select! {
                                        result = &mut write_future => {
                                            break result.map_err(PipelineError::Destination)?;
                                        }
                                        _ = write_ping_interval.tick() => {
                                            let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                                        }
                                    }
                                };

                                stream0_lsn = lsn;
                                // Send status update with the new LSN
                                let current_lsn = std::cmp::min(stream0_lsn, stream1_lsn);
                                let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                            }
                        }
                        None => {
                            info!("Destination 0 batch stream ended");
                        }
                    }
                }

                // Poll destination 1's batch stream
                batch_opt = batch_stream_1.next() => {
                    match batch_opt {
                        Some(batch) => {
                            info!("got {} cdc events in batch for destination {}",
                                  batch.len(), self.destination_ids[1]);

                            if !batch.is_empty() {
                                // Write with ping mechanism inline
                                let write_future = self.destinations[1].write_cdc_events(batch);
                                pin!(write_future);

                                let mut write_ping_interval = tokio::time::interval(Duration::from_secs(10));
                                write_ping_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

                                let lsn = loop {
                                    tokio::select! {
                                        result = &mut write_future => {
                                            break result.map_err(PipelineError::Destination)?;
                                        }
                                        _ = write_ping_interval.tick() => {
                                            let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                                        }
                                    }
                                };

                                stream1_lsn = lsn;
                                // Send status update with the new LSN
                                current_lsn = std::cmp::min(stream0_lsn, stream1_lsn);
                                let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                            }
                        }
                        None => {
                            info!("Destination 1 batch stream ended");
                        }
                    }
                }

                _ = ping_interval.tick() => {
                    debug!("Keeping connection alive, current LSN: {}", current_lsn);
                    let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                }

                else => break
            }
        }

        Ok(())
    }

    pub async fn start(&mut self) -> Result<(), PipelineError<Src::Error, Dst::Error>> {
        // Get resumption state from all destinations
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

        // Only consider tables copied if ALL destinations have them
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
