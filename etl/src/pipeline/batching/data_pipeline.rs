use futures::Stream;
use futures::{future::try_join_all, StreamExt};
use postgres::schema::TableId;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use std::{collections::HashSet, time::Instant};
use tokio::pin;
use tokio::sync::{mpsc, Mutex, Notify};
use tokio::time::{interval, MissedTickBehavior};
use tokio_postgres::types::PgLsn;
use tracing::{debug, error, info};

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

impl<Src: Source, Dst: BatchDestination + Send + 'static> BatchDataPipeline<Src, Dst> {
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

        // Create individual channels for each destination
        let mut destination_senders = Vec::new();
        let mut destination_receivers = Vec::new();

        for _ in 0..self.destinations.len() {
            let (sender, receiver) = mpsc::unbounded_channel::<CdcEvent>();
            destination_senders.push(sender);
            destination_receivers.push(receiver);
        }

        // Shared LSN tracking
        let lsn_tracker = Arc::new(Mutex::new(HashMap::new()));

        // Move destinations out of self and spawn tasks
        let destinations = std::mem::take(&mut self.destinations);
        let destination_ids = std::mem::take(&mut self.destination_ids);
        let batch_configs = std::mem::take(&mut self.batch_configs);
        let mut task_handles = Vec::new();

        for (idx, destination) in destinations.into_iter().enumerate() {
            let destination_id = destination_ids[idx].clone();
            let batch_config = batch_configs[idx].clone();
            let receiver = destination_receivers.remove(0);
            let lsn_tracker_clone = lsn_tracker.clone();

            let handle = tokio::spawn(destination_task(
                destination,
                destination_id,
                batch_config,
                receiver,
                lsn_tracker_clone,
                last_lsn.into(),
            ));

            task_handles.push(handle);
        }

        // Main event processing loop
        let mut ping_interval = interval(Duration::from_secs(10));
        ping_interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

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

                            // Send to all destination channels
                            for sender in &destination_senders {
                                if let Err(_) = sender.send(event.clone()) {
                                    return Err(PipelineError::Pipeline("Destination task stopped unexpectedly".to_string()));
                                }
                            }
                        }
                        None => {
                            // CDC stream ended unexpectedly, return error
                            return Err(PipelineError::Unexpected());
                        }
                    }
                }

                _ = ping_interval.tick() => {
                    // Calculate minimum LSN from all destinations
                    let current_lsn = {
                        let lsn_map = lsn_tracker.lock().await;
                        if lsn_map.is_empty() {
                            last_lsn.into()
                        } else {
                            *lsn_map.values().min().unwrap_or(&(last_lsn.into()))
                        }
                    };

                    debug!("Keeping connection alive, current LSN: {}", current_lsn);
                    let _ = cdc_events.as_mut().send_status_update(current_lsn).await;
                }

                // Check if any destination task has failed
                result = futures::future::select_all(&mut task_handles) => {
                    let (task_result, _index, _remaining) = result;
                    match task_result {
                        Ok(task_outcome) => {
                            match task_outcome {
                                Ok(_) => return Err(PipelineError::Pipeline("Destination task completed unexpectedly".to_string())),
                                Err(dest_error) => return Err(PipelineError::Destination(dest_error)),
                            }
                        }
                        Err(join_error) => return Err(PipelineError::Pipeline(format!("Destination task panicked: {}", join_error))),
                    }
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

async fn destination_task<Dest>(
    mut destination: Dest,
    destination_id: String,
    batch_config: BatchConfig,
    event_receiver: mpsc::UnboundedReceiver<CdcEvent>,
    lsn_tracker: Arc<Mutex<HashMap<String, PgLsn>>>,
    initial_lsn: PgLsn,
) -> Result<(), Dest::Error>
where
    Dest: BatchDestination + Send + 'static,
{
    // Initialize this destination's LSN in the tracker
    {
        let mut lsn_map = lsn_tracker.lock().await;
        lsn_map.insert(destination_id.clone(), initial_lsn);
    }

    // Create batching stream directly from the receiver
    let dummy_notify = Arc::new(tokio::sync::Notify::new());
    let receiver_stream = ReceiverStream::new(event_receiver);
    let batch_stream =
        BatchTimeoutStream::new(receiver_stream, batch_config, dummy_notify.notified());
    pin!(batch_stream);

    loop {
        match batch_stream.next().await {
            Some(batch) => {
                if !batch.is_empty() {
                    info!(
                        "got {} cdc events in batch for destination {}",
                        batch.len(),
                        destination_id
                    );

                    if let Err(e) = write_batch_with_retry(
                        &mut destination,
                        batch,
                        &destination_id,
                        &lsn_tracker,
                    )
                    .await
                    {
                        let mut lsn_map = lsn_tracker.lock().await;
                        lsn_map.remove(&destination_id);
                        return Err(e);
                    }
                }
            }
            None => {
                info!("Destination {} batch stream ended", destination_id);
                break;
            }
        }
    }

    // Clean up this destination's LSN from tracker
    let mut lsn_map = lsn_tracker.lock().await;
    lsn_map.remove(&destination_id);

    Ok(())
}

async fn write_batch_with_retry<Dest>(
    destination: &mut Dest,
    batch: Vec<CdcEvent>,
    destination_id: &str,
    lsn_tracker: &Arc<Mutex<HashMap<String, PgLsn>>>,
) -> Result<(), Dest::Error>
where
    Dest: BatchDestination + Send + 'static,
{
    const INITIAL_DELAY: Duration = Duration::from_secs(30);
    const MAX_DELAY: Duration = Duration::from_secs(5 * 60);
    const MAX_TOTAL_TIME: Duration = Duration::from_secs(30 * 60);

    let mut current_delay = INITIAL_DELAY;
    let start_time = Instant::now();
    let mut attempt = 1;

    loop {
        match destination.write_cdc_events(batch.clone()).await {
            Ok(new_lsn) => {
                // Success! Update LSN tracker
                let mut lsn_map = lsn_tracker.lock().await;
                lsn_map.insert(destination_id.to_string(), new_lsn);
                return Ok(());
            }
            Err(e) if e.to_string().contains("schema change detected:") => {
                return Err(e);
            }
            Err(e) => {
                error!(
                    "Destination {} write failed (attempt {}): {:?}",
                    destination_id, attempt, e
                );

                // Check if we've exceeded total retry time
                if start_time.elapsed() >= MAX_TOTAL_TIME {
                    error!(
                        "Destination {} exceeded maximum retry time of {} seconds",
                        destination_id,
                        MAX_TOTAL_TIME.as_secs()
                    );
                    // Return the last error instead of a generic timeout message
                    return Err(e);
                }

                info!(
                    "Retrying destination {} in {:?} (attempt {})",
                    destination_id,
                    current_delay,
                    attempt + 1
                );

                // Wait with exponential backoff
                tokio::time::sleep(current_delay).await;

                // Update delay for next iteration (exponential backoff, capped at max)
                current_delay = std::cmp::min(current_delay * 2, MAX_DELAY);
                attempt += 1;
            }
        }
    }
}
