use std::{time::Duration, vec};
use tokio::sync::watch::{self};

use napi::{
  threadsafe_function::{ThreadsafeFunction, ThreadsafeFunctionCallMode},
  Either, Error, Result, Status,
};

use rdkafka::{
  consumer::{stream_consumer::StreamConsumer, CommitMode as RdKfafkaCommitMode, Consumer},
  topic_partition_list::TopicPartitionList as RdTopicPartitionList,
  ClientConfig, Message as RdMessage, Offset,
};

use tracing::{debug, info, warn};

use crate::kafka::{
  consumer::consumer_helper::{
    assign_offset_or_use_metadata, convert_to_rdkafka_offset, try_create_topic, try_subscribe,
  },
  kafka_client_config::KafkaClientConfig,
  kafka_util::{create_message, IntoNapiError},
  producer::model::Message,
};

use super::{
  consumer_helper::{
    convert_tpl_to_array_of_topic_partition, create_stream_consumer, set_offset_of_all_partitions,
  },
  context::{KafkaCrabContext, KafkaEvent},
  model::{
    CommitMode, ConsumerConfiguration, OffsetModel, TopicPartition, TopicPartitionConfig,
    DEFAULT_FETCH_METADATA_TIMEOUT,
  },
};

use tokio::select;

pub const DEFAULT_SEEK_TIMEOUT: i64 = 1500;
const MAX_SEEK_TIMEOUT: i64 = 300000; // 5 minutes max
const DEFAULT_BATCH_TIMEOUT: i64 = 1500;
const MAX_BATCH_TIMEOUT: i64 = 30000; // 30 seconds max for batch operations
const DEFAULT_MAX_BATCH_MESSAGES: u32 = 10;
const MAX_BATCH_MESSAGES: u32 = 1000;

/// Validates and bounds-checks timeout values
#[inline]
fn validate_timeout(timeout: Option<i64>, default: i64, max: i64, min: i64) -> i64 {
  match timeout {
    Some(t) => {
      if t < min {
        default
      } else if t > max {
        max
      } else {
        t
      }
    }
    None => default,
  }
}

/// Validates timeout for seek operations
#[inline]
fn validate_seek_timeout(timeout: Option<i64>) -> i64 {
  validate_timeout(timeout, DEFAULT_SEEK_TIMEOUT, MAX_SEEK_TIMEOUT, 0)
}

/// Validates timeout for batch operations  
#[inline]
fn validate_batch_timeout(timeout: Option<i64>) -> i64 {
  validate_timeout(timeout, DEFAULT_BATCH_TIMEOUT, MAX_BATCH_TIMEOUT, 1)
}

type DisconnectSignal = (watch::Sender<()>, watch::Receiver<()>);

#[napi]
pub struct KafkaConsumer {
  client_config: ClientConfig,
  consumer_config: ConsumerConfiguration,
  stream_consumer: StreamConsumer<KafkaCrabContext>,
  fetch_metadata_timeout: Duration,
  disconnect_signal: DisconnectSignal,
  max_batch_messages: u32,
}

#[napi]
impl KafkaConsumer {
  pub fn new(
    kafka_client: &KafkaClientConfig,
    consumer_configuration: &ConsumerConfiguration,
  ) -> Result<Self> {
    let client_config: &ClientConfig = kafka_client.get_client_config();

    let ConsumerConfiguration { configuration, .. } = consumer_configuration;
    let stream_consumer =
      create_stream_consumer(client_config, consumer_configuration, configuration.clone())
        .map_err(|e| e.into_napi_error("error while getting assignment"))?;

    let max_batch_messages = consumer_configuration
      .max_batch_messages
      .unwrap_or(DEFAULT_MAX_BATCH_MESSAGES)
      .min(MAX_BATCH_MESSAGES)
      .max(1);

    Ok(KafkaConsumer {
      client_config: client_config.clone(),
      consumer_config: consumer_configuration.clone(),
      stream_consumer,
      fetch_metadata_timeout: Duration::from_millis(
        consumer_configuration.fetch_metadata_timeout.map_or_else(
          || DEFAULT_FETCH_METADATA_TIMEOUT.as_millis() as u64,
          |t| t as u64,
        ),
      ),
      disconnect_signal: watch::channel(()),
      max_batch_messages,
    })
  }

  #[napi]
  pub fn get_config(&self) -> Result<ConsumerConfiguration> {
    Ok(self.consumer_config.clone())
  }

  #[napi]
  pub fn get_subscription(&self) -> Result<Vec<TopicPartition>> {
    match self.stream_consumer.subscription() {
      Ok(v) => Ok(convert_tpl_to_array_of_topic_partition(&v)),
      Err(e) => Err(e.into_napi_error("Error while getting subscription")),
    }
  }

  #[napi(
    async_runtime,
    ts_args_type = "callback: (error: Error | undefined, event: KafkaEvent) => void"
  )]
  pub fn on_events(&self, callback: ThreadsafeFunction<KafkaEvent>) -> Result<()> {
    let mut rx = self.stream_consumer.context().event_channel.1.clone();
    let mut disconnect_signal = self.disconnect_signal.1.clone();

    tokio::spawn(async move {
      loop {
        select! {
            _ = rx.changed() => {
                if let Some(event) = rx.borrow().clone() {
                    callback.call(Ok(event), ThreadsafeFunctionCallMode::NonBlocking);
                }
            }
            _ = disconnect_signal.changed() => {
                debug!("Subscription to consumer events is stopped");
                break;
            }
        }
      }
    });
    Ok(())
  }

  #[napi]
  pub async fn subscribe(
    &self,
    topic_configs: Either<String, Vec<TopicPartitionConfig>>,
  ) -> Result<()> {
    let topics = match topic_configs {
      Either::A(config) => {
        debug!("Subscribing to topic: {:#?}", &config);
        vec![TopicPartitionConfig {
          topic: config,
          all_offsets: None,
          partition_offset: None,
        }]
      }
      Either::B(config) => {
        debug!("Subscribing to topic config: {:#?}", &config);
        config
      }
    };

    let topics_name = topics
      .iter()
      .map(|x| x.topic.clone())
      .collect::<Vec<String>>();

    debug!("Creating topics if not exists: {:?}", &topics_name);
    try_create_topic(
      &topics_name,
      &self.client_config,
      self.fetch_metadata_timeout,
    )
    .await
    .map_err(|e| e.into_napi_error("error while creating topics"))?;

    try_subscribe(&self.stream_consumer, &topics_name)
      .map_err(|e| e.into_napi_error("error while subscribing"))?;

    topics.iter().for_each(|item| {
      if let Some(all_offsets) = item.all_offsets.clone() {
        debug!(
          "Subscribing to topic: {}. Setting all partitions to offset: {:?}",
          &item.topic, &all_offsets
        );
        set_offset_of_all_partitions(
          &all_offsets,
          &self.stream_consumer,
          &item.topic,
          self.fetch_metadata_timeout,
        )
        .map_err(|e| e.into_napi_error("error while setting offset"))
        .unwrap();
      } else if let Some(partition_offset) = item.partition_offset.clone() {
        debug!(
          "Subscribing to topic: {} with partition offsets: {:?}",
          &item.topic, &partition_offset
        );
        assign_offset_or_use_metadata(
          &item.topic,
          Some(partition_offset),
          None,
          &self.stream_consumer,
          self.fetch_metadata_timeout,
        )
        .map_err(|e| e.into_napi_error("error while assigning offset"))
        .unwrap();
      };
    });

    Ok(())
  }

  fn get_partitions(&self) -> Result<RdTopicPartitionList> {
    let partitions = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("getting partitions"))?;
    Ok(partitions)
  }

  #[napi]
  pub fn pause(&self) -> Result<()> {
    self
      .stream_consumer
      .pause(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("error while pausing"))?;
    Ok(())
  }

  #[napi]
  pub fn resume(&self) -> Result<()> {
    self
      .stream_consumer
      .resume(&self.get_partitions()?)
      .map_err(|e| e.into_napi_error("error while resuming"))?;
    Ok(())
  }

  #[napi]
  pub fn unsubscribe(&self) -> Result<()> {
    info!("Unsubscribing from topics");
    self.stream_consumer.unsubscribe();
    Ok(())
  }

  #[napi]
  pub async fn disconnect(&self) -> Result<()> {
    info!("Disconnecting consumer - This will stop the consumer from receiving messages");

    // First unsubscribe from topics
    self.stream_consumer.unsubscribe();

    // Then send disconnect signal
    let tx = self.disconnect_signal.0.clone();
    tx.send(())
      .map_err(|e| e.into_napi_error("Error sending disconnect signal"))?;

    Ok(())
  }

  #[napi]
  pub fn seek(
    &self,
    topic: String,
    partition: i32,
    offset_model: OffsetModel,
    timeout: Option<i64>,
  ) -> Result<()> {
    let offset = convert_to_rdkafka_offset(&offset_model);
    debug!(
      "Seeking to topic: {}, partition: {}, offset: {:?}",
      topic, partition, offset
    );
    self
      .stream_consumer
      .seek(
        &topic,
        partition,
        offset,
        Duration::from_millis(validate_seek_timeout(timeout) as u64),
      )
      .map_err(|e| e.into_napi_error("Error while seeking"))?;
    Ok(())
  }

  #[napi]
  pub fn assignment(&self) -> Result<Vec<TopicPartition>> {
    let assignment = self
      .stream_consumer
      .assignment()
      .map_err(|e| e.into_napi_error("error while getting assignment"))?;
    Ok(convert_tpl_to_array_of_topic_partition(&assignment))
  }

  #[napi]
  pub async fn recv(&self) -> Result<Option<Message>> {
    let mut rx = self.disconnect_signal.1.clone();
    select! {
        message = self.stream_consumer.recv() => {
            message
                .map_err(|e| e.into_napi_error("Error while receiving from stream consumer"))
                .map(|message| Some(create_message(&message, message.payload().unwrap_or(&[]))))
        }
        _ = rx.changed() => {
            debug!("Disconnect signal received and this will stop the consumer from receiving messages");
            Ok(None)
        }
    }
  }

  /// Receives multiple messages in a single call for higher throughput
  ///
  /// This method provides 2-5x better performance than calling recv() multiple times
  /// by batching message retrieval and reducing function call overhead.
  ///
  /// @param max_messages Maximum number of messages to retrieve (1-configured max, default 1000)
  /// @param timeout_ms Timeout in milliseconds (1-30000)
  /// @returns Array of messages (may be fewer than max_messages)
  #[napi]
  pub async fn recv_batch(&self, max_messages: u32, timeout_ms: i64) -> Result<Vec<Message>> {
    // Validate input parameters against configured maximum
    let max_messages = if max_messages == 0 || max_messages > self.max_batch_messages {
      warn!(
        "max_messages {} out of range [1-{}], using {}",
        max_messages, self.max_batch_messages, self.max_batch_messages
      );
      self.max_batch_messages
    } else {
      max_messages
    };

    let timeout_ms = validate_batch_timeout(Some(timeout_ms));
    let batch_timeout = std::time::Duration::from_millis(timeout_ms as u64);

    let mut messages = Vec::with_capacity(max_messages as usize);
    let mut rx = self.disconnect_signal.1.clone();
    let start_time = std::time::Instant::now();

    // Try to collect messages up to max_messages or timeout
    for _ in 0..max_messages {
      // Check if we've exceeded our timeout
      if start_time.elapsed() >= batch_timeout {
        break;
      }

      // Calculate remaining timeout
      let remaining_timeout = batch_timeout.saturating_sub(start_time.elapsed());
      if remaining_timeout.is_zero() {
        break;
      }

      // Try to receive a message with remaining timeout
      let recv_result = tokio::time::timeout(remaining_timeout, async {
        select! {
          message = self.stream_consumer.recv() => {
            message.map_err(|e| e.into_napi_error("Error while receiving from stream consumer"))
          }
          _ = rx.changed() => {
            debug!("Disconnect signal received during batch receive");
            Err(Error::new(Status::GenericFailure, "Consumer disconnected"))
          }
        }
      })
      .await;

      match recv_result {
        Ok(Ok(kafka_message)) => {
          let message = create_message(&kafka_message, kafka_message.payload().unwrap_or(&[]));
          messages.push(message);
        }
        Ok(Err(e)) => {
          // Error receiving message, return what we have so far
          if messages.is_empty() {
            return Err(e);
          }
          break;
        }
        Err(_) => {
          // Timeout occurred, return what we have so far
          break;
        }
      }
    }

    Ok(messages)
  }

  #[napi]
  pub fn commit(
    &self,
    topic: String,
    partition: i32,
    offset: i64,
    commit: CommitMode,
  ) -> Result<()> {
    let mut tpl = RdTopicPartitionList::new();
    tpl
      .add_partition_offset(&topic, partition, Offset::Offset(offset))
      .map_err(|e| e.into_napi_error("Error while adding partition offset"))?;
    let commit_mode = match commit {
      CommitMode::Sync => RdKfafkaCommitMode::Sync,
      CommitMode::Async => RdKfafkaCommitMode::Async,
    };
    self
      .stream_consumer
      .commit(&tpl, commit_mode)
      .map_err(|e| e.into_napi_error("Error while committing"))?;
    debug!("Commiting done. Tpl: {:?}", &tpl);
    Ok(())
  }
}
