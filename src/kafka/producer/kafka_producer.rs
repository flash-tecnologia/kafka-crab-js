use std::{
  collections::{HashMap, HashSet},
  sync::{Arc, Mutex},
  time::Duration,
};

use nanoid::nanoid;
use napi::{Error, Result, Status};
use rdkafka::{
  error::KafkaError,
  message::{OwnedHeaders, OwnedMessage},
  producer::{BaseRecord, DeliveryResult, NoCustomPartitioner, Partitioner, ThreadedProducer},
  ClientConfig, ClientContext, Message, Statistics,
};
use rdkafka::{
  message::ToBytes,
  producer::{Producer, ProducerContext},
};
use tracing::{debug, info};

use crate::kafka::kafka_util::hashmap_to_kafka_headers;

use super::model::{
  KafkaCrabError, MessageProducer, ProducerConfiguration, ProducerRecord, RecordMetadata,
};

const DEFAULT_QUEUE_TIMEOUT: i64 = 5000;

type ProducerDeliveryResult = (OwnedMessage, Option<KafkaError>, Arc<String>);

#[derive(Clone)]
struct CollectingContext<Part: Partitioner = NoCustomPartitioner> {
  results: Arc<Mutex<HashMap<String, ProducerDeliveryResult>>>,
  partitioner: Option<Part>,
}

impl CollectingContext {
  fn new() -> CollectingContext {
    CollectingContext {
      results: Arc::new(Mutex::new(HashMap::new())),
      partitioner: None,
    }
  }
}

impl<Part: Partitioner + Send + Sync> ClientContext for CollectingContext<Part> {
  fn stats(&self, stats: Statistics) {
    debug!("Stats: {:?}", stats);
  }
}

impl<Part: Partitioner + Send + Sync> ProducerContext<Part> for CollectingContext<Part> {
  type DeliveryOpaque = Arc<String>;

  fn delivery(&self, delivery_result: &DeliveryResult, delivery_opaque: Self::DeliveryOpaque) {
    let mut results = self.results.lock().unwrap();
    let (message, err) = match *delivery_result {
      Ok(ref message) => (message.detach(), None),
      Err((ref err, ref message)) => (message.detach(), Some(err.clone())),
    };
    results.insert((*delivery_opaque).clone(), (message, err, delivery_opaque));
  }

  fn get_custom_partitioner(&self) -> Option<&Part> {
    self.partitioner.as_ref()
  }
}

fn threaded_producer_with_context<Part, C>(
  context: C,
  client_config: ClientConfig,
) -> ThreadedProducer<C, Part>
where
  Part: Partitioner + Send + Sync + 'static,
  C: ProducerContext<Part>,
{
  client_config
    .create_with_context::<C, ThreadedProducer<_, _>>(context)
    .unwrap()
}

#[napi]
pub struct KafkaProducer {
  queue_timeout: Duration,
  auto_flush: bool,
  context: CollectingContext,
  producer: ThreadedProducer<CollectingContext>,
}

#[napi]
impl KafkaProducer {
  pub fn new(
    client_config: ClientConfig,
    producer_configuration: ProducerConfiguration,
  ) -> Result<Self> {
    let mut producer_config = client_config;

    if let Some(config) = producer_configuration.configuration {
      producer_config.extend(config);
    }

    let queue_timeout = Duration::from_millis(
      producer_configuration
        .queue_timeout
        .unwrap_or(DEFAULT_QUEUE_TIMEOUT)
        .try_into()
        .map_err(|e| Error::new(Status::GenericFailure, e))?,
    );

    let auto_flush = producer_configuration.auto_flush.unwrap_or(true);

    if !auto_flush {
      info!("Auto flush is disabled. You must call flush() manually.");
    }

    let context = CollectingContext::new();
    let producer: ThreadedProducer<CollectingContext> =
      threaded_producer_with_context(context.clone(), producer_config);

    Ok(KafkaProducer {
      queue_timeout,
      auto_flush,
      context,
      producer,
    })
  }

  #[napi]
  pub fn in_flight_count(&self) -> Result<i32> {
    Ok(self.producer.in_flight_count())
  }

  #[napi]
  pub async fn flush(&self) -> Result<Vec<RecordMetadata>> {
    if self.auto_flush {
      Ok(vec![])
    } else {
      self.flush_delivery_results()
    }
  }

  #[napi]
  pub async fn send(&self, producer_record: ProducerRecord) -> Result<Vec<RecordMetadata>> {
    let topic = producer_record.topic.as_str();

    let ids: HashSet<String> = producer_record
      .messages
      .iter()
      .map(|_| nanoid!(14))
      .collect();

    for (message, record_id) in producer_record.messages.into_iter().zip(ids.iter()) {
      self.send_single_message(topic, &message, record_id)?;
    }

    if self.auto_flush {
      self.flush_delivery_results_with_filter(&ids)
    } else {
      Ok(vec![])
    }
  }

  fn send_single_message(
    &self,
    topic: &str,
    message: &MessageProducer,
    record_id: &str,
  ) -> Result<()> {
    let headers = message
      .headers
      .as_ref()
      .map_or_else(OwnedHeaders::new, hashmap_to_kafka_headers);

    let key = message
      .key
      .as_deref()
      .map(ToBytes::to_bytes)
      .unwrap_or_default();

    let opaque = Arc::new(record_id.to_string());
    let record: BaseRecord<'_, [u8], [u8], Arc<String>> = BaseRecord::with_opaque_to(topic, opaque)
      .payload(message.payload.to_bytes())
      .headers(headers)
      .key(key);

    self
      .producer
      .send(record)
      .map_err(|(e, _)| Error::new(Status::GenericFailure, e.to_string()))?;

    Ok(())
  }

  fn flush_delivery_results(&self) -> Result<Vec<RecordMetadata>> {
    self
      .producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let mut delivery_results = self.context.results.lock().unwrap();
    let result: Vec<RecordMetadata> = delivery_results
      .iter()
      .map(|(_, (message, error, _))| to_record_metadata(message, error))
      .collect();
    delivery_results.clear();
    Ok(result)
  }

  fn flush_delivery_results_with_filter(
    &self,
    ids: &HashSet<String>,
  ) -> Result<Vec<RecordMetadata>> {
    self
      .producer
      .flush(self.queue_timeout)
      .map_err(|e| Error::new(Status::GenericFailure, e))?;

    let mut delivery_results = self.context.results.lock().unwrap();
    let result = delivery_results
      .iter()
      .filter(|(id, _)| ids.contains(*id))
      .map(|(_, (message, error, _))| to_record_metadata(message, error))
      .collect();
    delivery_results.retain(|key, _| !ids.contains(key));
    Ok(result)
  }
}

fn to_record_metadata(message: &OwnedMessage, error: &Option<KafkaError>) -> RecordMetadata {
  RecordMetadata {
    topic: message.topic().to_string(),
    partition: message.partition(),
    offset: message.offset(),
    error: error.as_ref().map(|err| KafkaCrabError {
      code: err
        .rdkafka_error_code()
        .unwrap_or(rdkafka::types::RDKafkaErrorCode::Unknown) as i32,
      message: err.to_string(),
    }),
  }
}
