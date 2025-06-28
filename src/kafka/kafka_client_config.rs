use std::{collections::HashMap, fmt, str::FromStr};

use napi::{bindgen_prelude::*, Result};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};

use tracing::{trace, warn, Level};

use super::{
  consumer::{kafka_consumer::KafkaConsumer, model::ConsumerConfiguration},
  producer::{kafka_producer::KafkaProducer, model::ProducerConfiguration},
};

#[derive(Clone, Debug)]
#[napi(string_enum)]
pub enum SecurityProtocol {
  Plaintext,
  Ssl,
  SaslPlaintext,
  SaslSsl,
}

impl fmt::Display for SecurityProtocol {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match self {
      SecurityProtocol::Plaintext => write!(f, "plaintext"),
      SecurityProtocol::Ssl => write!(f, "ssl"),
      SecurityProtocol::SaslPlaintext => write!(f, "sasl_plaintext"),
      SecurityProtocol::SaslSsl => write!(f, "sasl_ssl"),
    }
  }
}

#[derive(Clone, Debug)]
#[napi(object)]
pub struct KafkaConfiguration {
  pub brokers: String,
  pub client_id: String,
  pub security_protocol: Option<SecurityProtocol>,
  pub configuration: Option<HashMap<String, String>>,
  pub log_level: Option<String>,
  pub broker_address_family: Option<String>,
}

#[derive(Debug)]
#[napi]
pub struct KafkaClientConfig {
  rdkafka_client_config: ClientConfig,
  // #[napi(readonly)]
  kafka_configuration: KafkaConfiguration,
}

#[napi]
impl KafkaClientConfig {
  #[napi(constructor)]
  pub fn new(kafka_configuration: KafkaConfiguration) -> Self {
    let log_level = kafka_configuration.clone().log_level;
    match tracing_subscriber::fmt()
      .with_max_level(
        Level::from_str(log_level.unwrap_or("error".to_owned()).as_str()).unwrap_or(Level::ERROR),
      )
      .json()
      .with_ansi(false)
      .try_init()
    {
      Ok(_) => {
        trace!("Tracing initialized successfully");
      }
      Err(e) => {
        warn!(
          "Tracing initialization failed, but we continue without detailed logging {:?}",
          e
        );
      }
    };
    KafkaClientConfig::with_kafka_configuration(kafka_configuration)
  }

  pub fn get_client_config(&self) -> &ClientConfig {
    &self.rdkafka_client_config
  }

  pub fn with_kafka_configuration(kafka_configuration: KafkaConfiguration) -> Self {
    let KafkaConfiguration {
      brokers,
      security_protocol,
      client_id,
      configuration,
      broker_address_family,
      log_level,
      ..
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    // Set rdkafka log level based on user configuration, defaulting to Error for production safety
    let rdkafka_log_level = match log_level.as_deref().unwrap_or("error") {
      "trace" | "debug" => RDKafkaLogLevel::Debug,
      "info" => RDKafkaLogLevel::Info,
      "warn" => RDKafkaLogLevel::Warning,
      "error" => RDKafkaLogLevel::Error,
      _ => RDKafkaLogLevel::Error, // Safe default for unknown levels
    };
    rdkafka_client_config.set_log_level(rdkafka_log_level);
    rdkafka_client_config.set("bootstrap.servers", brokers);
    rdkafka_client_config.set("client.id", client_id);
    rdkafka_client_config.set(
      "broker.address.family",
      broker_address_family.unwrap_or("v4".to_string()),
    );
    if let Some(security_protocol) = security_protocol {
      rdkafka_client_config.set("security.protocol", security_protocol.to_string());
    }
    if let Some(config) = configuration {
      for (key, value) in config {
        rdkafka_client_config.set(key, value);
      }
    }

    KafkaClientConfig {
      rdkafka_client_config,
      kafka_configuration,
    }
  }

  #[napi(getter)] // Expose this as a property named 'kafkaConfiguration' in JS
  pub fn configuration(&self) -> KafkaConfiguration {
    // Return a clone. Since KafkaConfiguration derives Clone and has
    // #[napi(object)], napi-rs knows how to convert the owned value.
    self.kafka_configuration.clone()
  }

  #[napi]
  pub fn create_producer(
    &self,
    producer_configuration: ProducerConfiguration,
  ) -> Result<KafkaProducer> {
    match KafkaProducer::new(self.rdkafka_client_config.clone(), producer_configuration) {
      Ok(producer) => Ok(producer),
      Err(e) => Err(Error::new(Status::GenericFailure, e.to_string())),
    }
  }

  #[napi(async_runtime)]
  pub fn create_consumer(
    &self,
    consumer_configuration: ConsumerConfiguration,
  ) -> Result<KafkaConsumer> {
    KafkaConsumer::new(self, &consumer_configuration).map_err(|e| {
      Error::new(
        Status::GenericFailure,
        format!("Failed to create stream consumer: {e:?}"),
      )
    })
  }
}
