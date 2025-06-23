use std::{collections::HashMap, fmt, str::FromStr};

use napi::{bindgen_prelude::*, Result};
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};

use tracing::{Level, warn};

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

/// Basic sanitization for configuration values - let rdkafka handle validation
fn sanitize_config_values(config: HashMap<String, String>) -> HashMap<String, String> {
  let mut sanitized_config = HashMap::new();
  
  for (key, value) in config {
    // Only apply basic sanitization for obviously problematic values
    if key.contains("password") || key.contains("secret") || key.contains("key") {
      // Prevent excessively long credential values that might indicate an attack
      if value.len() > 10000 {
        warn!("Configuration value for '{}' is extremely long ({}), truncating for security", key, value.len());
        sanitized_config.insert(key, value.chars().take(10000).collect());
      } else {
        sanitized_config.insert(key, value);
      }
    } else {
      // For non-sensitive values, pass through as-is - let rdkafka validate
      sanitized_config.insert(key, value);
    }
  }
  
  sanitized_config
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
      Ok(_) => {}
      Err(_) => {
        // Tracing initialization failed, but we continue without detailed logging
        // This is a non-critical error that shouldn't expose system details
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
      ..
    } = kafka_configuration.clone();

    let mut rdkafka_client_config = ClientConfig::new();

    rdkafka_client_config.set_log_level(RDKafkaLogLevel::Debug);
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
      let sanitized_config = sanitize_config_values(config);
      for (key, value) in sanitized_config {
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
        format!("Failed to create stream consumer: {:?}", e),
      )
    })
  }
}
