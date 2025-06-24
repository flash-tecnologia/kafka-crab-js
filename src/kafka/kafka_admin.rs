use rdkafka::{
  admin::{
    AdminClient, AdminOptions, ConfigResource, NewTopic, ResourceSpecifier, TopicReplication,
  },
  client::DefaultClientContext,
  config::ClientConfig,
  consumer::{BaseConsumer, Consumer},
  error::KafkaError,
  types::RDKafkaErrorCode,
};
use tracing::{debug, trace};

use std::{collections::HashMap, str::FromStr, time::Duration};

const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

const DEFAULT_NUM_PARTITIONS: i32 = 3;
const DEFAULT_REPLICATION: i32 = 3;

pub struct KafkaAdmin<'a> {
  client_config: &'a ClientConfig,
  admin_client: AdminClient<DefaultClientContext>,
  fetch_metadata_timeout: Duration,
}

impl<'a> KafkaAdmin<'a> {
  pub fn new(
    client_config: &'a ClientConfig,
    fetch_metadata_timeout: Option<Duration>,
  ) -> Result<Self, KafkaError> {
    let admin_client: AdminClient<DefaultClientContext> = client_config.create()?;

    Ok(KafkaAdmin {
      client_config,
      admin_client,
      fetch_metadata_timeout: fetch_metadata_timeout.unwrap_or(DEFAULT_FETCH_METADATA_TIMEOUT),
    })
  }

  async fn fetch_config_resource(&self) -> Result<HashMap<String, String>, KafkaError> {
    let consumer: BaseConsumer = self.client_config.create()?;

    let metadata = consumer.fetch_metadata(None, self.fetch_metadata_timeout)?;

    for broker in metadata.brokers() {
      debug!(
        "Metadata borker id:{}  Host: {}:{}",
        broker.id(),
        broker.host(),
        broker.port()
      );
    }

    let config_resource = self
      .admin_client
      .describe_configs(
        &[ResourceSpecifier::Broker(metadata.orig_broker_id())],
        &AdminOptions::new(),
      )
      .await?;

    Ok(extract_config_resource(config_resource))
  }

  pub async fn create_topic(&self, topics: &Vec<String>) -> anyhow::Result<()> {
    let broker_properties = self.fetch_config_resource().await?.clone();
    trace!("Broker properties: {:?}", broker_properties);

    let new_topics: Vec<NewTopic> = topics
      .iter()
      .map(|topic| NewTopic {
        name: topic,
        num_partitions: broker_properties
          .get("num.partitions")
          .get_parsed_or_default_value(DEFAULT_NUM_PARTITIONS),
        replication: TopicReplication::Fixed(
          broker_properties
            .get("default.replication.factor")
            .get_parsed_or_default_value(DEFAULT_REPLICATION),
        ),
        config: vec![],
      })
      .collect();

    self
      .admin_client
      .create_topics(&new_topics, &AdminOptions::default())
      .await
      .map_err(anyhow::Error::new)?;

    debug!("Topic(s) {:?} was created successfully", topics);
    Ok(())
  }
}

fn extract_config_resource(
  config_resource: Vec<Result<ConfigResource, RDKafkaErrorCode>>,
) -> HashMap<String, String> {
  let mut properties: HashMap<String, String> = HashMap::new();
  for config_resource_list in config_resource {
    match config_resource_list {
      Ok(v) => {
        for config_entry_list in v.entries {
          if let Some(value) = config_entry_list.value {
            properties.insert(config_entry_list.name, value);
          }
        }
      }
      Err(e) => {
        debug!("Error on fetching config entry {:?}", e)
      }
    }
  }

  properties
}

trait ParsedOrDefaultValue {
  fn get_parsed_or_default_value<T: FromStr>(self, default_value: T) -> T;
}

impl ParsedOrDefaultValue for Option<&String> {
  fn get_parsed_or_default_value<T: FromStr>(self, default_value: T) -> T {
    match self {
      Some(ret) => ret.parse().unwrap_or(default_value),
      None => default_value,
    }
  }
}
