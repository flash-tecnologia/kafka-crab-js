use std::{collections::HashMap, time::Duration};

pub const DEFAULT_FETCH_METADATA_TIMEOUT: Duration = Duration::from_millis(2000);

#[napi(string_enum)]
#[derive(Debug, PartialEq)]
pub enum CommitMode {
  Sync,
  Async,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct ConsumerConfiguration {
  pub group_id: String,
  pub create_topic: Option<bool>,
  pub enable_auto_commit: Option<bool>,
  pub configuration: Option<HashMap<String, String>>,
  pub fetch_metadata_timeout: Option<i64>,
  pub max_batch_messages: Option<u32>,
}

#[napi(string_enum)]
#[derive(Debug, Clone)]
pub enum PartitionPosition {
  Beginning,
  End,
  Stored,
  Invalid,
}
#[napi(object)]
#[derive(Clone, Debug)]
pub struct OffsetModel {
  pub offset: Option<i64>,
  pub position: Option<PartitionPosition>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct PartitionOffset {
  pub partition: i32,
  pub offset: OffsetModel,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartitionConfig {
  pub topic: String,
  pub all_offsets: Option<OffsetModel>,
  pub partition_offset: Option<Vec<PartitionOffset>>,
}

#[napi(object)]
#[derive(Clone, Debug)]
pub struct TopicPartition {
  pub topic: String,
  pub partition_offset: Vec<PartitionOffset>,
}
