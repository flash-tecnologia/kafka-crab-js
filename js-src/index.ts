export type {
  ConsumerConfiguration,
  KafkaConfiguration,
  KafkaCrabError,
  KafkaEvent,
  KafkaEventPayload,
  Message,
  MessageProducer,
  OffsetModel,
  PartitionOffset,
  ProducerConfiguration,
  ProducerRecord,
  RecordMetadata,
  TopicPartition,
  TopicPartitionConfig,
} from '../js-binding.js'

export {
  CommitMode,
  KafkaClientConfig,
  KafkaConsumer,
  KafkaEventName,
  KafkaProducer,
  PartitionPosition,
  SecurityProtocol,
} from '../js-binding.js'
export { KafkaClient } from './kafka-client.js'
export { BaseKafkaStreamReadable } from './streams/base-kafka-stream-readable.js'
export { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
export { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

export type { KafkaClientConfiguration, StreamConsumerConfiguration } from './kafka-client.js'

// OpenTelemetry exports
export type {
  BatchOtelContext,
  InstrumentedMessage,
  InstrumentedProducerRecord,
  KafkaOtelContext,
  KafkaOtelInstrumentationConfig,
  MessageHookFn,
  ProducerHookFn,
  TopicFilterFn,
} from './otel/types.js'

export { getKafkaInstrumentation, KafkaCrabInstrumentation, resetKafkaInstrumentation } from './otel/instrumentation.js'

export { KAFKA_OPERATION_TYPES, KAFKA_SEMANTIC_CONVENTIONS, PACKAGE_INFO } from './otel/constants.js'
