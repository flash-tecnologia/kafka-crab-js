// OpenTelemetry semantic conventions for Kafka messaging
export const KAFKA_SEMANTIC_CONVENTIONS = {
  // Messaging system attributes (https://opentelemetry.io/docs/specs/semconv/messaging/messaging-spans/)
  MESSAGING_SYSTEM: 'messaging.system',
  MESSAGING_DESTINATION_NAME: 'messaging.destination.name',
  MESSAGING_DESTINATION_KIND: 'messaging.destination.kind',
  MESSAGING_OPERATION_NAME: 'messaging.operation.name',
  MESSAGING_OPERATION_TYPE: 'messaging.operation.type',
  MESSAGING_CLIENT_ID: 'messaging.client.id',
  MESSAGING_MESSAGE_ID: 'messaging.message.id',
  MESSAGING_MESSAGE_BODY_SIZE: 'messaging.message.body.size',
  MESSAGING_BATCH_MESSAGE_COUNT: 'messaging.batch.message_count',

  // Kafka-specific attributes
  MESSAGING_KAFKA_OFFSET: 'messaging.kafka.offset',
  MESSAGING_KAFKA_PARTITION: 'messaging.kafka.partition',
  MESSAGING_KAFKA_MESSAGE_KEY: 'messaging.kafka.message.key',
  MESSAGING_KAFKA_CONSUMER_GROUP: 'messaging.kafka.consumer.group',
  MESSAGING_KAFKA_TOMBSTONE: 'messaging.kafka.tombstone',

  // Consumer-specific attributes
  MESSAGING_CONSUMER_GROUP_NAME: 'messaging.consumer.group.name',
} as const

// Operation types
export const KAFKA_OPERATION_TYPES = {
  SEND: 'send',
  RECEIVE: 'receive',
  PROCESS: 'process',
  BATCH_RECEIVE: 'batch_receive',
  BATCH_PROCESS: 'batch_process',
} as const

// Span names
export const KAFKA_SPAN_NAMES = {
  PRODUCER_SEND: (topic: string) => `${topic} send`,
  CONSUMER_RECEIVE: (topic: string) => `${topic} receive`,
  CONSUMER_PROCESS: (topic: string) => `${topic} process`,
  BATCH_RECEIVE: 'kafka batch receive',
  BATCH_PROCESS: (topic: string) => `${topic} batch process`,
} as const

// Default values
export const KAFKA_DEFAULTS = {
  MESSAGING_SYSTEM: 'kafka',
  DESTINATION_KIND: 'topic',
  BATCH_TIMEOUT_MS: 1000,
} as const

// Package information
export const PACKAGE_INFO = {
  NAME: 'kafka-crab-js',
  VERSION: '2.0.1', // Should match package.json
} as const
