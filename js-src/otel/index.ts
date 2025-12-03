// Main OTEL module entry point
export type {
  BatchOtelContext,
  InstrumentedMessage,
  InstrumentedProducerRecord,
  KafkaOtelContext,
  KafkaOtelInstrumentationConfig,
  MessageHookFn,
  ProducerHookFn,
  TopicFilterFn,
} from './types.js'

export { getKafkaInstrumentation, KafkaCrabInstrumentation, resetKafkaInstrumentation } from './instrumentation.js'

export {
  KAFKA_DEFAULTS,
  KAFKA_OPERATION_TYPES,
  KAFKA_SEMANTIC_CONVENTIONS,
  KAFKA_SPAN_NAMES,
  PACKAGE_INFO,
} from './constants.js'

export {
  createBatchSpan,
  createConsumerSpan,
  createProducerSpan,
  extractTraceContext,
  getTracer,
  injectTraceContext,
  setSpanStatus,
  shouldIgnoreTopic,
} from './utils.js'
