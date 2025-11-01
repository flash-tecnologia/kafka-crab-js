import type { Attributes, Context, Span, Tracer } from '@opentelemetry/api'
import type { InstrumentationConfig } from '@opentelemetry/instrumentation'
import type { Message, ProducerRecord, RecordMetadata } from '../../js-binding.js'

// OpenTelemetry TracerProvider interface
export interface TracerProvider {
  getTracer(name: string, version?: string, options?: unknown): Tracer
}

// Use the actual types from js-binding instead of custom interfaces
// This avoids type compatibility issues with the real implementations

// Configuration interface for Kafka OTEL instrumentation
export interface KafkaOtelInstrumentationConfig extends InstrumentationConfig {
  // Service name for telemetry (defaults to OTEL_SERVICE_NAME env var or 'kafka-client')
  serviceName?: string

  // Whether to register instrumentation automatically on initialization
  registerOnInitialization?: boolean

  // Function to filter topics from instrumentation
  ignoreTopics?: string[] | ((topic: string) => boolean)

  // Custom hook called for each message/span
  messageHook?: (span: Span, message: Message) => void

  // Custom hook called for producer operations
  producerHook?: (span: Span, record: ProducerRecord, metadata?: RecordMetadata) => void

  // Whether to capture message payloads as span attributes (security sensitive)
  captureMessagePayload?: boolean

  // Maximum size of message payload to capture (in bytes)
  maxPayloadSize?: number

  // Whether to capture message headers as span attributes
  captureMessageHeaders?: boolean

  // Whether to enable batch operation instrumentation
  enableBatchInstrumentation?: boolean
}

// OpenTelemetry context interface exposed by clients
export interface KafkaOtelContext {
  // Whether OTEL is enabled for this client
  enabled: boolean

  // Current active span (if any)
  span: Span | null

  // Tracer instance for creating spans
  tracer: Tracer

  // Current OTEL context
  context: Context | null

  // Inject trace context into carrier (e.g., Kafka headers)
  inject: (carrier: Record<string, string | string[] | undefined>) => void

  // Extract trace context from carrier (e.g., Kafka headers)
  extract: (carrier: Record<string, string | string[] | undefined>) => Context

  // Create a child span with proper context
  startSpan: (name: string, attributes?: Attributes) => Span

  // End a span with proper status handling
  endSpan: (span: Span, error?: Error) => void
}

// Enhanced message interface with OTEL context
export interface InstrumentedMessage extends Message {
  // Extracted OTEL context from message headers
  otelContext?: Context

  // Optional span created for this message
  span?: Span
}

// Enhanced producer record with OTEL context injection
export interface InstrumentedProducerRecord extends ProducerRecord {
  // Headers will be automatically injected with trace context
  headers?: Record<string, string | string[] | undefined>

  // Optional span for tracking this record
  span?: Span
}

// Batch processing context
export interface BatchOtelContext {
  // Parent span for the entire batch
  batchSpan: Span

  // Individual message spans within the batch
  messageSpans: Span[]

  // Batch size for telemetry
  batchSize: number

  // Processing start time
  startTime: number
}

// Hook function signatures
export type MessageHookFn = (span: Span, message: Message) => void
export type ProducerHookFn = (span: Span, record: ProducerRecord, metadata?: RecordMetadata) => void
export type TopicFilterFn = (topic: string) => boolean

// Configuration defaults
export const DEFAULT_OTEL_CONFIG: Required<
  Pick<
    KafkaOtelInstrumentationConfig,
    | 'serviceName'
    | 'registerOnInitialization'
    | 'captureMessagePayload'
    | 'maxPayloadSize'
    | 'captureMessageHeaders'
    | 'enableBatchInstrumentation'
  >
> = {
  serviceName: process.env.OTEL_SERVICE_NAME || 'kafka-client',
  registerOnInitialization: true,
  captureMessagePayload: false,
  maxPayloadSize: 1024,
  captureMessageHeaders: true,
  enableBatchInstrumentation: true,
} as const
