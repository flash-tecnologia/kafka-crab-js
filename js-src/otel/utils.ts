import {
  type Attributes,
  type Context,
  context,
  propagation,
  type Span,
  SpanKind,
  SpanStatusCode,
  trace,
  type Tracer,
} from '@opentelemetry/api'
import type { Message, ProducerRecord } from '../../js-binding.js'
import { KAFKA_DEFAULTS, KAFKA_SEMANTIC_CONVENTIONS } from './constants.js'

// Safely get tracer
export function getTracer(name: string, version?: string) {
  return trace.getTracer(name, version)
}

// Set span status based on error
export function setSpanStatus(span: Span, error?: Error): void {
  if (error) {
    span.setStatus({
      code: SpanStatusCode.ERROR,
      message: error.message,
    })
    span.recordException(error)
  } else {
    span.setStatus({ code: SpanStatusCode.OK })
  }
}

// Extract common Kafka attributes from message
export function getMessageAttributes(message: Message, operation: string): Attributes {
  const attributes: Attributes = {
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: message.topic,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_KIND]: KAFKA_DEFAULTS.DESTINATION_KIND,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operation,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_PARTITION]: message.partition,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_OFFSET]: message.offset,
  }

  // Add optional attributes if present
  if (message.key !== null && message.key !== undefined) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY] = typeof message.key === 'string'
      ? message.key
      : String(message.key)
  }

  if (message.payload) {
    let payloadSize: number
    if (Buffer.isBuffer(message.payload)) {
      payloadSize = message.payload.length
    } else if (typeof message.payload === 'string') {
      payloadSize = Buffer.byteLength(message.payload, 'utf8')
    } else {
      payloadSize = JSON.stringify(message.payload).length
    }
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE] = payloadSize
  }

  return attributes
}

// Extract common Kafka attributes from producer record
export function getProducerRecordAttributes(record: ProducerRecord, operation: string): Attributes {
  const attributes: Attributes = {
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME]: record.topic,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_KIND]: KAFKA_DEFAULTS.DESTINATION_KIND,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operation,
  }

  // Add message count
  if (record.messages && record.messages.length > 0) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT] = record.messages.length

    // Use first message for key and payload size
    const [firstMessage] = record.messages

    // Add key if present
    if (firstMessage.key !== null && firstMessage.key !== undefined) {
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_KAFKA_MESSAGE_KEY] = Buffer.isBuffer(firstMessage.key)
        ? firstMessage.key.toString()
        : String(firstMessage.key)
    }

    // Add payload size if present
    if (firstMessage.payload) {
      let payloadSize: number
      if (Buffer.isBuffer(firstMessage.payload)) {
        payloadSize = firstMessage.payload.length
      } else if (typeof firstMessage.payload === 'string') {
        payloadSize = Buffer.byteLength(firstMessage.payload, 'utf8')
      } else {
        payloadSize = JSON.stringify(firstMessage.payload).length
      }
      attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_MESSAGE_BODY_SIZE] = payloadSize
    }
  }

  return attributes
}

// Convert Buffer headers to string headers for trace context injection
function convertBufferHeaders(
  bufferHeaders: Record<string, Buffer> = {},
): Record<string, string | string[] | undefined> {
  const stringHeaders: Record<string, string | string[] | undefined> = {}
  for (const [key, value] of Object.entries(bufferHeaders)) {
    stringHeaders[key] = value.toString('utf8')
  }
  return stringHeaders
}

// Convert string headers back to Buffer headers
function convertToBufferHeaders(stringHeaders: Record<string, string | string[] | undefined>): Record<string, Buffer> {
  const bufferHeaders: Record<string, Buffer> = {}
  for (const [key, value] of Object.entries(stringHeaders)) {
    if (value !== undefined) {
      const stringValue = Array.isArray(value) ? value.join(',') : value
      bufferHeaders[key] = Buffer.from(stringValue, 'utf8')
    }
  }
  return bufferHeaders
}

export type KafkaHeaderValue = Buffer | string | string[] | undefined
export type KafkaHeaderCarrier = Record<string, KafkaHeaderValue>

// Inject trace context into Kafka headers (works with both string and Buffer headers)
export function injectTraceContext(
  headers: KafkaHeaderCarrier = {},
  ctx?: Context,
): KafkaHeaderCarrier {
  const activeContext = ctx || context.active()
  const headerEntries = Object.entries(headers)
  const hasBufferValues = headerEntries.some(([, value]) => Buffer.isBuffer(value))

  // Convert Buffer headers to string headers if needed
  if (hasBufferValues) {
    const bufferCarrier = headers as Record<string, Buffer>
    const stringHeaders = convertBufferHeaders(bufferCarrier)

    propagation.inject(activeContext, stringHeaders, {
      set: (carrier: Record<string, string | string[] | undefined>, key: string, value: string) => {
        carrier[key] = value
      },
    })

    const bufferHeaders = convertToBufferHeaders(stringHeaders)

    for (const [key, value] of Object.entries(bufferHeaders)) {
      bufferCarrier[key] = value
    }

    return bufferCarrier
  }

  const stringCarrier = headers as Record<string, string | string[] | undefined>

  propagation.inject(activeContext, stringCarrier, {
    set: (carrier: Record<string, string | string[] | undefined>, key: string, value: string) => {
      carrier[key] = value
    },
  })

  // Preserve original string-based carrier for callers expecting strings
  return stringCarrier
}

// Normalize any header carrier into Buffer headers for Kafka bindings
export function normalizeHeadersToBuffer(headers: KafkaHeaderCarrier): Record<string, Buffer> {
  const bufferHeaders: Record<string, Buffer> = {}

  for (const [key, value] of Object.entries(headers)) {
    if (value !== undefined) {
      if (Buffer.isBuffer(value)) {
        bufferHeaders[key] = value
      } else if (Array.isArray(value)) {
        bufferHeaders[key] = Buffer.from(value.join(','), 'utf8')
      } else {
        bufferHeaders[key] = Buffer.from(value, 'utf8')
      }
    }
  }

  return bufferHeaders
}

// Extract trace context from Kafka headers
export function extractTraceContext(
  headers: Record<string, string | string[] | undefined> = {},
): Context {
  return propagation.extract(context.active(), headers, {
    get: (carrier: Record<string, string | string[] | undefined>, key: string) => {
      const value = carrier[key]
      const singleValue = Array.isArray(value) ? value[0] : value

      if (Buffer.isBuffer(singleValue)) {
        return singleValue.toString('utf8')
      }

      return singleValue
    },
    keys: (carrier: Record<string, string | string[] | undefined>) => Object.keys(carrier),
  })
}

// Check if topic should be ignored based on filter configuration
export function shouldIgnoreTopic(
  topic: string,
  ignoreTopics?: string[] | ((topic: string) => boolean),
): boolean {
  if (!ignoreTopics) {
    return false
  }

  if (Array.isArray(ignoreTopics)) {
    return ignoreTopics.includes(topic)
  }

  if (typeof ignoreTopics === 'function') {
    try {
      return ignoreTopics(topic)
    } catch {
      // If filter function throws, don't ignore the topic
      return false
    }
  }

  return false
}

// Create span for producer operation
export function createProducerSpan(
  tracer: Tracer,
  record: ProducerRecord,
  operation = 'send',
  parentContext?: Context,
): Span | null {
  if (!tracer) {
    return null
  }
  const parent = parentContext ?? context.active()

  const spanName = `${record.topic} ${operation}`
  const attributes = getProducerRecordAttributes(record, operation)

  const spanOptions = {
    kind: SpanKind.PRODUCER,
    attributes,
  }

  return tracer.startSpan(spanName, spanOptions, parent)
}

// Create span for consumer operation
export function createConsumerSpan(
  tracer: Tracer,
  message: Message,
  operation = 'process',
  parentContext?: Context,
): Span | null {
  if (!tracer) {
    return null
  }

  const spanName = `${message.topic} ${operation}`
  const attributes = getMessageAttributes(message, operation)

  const spanOptions = {
    kind: SpanKind.CONSUMER,
    attributes,
  }

  // Start span with parent context if provided
  if (parentContext) {
    return tracer.startSpan(spanName, spanOptions, parentContext)
  }

  return tracer.startSpan(spanName, spanOptions)
}

// Create batch span for batch operations
export function createBatchSpan(
  tracer: Tracer,
  batchSize: number,
  topic?: string,
  operation = 'batch_process',
  parentContext?: Context,
): Span | null {
  if (!tracer) {
    return null
  }

  const spanName = topic ? `${topic} ${operation}` : `kafka ${operation}`
  const attributes: Attributes = {
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_SYSTEM]: KAFKA_DEFAULTS.MESSAGING_SYSTEM,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_OPERATION_NAME]: operation,
    [KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_BATCH_MESSAGE_COUNT]: batchSize,
  }

  if (topic) {
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_NAME] = topic
    attributes[KAFKA_SEMANTIC_CONVENTIONS.MESSAGING_DESTINATION_KIND] = KAFKA_DEFAULTS.DESTINATION_KIND
  }

  const spanOptions = {
    kind: SpanKind.CONSUMER,
    attributes,
  }

  if (parentContext) {
    return tracer.startSpan(spanName, spanOptions, parentContext)
  }

  return tracer.startSpan(spanName, spanOptions)
}
