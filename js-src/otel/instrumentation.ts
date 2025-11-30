import { type Attributes, context, diag, type Span, trace, type Tracer } from '@opentelemetry/api'
import { InstrumentationBase, type InstrumentationNodeModuleDefinition } from '@opentelemetry/instrumentation'

import type { KafkaConsumer, KafkaProducer, Message, ProducerRecord, RecordMetadata } from '../../js-binding.js'
import { PACKAGE_INFO } from './constants.js'
import {
  DEFAULT_OTEL_CONFIG,
  type KafkaOtelContext,
  type KafkaOtelInstrumentationConfig,
  type TracerProvider,
} from './types.js'
import {
  createBatchSpan,
  createConsumerSpan,
  createProducerSpan,
  extractTraceContext,
  getTracer,
  injectTraceContext,
  isOtelAvailable,
  normalizeHeadersToBuffer,
  setSpanStatus,
  shouldIgnoreTopic,
} from './utils.js'

export class KafkaCrabInstrumentation extends InstrumentationBase {
  private _kafkaTracer: Tracer | null = null
  private _kafkaConfig: KafkaOtelInstrumentationConfig

  constructor(config: KafkaOtelInstrumentationConfig = {}) {
    // Initialize kafka config before calling super to avoid undefined access in enable()
    const mergedConfig = { ...DEFAULT_OTEL_CONFIG, ...config }
    super(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION, mergedConfig)
    this._kafkaConfig = mergedConfig
  }

  public get kafkaConfig(): KafkaOtelInstrumentationConfig {
    return this._kafkaConfig
  }

  public get kafkaTracer(): Tracer | null {
    return this._kafkaTracer
  }

  public updateConfig(config: KafkaOtelInstrumentationConfig): void {
    // Merge new config with existing config
    this._kafkaConfig = { ...this._kafkaConfig, ...config }
  }

  public setTracerProvider(provider: TracerProvider): void {
    // Re-initialize tracer with the new provider
    this._kafkaTracer = provider.getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)
  }

  // eslint-disable-next-line class-methods-use-this
  protected init(): InstrumentationNodeModuleDefinition[] {
    // We don't need module patching since we'll instrument directly
    return []
  }

  public enable(): void {
    if (!isOtelAvailable()) {
      diag.warn('OpenTelemetry API not available, instrumentation disabled')
      return
    }

    this._kafkaTracer = getTracer(PACKAGE_INFO.NAME, PACKAGE_INFO.VERSION)

    if (this._kafkaConfig?.registerOnInitialization && this._kafkaTracer) {
      diag.debug('Kafka OTEL instrumentation enabled')
    }
  }

  public disable(): void {
    this._kafkaTracer = null
    diag.debug('Kafka OTEL instrumentation disabled')
  }

  // Check if instrumentation is enabled and available
  public isEnabled(): boolean {
    return this._kafkaTracer !== null && isOtelAvailable()
  }

  // Create OTEL context for Kafka clients
  public createOtelContext(): KafkaOtelContext {
    if (!this.isEnabled()) {
      return this._createDisabledContext()
    }

    return {
      enabled: true,
      span: trace.getActiveSpan() || null,
      // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
      tracer: this._kafkaTracer!,
      context: context.active(),
      inject: (carrier, spanToInject?: Span) => {
        // If a specific span is provided, use its context
        if (spanToInject) {
          const spanContext = trace.setSpan(context.active(), spanToInject)
          injectTraceContext(carrier, spanContext)
          return
        }

        // Otherwise try to get the active span
        const activeSpan = trace.getActiveSpan()
        if (activeSpan) {
          const spanContext = trace.setSpan(context.active(), activeSpan)
          injectTraceContext(carrier, spanContext)
        } else {
          // Fallback to active context
          injectTraceContext(carrier, context.active())
        }
      },
      extract: (carrier) => extractTraceContext(carrier),
      startSpan: (name, attributes: Attributes = {}) => {
        if (!this._kafkaTracer) {
          throw new Error('Tracer not available')
        }
        const span = this._kafkaTracer.startSpan(name, { attributes })
        return span
      },
      endSpan: (span, error) => {
        if (!span) {
          return
        }
        setSpanStatus(span, error)
        span.end()
      },
    }
  }

  // Instrument producer send operation
  public instrumentProducerSend(
    originalSend: Function,
    clientId?: string,
  ): (producerRecord: ProducerRecord) => Promise<RecordMetadata[]> {
    if (!this.isEnabled()) {
      return originalSend as (producerRecord: ProducerRecord) => Promise<RecordMetadata[]>
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!

    return async function instrumentedSend(this: KafkaProducer, record: ProducerRecord | ProducerRecord[]) {
      if (!record) {
        return originalSend.call(this, record as ProducerRecord)
      }

      // Capture the current caller context to preserve parent relationships across async boundaries
      const callerContext = context.active()

      const isArrayInput = Array.isArray(record)
      const records = isArrayInput ? record : [record]

      if (records.length === 0) {
        return originalSend.call(this, isArrayInput ? records : (record as ProducerRecord))
      }

      // If every record should be ignored, bypass instrumentation altogether
      const allIgnored = records.every((currentRecord) =>
        shouldIgnoreTopic(currentRecord.topic, instrumentation._kafkaConfig.ignoreTopics)
      )

      if (allIgnored) {
        return originalSend.call(this, isArrayInput ? records : records[0])
      }

      const spans: Span[] = []

      const spanMetadata: { span: Span; record: ProducerRecord }[] = []

      const instrumentedRecords = records.map((currentRecord) => {
        if (!currentRecord || shouldIgnoreTopic(currentRecord.topic, instrumentation._kafkaConfig.ignoreTopics)) {
          return currentRecord
        }

        // Make producer span a child of the caller's active context
        const span = createProducerSpan(tracer, currentRecord, 'send', callerContext)

        if (!span) {
          return currentRecord
        }

        if (clientId) {
          span.setAttributes({ 'messaging.client.id': clientId })
        }

        spans.push(span)
        spanMetadata.push({ span, record: currentRecord })

        const spanContext = trace.setSpan(callerContext, span)

        const instrumentedRecord: ProducerRecord = {
          ...currentRecord,
          messages: (currentRecord.messages ?? []).map(message => {
            const originalHeaders = { ...message.headers }
            const injectedHeaders = injectTraceContext(originalHeaders, spanContext)
            const normalizedHeaders = normalizeHeadersToBuffer(injectedHeaders)

            return {
              ...message,
              headers: normalizedHeaders,
            }
          }),
        }

        if (instrumentation._kafkaConfig.producerHook) {
          try {
            context.with(spanContext, () => {
              instrumentation._kafkaConfig.producerHook?.(span, currentRecord)
            })
          } catch (error) {
            diag.warn('Producer hook failed:', error)
          }
        }

        return instrumentedRecord
      })

      const payload = isArrayInput ? instrumentedRecords : instrumentedRecords[0]

      try {
        const result = await context.with(callerContext, async () => originalSend.call(this, payload as ProducerRecord))

        // Enrich spans with delivery metadata when available
        const metadataArray = Array.isArray(result) ? result : []
        for (let idx = 0; idx < spanMetadata.length; idx++) {
          const { span } = spanMetadata[idx]
          const metadata = metadataArray[idx]
          if (metadata) {
            if (metadata.partition !== undefined) {
              span.setAttribute('messaging.kafka.partition', metadata.partition)
            }
            if (metadata.offset !== undefined) {
              span.setAttribute('messaging.kafka.offset', metadata.offset)
            }
          }
          setSpanStatus(span, metadata?.error ? new Error(metadata.error.message) : undefined)
          span.end()
        }

        // If a producerHook is configured, invoke it with metadata for the first record
        if (instrumentation._kafkaConfig.producerHook && metadataArray.length) {
          const [first] = spanMetadata
          if (first) {
            try {
              instrumentation._kafkaConfig.producerHook(first.span, first.record, metadataArray[0])
            } catch (error) {
              diag.warn('Producer hook failed with metadata:', error)
            }
          }
        }

        return result
      } catch (error) {
        for (const span of spans) {
          setSpanStatus(span, error instanceof Error ? error : new Error(String(error)))
          span.end()
        }
        throw error
      }
    }
  }

  // Instrument consumer receive operation
  public instrumentConsumerReceive(originalReceive: Function, groupId?: string): () => Promise<Message | null> {
    if (!this.isEnabled()) {
      return originalReceive as () => Promise<Message | null>
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!

    return async function instrumentedReceive(this: KafkaConsumer) {
      const message = await originalReceive.call(this)

      if (!message) {
        return message
      }

      // Check if topic should be ignored
      if (shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)) {
        return message
      }

      // Extract trace context from message headers
      const parentContext = extractTraceContext(message.headers || {}) || context.active()

      // Create consumer span with extracted context
      const span = createConsumerSpan(tracer, message, 'process', parentContext)

      if (span) {
        const spanCtx = trace.setSpan(parentContext, span)

        context.with(spanCtx, () => {
          // Add consumer group if available
          if (groupId) {
            span.setAttributes({ 'messaging.consumer.group.name': groupId })
          }

          // Call message hook if configured
          if (instrumentation._kafkaConfig.messageHook) {
            try {
              instrumentation._kafkaConfig.messageHook(span, message)
            } catch (error) {
              diag.warn('Message hook failed:', error)
            }
          }

          // End span immediately for receive operation
          setSpanStatus(span)
        })

        span.end()
      }

      return message
    }
  }

  // Instrument batch consumer receive operation
  public instrumentBatchReceive(
    originalBatchReceive: Function,
    groupId?: string,
  ): (size: number, timeoutMs: number) => Promise<Message[]> {
    if (!this.isEnabled() || !this._kafkaConfig.enableBatchInstrumentation) {
      return originalBatchReceive as (size: number, timeoutMs: number) => Promise<Message[]>
    }

    // eslint-disable-next-line @typescript-eslint/no-this-alias
    const instrumentation = this
    // eslint-disable-next-line @typescript-eslint/no-non-null-assertion
    const tracer = this._kafkaTracer!

    return async function instrumentedBatchReceive(this: KafkaConsumer, size: number, timeoutMs: number) {
      const messages = await originalBatchReceive.call(this, size, timeoutMs)

      if (!Array.isArray(messages) || messages.length === 0) {
        return messages
      }

      // Filter out ignored topics
      const instrumentedMessages = messages.filter((message: Message) =>
        !shouldIgnoreTopic(message.topic, instrumentation._kafkaConfig.ignoreTopics)
      )

      if (instrumentedMessages.length === 0) {
        return messages
      }

      // Create batch span; prefer extracted context, fallback to active context
      const [firstMessage] = instrumentedMessages
      const parentContext = extractTraceContext(firstMessage.headers || {}) || context.active()
      const batchSpan = createBatchSpan(
        tracer,
        instrumentedMessages.length,
        firstMessage.topic,
        'batch_process',
        parentContext,
      )

      if (batchSpan) {
        // Add consumer group if available
        if (groupId) {
          batchSpan.setAttributes({ 'messaging.consumer.group.name': groupId })
        }

        try {
          // Create individual spans for each message in the batch within parent context
          for (const message of instrumentedMessages) {
            const msgParentContext = extractTraceContext(message.headers || {}) || parentContext
            const messageSpan = createConsumerSpan(tracer, message, 'process', msgParentContext)

            if (messageSpan) {
              const messageSpanContext = trace.setSpan(msgParentContext || context.active(), messageSpan)
              context.with(messageSpanContext, () => {
                // Link to batch span
                messageSpan.setAttributes({
                  'messaging.batch.message_count': instrumentedMessages.length,
                })

                // Call message hook if configured
                if (instrumentation._kafkaConfig.messageHook) {
                  try {
                    instrumentation._kafkaConfig.messageHook(messageSpan, message)
                  } catch (error) {
                    diag.warn('Message hook failed:', error)
                  }
                }

                setSpanStatus(messageSpan)
              })
              messageSpan.end()
            }
          }

          setSpanStatus(batchSpan)
        } catch (error) {
          setSpanStatus(batchSpan, error instanceof Error ? error : new Error(String(error)))
        } finally {
          batchSpan.end()
        }
      }

      return messages
    }
  }

  // eslint-disable-next-line class-methods-use-this
  private _createDisabledContext(): KafkaOtelContext {
    return {
      enabled: false,
      span: null,
      tracer: null,
      context: context.active(),
      inject: () => {
        /* no-op */
      },
      extract: () => context.active(),
      startSpan: () => null,
      endSpan: () => {
        /* no-op */
      },
    }
  }
}

// Singleton instance for global use
let globalInstrumentation: KafkaCrabInstrumentation | null = null

export function getKafkaInstrumentation(config?: KafkaOtelInstrumentationConfig): KafkaCrabInstrumentation {
  if (!globalInstrumentation) {
    globalInstrumentation = new KafkaCrabInstrumentation(config)
    globalInstrumentation.enable()
  } else if (config) {
    // Update existing instrumentation with new configuration
    globalInstrumentation.updateConfig(config)
  }
  return globalInstrumentation
}

export function resetKafkaInstrumentation(): void {
  if (globalInstrumentation) {
    globalInstrumentation.disable()
    globalInstrumentation = null
  }
}
