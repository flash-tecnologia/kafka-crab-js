import { context, propagation, SpanKind, trace } from '@opentelemetry/api'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { W3CTraceContextPropagator } from '@opentelemetry/core'
import { InMemorySpanExporter, SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import assert from 'node:assert/strict'
import { createRequire } from 'node:module'
import { test } from 'node:test'

const require = createRequire(import.meta.url)

const {
  KafkaCrabInstrumentation,
  KafkaClient,
  KafkaClientConfig,
  KafkaStreamReadable,
  KafkaBatchStreamReadable,
  resetKafkaInstrumentation,
} = require('../../dist/index.cjs')

function setupOtelProvider() {
  const contextManager = new AsyncHooksContextManager()
  context.setGlobalContextManager(contextManager.enable())
  propagation.setGlobalPropagator(new W3CTraceContextPropagator())

  const exporter = new InMemorySpanExporter()
  const provider = new NodeTracerProvider()
  provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
  provider.register()

  return { contextManager, exporter, provider }
}

function teardownOtelProvider({ contextManager, exporter, provider }) {
  contextManager.disable()
  exporter.reset()
  provider.shutdown().catch(() => {})
}

test('producer spans propagate context to consumer spans via Kafka headers', async () => {
  const otel = setupOtelProvider()

  const instrumentation = new KafkaCrabInstrumentation()
  instrumentation.enable()
  instrumentation.setTracerProvider(otel.provider)

  assert(instrumentation.isEnabled(), 'Instrumentation should be enabled')

  let capturedRecord
  const originalSend = async function(record) {
    capturedRecord = record
    return []
  }

  const instrumentedSend = instrumentation.instrumentProducerSend(originalSend, 'client-a')

  const record = {
    topic: 'test-topic',
    messages: [
      {
        payload: Buffer.from('hello world'),
        headers: {},
      },
    ],
  }

  await instrumentedSend.call({}, record)

  assert(capturedRecord, 'Original send should receive the instrumented record')
  const headers = capturedRecord.messages[0].headers
  assert(headers.traceparent, 'Trace context should be injected into headers')
  assert(Buffer.isBuffer(headers.traceparent), 'Injected trace header should be a Buffer')
  const traceparentHeader = headers.traceparent.toString('utf8')
  const injectedTraceId = traceparentHeader.split('-')[1]

  const otelContext = instrumentation.createOtelContext()
  const extractedSpanContext = trace.getSpanContext(otelContext.extract(headers))
  assert(extractedSpanContext, 'Extracted span context should exist')
  assert.equal(extractedSpanContext.traceId, injectedTraceId, 'Extracted trace id should match injected header')

  await otel.provider.forceFlush()

  const originalReceive = async () => ({
    topic: 'test-topic',
    partition: 0,
    offset: 0,
    payload: Buffer.from('hello world'),
    headers,
  })

  const instrumentedReceive = instrumentation.instrumentConsumerReceive(originalReceive, 'group-a')
  await instrumentedReceive.call({})

  await otel.provider.forceFlush()

  const spans = otel.exporter.getFinishedSpans()
  const producerSpan = spans.find(span => span.kind === SpanKind.PRODUCER)
  const consumerSpan = spans.find(span => span.kind === SpanKind.CONSUMER)

  assert(producerSpan, 'Producer span should be recorded')
  assert(consumerSpan, 'Consumer span should be recorded')
  assert.equal(
    consumerSpan.spanContext().traceId,
    producerSpan.spanContext().traceId,
    'Consumer span should share trace with producer span',
  )
  assert.equal(
    consumerSpan.parentSpanId,
    producerSpan.spanContext().spanId,
    'Consumer span should be the child of the producer span',
  )

  instrumentation.disable()
  teardownOtelProvider(otel)
})

test('createStreamConsumer instruments underlying consumer when OTEL is enabled', async () => {
  resetKafkaInstrumentation()

  const originalCreateConsumer = KafkaClientConfig.prototype.createConsumer

  const makeStubConsumer = () => {
    const consumer = {
      recv: async () => null,
      recvBatch: async () => [],
      subscribe: async () => {},
      unsubscribe: () => {},
      disconnect: async () => {},
      commit: async () => {},
      seek: () => {},
    }

    consumer._originalRecv = consumer.recv
    consumer._originalRecvBatch = consumer.recvBatch
    return consumer
  }

  const singleConsumer = makeStubConsumer()
  const batchConsumer = makeStubConsumer()

  const consumerQueue = [singleConsumer, batchConsumer]

  KafkaClientConfig.prototype.createConsumer = function mockCreateConsumer() {
    const next = consumerQueue.shift()
    if (!next) {
      throw new Error('No stub consumers left')
    }
    return next
  }

  try {
    const client = new KafkaClient({
      brokers: 'localhost:9092',
      clientId: 'otel-test-client',
      otel: {
        serviceName: 'otel-stream-test',
      },
    })

    const stream = client.createStreamConsumer({
      groupId: 'stream-group',
      enableAutoCommit: false,
      streamOptions: { objectMode: true },
    })

    assert(stream instanceof KafkaStreamReadable, 'Should create a KafkaStreamReadable instance')

    const instrumentedSingle = stream.rawConsumer()
    assert.notStrictEqual(
      instrumentedSingle.recv,
      instrumentedSingle._originalRecv,
      'Single message consumer should have instrumented recv',
    )

    await stream.disconnect()

    const batchStream = client.createStreamConsumer({
      groupId: 'batch-group',
      enableAutoCommit: false,
      batchSize: 5,
      batchTimeout: 50,
      streamOptions: { objectMode: true },
    })

    assert(batchStream instanceof KafkaBatchStreamReadable, 'Should create a KafkaBatchStreamReadable instance')

    const instrumentedBatch = batchStream.rawConsumer()
    assert.notStrictEqual(
      instrumentedBatch.recv,
      instrumentedBatch._originalRecv,
      'Batch consumer should have instrumented recv',
    )
    assert.notStrictEqual(
      instrumentedBatch.recvBatch,
      instrumentedBatch._originalRecvBatch,
      'Batch consumer should have instrumented recvBatch',
    )

    await batchStream.disconnect()
  } finally {
    KafkaClientConfig.prototype.createConsumer = originalCreateConsumer
    resetKafkaInstrumentation()
  }
})
