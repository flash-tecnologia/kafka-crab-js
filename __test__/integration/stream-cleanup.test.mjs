import { strict as assert } from 'node:assert'
import { test } from 'node:test'
import { KafkaClient } from '../../dist/index.js'
import { cleanupConsumer, createConsumerConfig, createTestTopic, TEST_CONFIG } from './utils.mjs'

test('Stream Resource Cleanup Integration Tests', async (t) => {
  const client = new KafkaClient(TEST_CONFIG)

  await t.test('Stream _destroy() method cleanup', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-destroy-${Date.now()}`)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    // Subscribe to topic
    await streamConsumer.subscribe(topic)

    // Verify consumer is connected by checking assignment
    const rawConsumer = streamConsumer.rawConsumer()
    assert.ok(rawConsumer, 'Raw consumer should be available')

    // Track if disconnect was called
    let disconnectCalled = false
    const originalDisconnect = rawConsumer.disconnect
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return originalDisconnect.call(this)
    }

    // Manually destroy the stream (simulates error or explicit destroy)
    const destroyPromise = new Promise((resolve, reject) => {
      streamConsumer.on('error', reject)
      streamConsumer.on('close', resolve)

      // Trigger destroy
      streamConsumer.destroy(new Error('Test destroy'))
    })

    // Wait for stream to be destroyed
    await destroyPromise

    // Verify disconnect was called during cleanup
    assert.ok(disconnectCalled, 'Consumer disconnect should be called during stream destroy')

    // Verify stream is properly closed
    assert.ok(streamConsumer.destroyed, 'Stream should be marked as destroyed')
  })

  await t.test('Stream _final() method cleanup on end', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-final-${Date.now()}`)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    // Subscribe to topic
    await streamConsumer.subscribe(topic)

    // Track if disconnect was called
    let disconnectCalled = false
    const rawConsumer = streamConsumer.rawConsumer()
    const originalDisconnect = rawConsumer.disconnect
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return originalDisconnect.call(this)
    }

    // End the stream normally (triggers _final)
    const endPromise = new Promise((resolve) => {
      streamConsumer.on('end', resolve)
    })

    // For readable streams, we need to push null to signal end
    streamConsumer.push(null)

    // Wait for stream to end
    await endPromise

    // Give a moment for cleanup to complete
    await new Promise(resolve => setTimeout(resolve, 100))

    // Verify disconnect was called during final cleanup
    assert.ok(disconnectCalled, 'Consumer disconnect should be called during stream end')
  })

  await t.test('Stream cleanup handles disconnect errors gracefully', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-error-${Date.now()}`)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    // Subscribe to topic
    await streamConsumer.subscribe(topic)

    // Mock disconnect to throw error
    const rawConsumer = streamConsumer.rawConsumer()
    rawConsumer.disconnect = async function() {
      throw new Error('Disconnect failed')
    }

    // Track errors
    let destroyError = null
    const destroyPromise = new Promise((resolve) => {
      streamConsumer.on('error', (error) => {
        destroyError = error
      })
      streamConsumer.on('close', resolve)

      // Trigger destroy with original error
      streamConsumer.destroy(new Error('Original error'))
    })

    // Wait for stream to be destroyed
    await destroyPromise

    // Verify that disconnect error is combined with original error
    assert.ok(destroyError, 'Should emit combined error')
    assert.ok(destroyError.message.includes('Original error'), 'Should include original error')
    assert.ok(destroyError.message.includes('Disconnect failed'), 'Should include disconnect error')

    // Verify stream is still properly destroyed despite disconnect error
    assert.ok(streamConsumer.destroyed, 'Stream should be marked as destroyed even with disconnect error')
  })

  await t.test('Stream cleanup prevents memory leaks', async () => {
    const topic = createTestTopic()

    // Create multiple streams and destroy them
    const streams = []
    for (let i = 0; i < 5; i++) {
      const consumerConfig = createConsumerConfig(`cleanup-leak-${Date.now()}-${i}`)
      const streamConsumer = client.createStreamConsumer(consumerConfig)
      await streamConsumer.subscribe(topic)
      streams.push(streamConsumer)
    }

    // Destroy all streams
    const cleanupPromises = streams.map(stream =>
      new Promise((resolve) => {
        stream.on('close', resolve)
        stream.destroy()
      })
    )

    // Wait for all streams to be cleaned up
    await Promise.all(cleanupPromises)

    // Verify all streams are destroyed
    for (let i = 0; i < streams.length; i++) {
      assert.ok(streams[i].destroyed, `Stream ${i} should be destroyed`)
    }

    // This test doesn't directly measure memory, but ensures proper cleanup pattern
    assert.ok(true, 'All streams cleaned up without hanging')
  })

  await t.test('Batch stream cleanup works correctly', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-batch-${Date.now()}`)

    // Create batch stream consumer (batchSize > 1)
    const batchStreamConsumer = client.createStreamConsumer({
      ...consumerConfig,
      batchSize: 10,
      batchTimeout: 1000,
    })

    // Subscribe to topic
    await batchStreamConsumer.subscribe(topic)

    // Track if disconnect was called
    let disconnectCalled = false
    const rawConsumer = batchStreamConsumer.rawConsumer()
    const originalDisconnect = rawConsumer.disconnect
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return originalDisconnect.call(this)
    }

    // Destroy the batch stream
    const destroyPromise = new Promise((resolve) => {
      batchStreamConsumer.on('close', resolve)
      batchStreamConsumer.destroy()
    })

    // Wait for stream to be destroyed
    await destroyPromise

    // Verify cleanup happened
    assert.ok(disconnectCalled, 'Batch stream should also call disconnect during cleanup')
    assert.ok(batchStreamConsumer.destroyed, 'Batch stream should be marked as destroyed')
  })

  await t.test('Stream unsubscribe errors are handled during cleanup', async () => {
    const topic = createTestTopic()
    const consumerConfig = createConsumerConfig(`cleanup-unsubscribe-${Date.now()}`)

    // Create stream consumer
    const streamConsumer = client.createStreamConsumer(consumerConfig)

    // Subscribe to topic
    await streamConsumer.subscribe(topic)

    // Mock unsubscribe to throw error
    const rawConsumer = streamConsumer.rawConsumer()
    const originalUnsubscribe = rawConsumer.unsubscribe
    rawConsumer.unsubscribe = function() {
      throw new Error('Unsubscribe failed')
    }

    // Mock disconnect to succeed
    let disconnectCalled = false
    rawConsumer.disconnect = async function() {
      disconnectCalled = true
      return Promise.resolve()
    }

    // Destroy the stream
    const destroyPromise = new Promise((resolve) => {
      streamConsumer.on('close', resolve)
      streamConsumer.destroy()
    })

    // Wait for stream to be destroyed
    await destroyPromise

    // Verify that unsubscribe error doesn't prevent disconnect
    assert.ok(disconnectCalled, 'Disconnect should still be called even if unsubscribe fails')
    assert.ok(streamConsumer.destroyed, 'Stream should be destroyed despite unsubscribe error')
  })
})
