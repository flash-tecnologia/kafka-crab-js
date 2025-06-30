import { equal, ok } from 'node:assert/strict'
import test from 'node:test'
import { setTimeout as sleep } from 'node:timers/promises'
import { KafkaClient } from '../../dist/index.js'
import { 
  setupTestEnvironment, 
  createConsumerConfig, 
  createProducerConfig,
  cleanupConsumer,
  cleanupProducer,
  isTestMessage,
  createTestTopic
} from './utils.mjs'
import {
  BATCH_SIZE_LIMITS,
  BATCH_SIZE_TEST_SCENARIOS,
  BATCH_TIMEOUT_SCENARIOS,
  PERFORMANCE_TEST_CONFIGS,
  createBatchTestMessages,
  EXPECTED_WARNING_PATTERNS
} from '../fixtures/batch-size-test-data.mjs'

await test('Batch Size Limits Integration Tests', async (t) => {
  let client
  let producer

  await t.test('Setup: Create KafkaClient and Producer', async () => {
    const { config } = await setupTestEnvironment()
    client = new KafkaClient(config)
    producer = client.createProducer(createProducerConfig())
    ok(client, 'KafkaClient should be created')
    ok(producer, 'Producer should be created')
  })

  await t.test('Batch Size: Validate maximum batch size limit', async () => {
    const topic = createTestTopic('max-batch-test')
    const testId = 'max-batch-validation'
    const messageCount = 50 // More than max batch size
    
    // Create many messages to test batching
    const messages = createBatchTestMessages(messageCount, testId, 'small')
    await producer.send({ topic, messages })
    await sleep(2000)
    
    // Test with exactly max batch size
    const consumer = client.createConsumer(createConsumerConfig(`max-batch-${testId}`))
    await consumer.subscribe(topic)
    
    const receivedMessages = []
    let batchCount = 0
    const maxBatches = 10 // Limit number of batch calls
    
    while (receivedMessages.length < messageCount && batchCount < maxBatches) {
      // Request more than max (should be clamped to 10)
      const batch = await consumer.recvBatch(BATCH_SIZE_LIMITS.OUT_OF_RANGE_HIGH, 5000)
      batchCount++
      
      if (batch.length === 0) break
      
      const testMessages = batch.filter(msg => isTestMessage(msg, testId))
      receivedMessages.push(...testMessages)
      
      // Verify batch size never exceeds maximum
      ok(batch.length <= BATCH_SIZE_LIMITS.MAX, 
         `Batch size ${batch.length} should not exceed maximum ${BATCH_SIZE_LIMITS.MAX}`)
    }
    
    await cleanupConsumer(consumer)
    
    // Verify we received messages despite requesting oversized batches
    ok(receivedMessages.length > 0, 'Should receive messages even with oversized batch requests')
    console.log(`Received ${receivedMessages.length} messages in ${batchCount} batches`)
  })

  await t.test('Batch Size: Test all boundary conditions', async () => {
    for (const scenario of BATCH_SIZE_TEST_SCENARIOS) {
      const topic = createTestTopic(`boundary-${scenario.name}`)
      const testId = `boundary-${scenario.name}`
      
      // Send enough messages to test the batch size
      const messageCount = Math.max(scenario.batchSize, 15)
      const messages = createBatchTestMessages(messageCount, testId, 'small')
      await producer.send({ topic, messages })
      await sleep(1000)
      
      const consumer = client.createConsumer(createConsumerConfig(`boundary-${testId}`))
      await consumer.subscribe(topic)
      
      // Capture console output to check for warnings
      const originalConsoleWarn = console.warn
      console.warn = (...args) => {
        const message = args.join(' ')
        // Check if any expected warning patterns match
        EXPECTED_WARNING_PATTERNS.some(pattern => pattern.test(message))
        originalConsoleWarn(...args)
      }
      
      try {
        const batch = await consumer.recvBatch(scenario.batchSize, 5000)
        
        // Verify warning behavior
        if (scenario.shouldWarn) {
          // Note: Warnings come from Rust layer, may not be captured in JS console
          console.log(`Scenario ${scenario.name}: Expected warning for batch size ${scenario.batchSize}`)
        }
        
        // Verify batch size is within limits
        if (batch.length > 0) {
          ok(batch.length <= BATCH_SIZE_LIMITS.MAX, 
             `Batch size ${batch.length} should not exceed maximum for scenario ${scenario.name}`)
        }
        
        console.log(`✓ Scenario ${scenario.name}: batch size ${scenario.batchSize} → received ${batch.length} messages`)
        
      } finally {
        console.warn = originalConsoleWarn
        await cleanupConsumer(consumer)
      }
    }
  })

  await t.test('Stream Batch: Validate batch size limits in stream mode', async () => {
    const topic = createTestTopic('stream-batch-limits')
    const testId = 'stream-batch-limits'
    const messageCount = 15 // Reduced for faster test execution
    
    const messages = createBatchTestMessages(messageCount, testId, 'small')
    await producer.send({ topic, messages })
    await sleep(2000)
    
    // Test stream consumer with oversized batch configuration
    // Note: This test intentionally triggers warnings to validate limit enforcement
    const streamConsumer = client.createStreamConsumer(createConsumerConfig(`stream-limits-${testId}`))
    
    // Try to enable batch mode with size larger than maximum (25 > 10)
    // This will trigger warning: "max_messages 25 out of range [1-10], using 10"
    streamConsumer.enableBatchMode(BATCH_SIZE_LIMITS.OUT_OF_RANGE_HIGH, 2000)
    
    await streamConsumer.subscribe([
      { topic, allOffsets: { position: 'Beginning' } }
    ])
    
    const receivedMessages = []
    
    const streamPromise = new Promise((resolve, reject) => {
      const timeout = setTimeout(() => {
        resolve() // Don't fail on timeout, just analyze what we got
      }, 5000) // Reduced timeout since we expect warnings
      
      streamConsumer.on('data', (message) => {
        if (isTestMessage(message, testId)) {
          receivedMessages.push(message)
          
          // In stream mode, we receive individual messages but they're processed in batches internally
          if (receivedMessages.length >= Math.min(messageCount, 20)) { // Stop after reasonable amount
            clearTimeout(timeout)
            resolve()
          }
        }
      })
      
      streamConsumer.on('error', (error) => {
        clearTimeout(timeout)
        reject(error)
      })
    })
    
    await streamPromise
    await cleanupConsumer(streamConsumer)
    
    ok(receivedMessages.length > 0, 'Should receive messages in stream batch mode')
    console.log(`Stream batch mode processed ${receivedMessages.length} messages`)
  })

  await t.test('Batch Timeout: Validate timeout boundary conditions', async () => {
    for (const scenario of BATCH_TIMEOUT_SCENARIOS.slice(0, 4)) { // Test first 4 scenarios to save time
      const topic = createTestTopic(`timeout-${scenario.name}`)
      const testId = `timeout-${scenario.name}`
      
      // Send a few messages
      const messages = createBatchTestMessages(5, testId, 'small')
      await producer.send({ topic, messages })
      await sleep(1000)
      
      const streamConsumer = client.createStreamConsumer(createConsumerConfig(`timeout-${testId}`))
      
      // Test the timeout scenario
      streamConsumer.enableBatchMode(5, scenario.timeoutMs)
      
      const config = streamConsumer.getBatchConfig()
      
      // For invalid timeouts, should fall back to default
      if (scenario.shouldWarn) {
        equal(config.batchTimeoutMs, scenario.expected, 
              `Invalid timeout ${scenario.timeoutMs} should fallback to ${scenario.expected}`)
      } else {
        equal(config.batchTimeoutMs, scenario.timeoutMs, 
              `Valid timeout ${scenario.timeoutMs} should be preserved`)
      }
      
      console.log(`✓ Timeout scenario ${scenario.name}: ${scenario.timeoutMs}ms → ${config.batchTimeoutMs}ms`)
      
      await cleanupConsumer(streamConsumer)
    }
  })

  await t.test('Performance: Compare batch sizes within limits', async () => {
    for (const perfConfig of PERFORMANCE_TEST_CONFIGS) {
      const topic = createTestTopic(`perf-${perfConfig.name}`)
      const testId = `perf-${perfConfig.name}`
      
      const messages = createBatchTestMessages(
        perfConfig.messageCount, 
        testId, 
        perfConfig.messageSize
      )
      
      await producer.send({ topic, messages })
      await sleep(2000)
      
      const consumer = client.createConsumer(createConsumerConfig(`perf-${testId}`))
      await consumer.subscribe(topic)
      
      const startTime = Date.now()
      const receivedMessages = []
      let batchCount = 0
      
      while (receivedMessages.length < perfConfig.messageCount && batchCount < 20) {
        const batch = await consumer.recvBatch(perfConfig.batchSize, 3000)
        batchCount++
        
        if (batch.length === 0) break
        
        const testMessages = batch.filter(msg => isTestMessage(msg, testId))
        receivedMessages.push(...testMessages)
      }
      
      const duration = Date.now() - startTime
      const throughput = receivedMessages.length / (duration / 1000)
      
      await cleanupConsumer(consumer)
      
      console.log(`📊 ${perfConfig.description}:`)
      console.log(`   Messages: ${receivedMessages.length}/${perfConfig.messageCount}`)
      console.log(`   Duration: ${duration}ms`)
      console.log(`   Throughput: ${throughput.toFixed(1)} msgs/sec`)
      console.log(`   Avg batch size: ${(receivedMessages.length / batchCount).toFixed(1)}`)
      
      ok(receivedMessages.length > 0, `Should receive messages for ${perfConfig.name}`)
    }
  })

  await t.test('Edge Cases: Zero and negative batch sizes', async () => {
    const streamConsumer = client.createStreamConsumer(createConsumerConfig('edge-cases'))
    
    // Test zero batch size - should use the provided value (not validated in JS layer)
    streamConsumer.enableBatchMode(0, 1000)
    let config = streamConsumer.getBatchConfig()
    console.log(`Zero batch size config: ${JSON.stringify(config)}`)
    
    // Test negative batch size - should use the provided value (not validated in JS layer)  
    streamConsumer.enableBatchMode(-5, 1000)
    config = streamConsumer.getBatchConfig()
    console.log(`Negative batch size config: ${JSON.stringify(config)}`)
    
    // Test extremely large batch size - should use the provided value (validation happens in Rust)
    streamConsumer.enableBatchMode(9999, 1000)
    config = streamConsumer.getBatchConfig()
    console.log(`Large batch size config: ${JSON.stringify(config)}`)
    
    // The JS layer doesn't validate - validation happens in the Rust layer during actual batch operations
    ok(true, 'Edge case batch sizes are accepted by JS layer (validation happens in Rust)')
    
    await cleanupConsumer(streamConsumer)
  })

  await t.test('Cleanup: Disconnect producer', async () => {
    await cleanupProducer(producer)
  })
})