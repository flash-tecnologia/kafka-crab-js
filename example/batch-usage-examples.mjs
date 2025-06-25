#!/usr/bin/env node

/**
 * Kafka Crab JS - Batch Processing Usage Examples
 *
 * Practical examples showing when and how to use batch processing
 * for different scenarios and workloads.
 */

import { KafkaClient } from '../dist/index.js'

// Configuration
const config = {
  brokers: process.env.KAFKA_BROKERS || 'localhost:9092',
  clientId: 'batch-examples-client',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
}

/**
 * Example 1: High-throughput data processing
 * Use case: Processing large volumes of log data, metrics, or events
 */
async function highThroughputExample() {
  console.log('📊 Example 1: High-Throughput Data Processing')
  console.log('Use case: Processing large volumes of log data\n')

  const kafkaClient = new KafkaClient(config)
  const consumer = kafkaClient.createConsumer({
    groupId: 'high-throughput-processor',
    enableAutoCommit: true,
  })

  await consumer.subscribe([{ topic: 'application-logs' }])

  try {
    console.log('🚀 Using direct batch API for maximum throughput...')

    let totalProcessed = 0
    const startTime = Date.now()

    while (totalProcessed < 1000) { // Process 1000 messages
      // Large batch size for high throughput
      const messages = await consumer.recvBatch(100, 1000)

      if (messages.length === 0) break

      // Batch processing - more efficient than individual processing
      const logEntries = messages.map(message => {
        try {
          return JSON.parse(message.payload.toString())
        } catch {
          return { error: 'Invalid JSON', raw: message.payload.toString() }
        }
      })

      // Process logs in batch (aggregation, filtering, etc.)
      const errors = logEntries.filter(log => log.level === 'ERROR')
      const warnings = logEntries.filter(log => log.level === 'WARN')

      totalProcessed += messages.length

      console.log(`   Processed ${messages.length} logs (${errors.length} errors, ${warnings.length} warnings)`)
      console.log(
        `   Total processed: ${totalProcessed}, Rate: ${
          (totalProcessed / (Date.now() - startTime) * 1000).toFixed(1)
        } msg/sec`,
      )
    }
  } catch (error) {
    console.error('❌ Error:', error)
  } finally {
    consumer.unsubscribe()
  }

  console.log('✅ High-throughput processing completed\n')
}

/**
 * Example 2: Adaptive batch processing with stream
 * Use case: Real-time processing that adapts to message volume
 */
async function adaptiveStreamExample() {
  console.log('🔄 Example 2: Adaptive Stream Processing')
  console.log('Use case: Real-time processing that adapts to load\n')

  const kafkaClient = new KafkaClient(config)
  const stream = kafkaClient.createStreamConsumer({
    groupId: 'adaptive-processor',
    enableAutoCommit: true,
  })

  await stream.subscribe([{ topic: 'user-events' }])

  let messageCount = 0
  let lastCountTime = Date.now()

  // Start in single message mode for low latency
  console.log('🐌 Starting in single message mode (low latency)...')
  console.log(`   Batch mode: ${stream.isBatchModeEnabled()}`)

  stream.on('data', (message) => {
    messageCount++

    try {
      const event = JSON.parse(message.payload.toString())

      // Simulate processing user event
      console.log(`   Processing user event: ${event.type} for user ${event.userId}`)

      // Adaptive logic: switch to batch mode if high volume
      const currentTime = Date.now()
      const timeDiff = currentTime - lastCountTime

      if (timeDiff >= 5000) { // Check every 5 seconds
        const messagesPerSecond = (messageCount / timeDiff) * 1000

        console.log(`\n📈 Current rate: ${messagesPerSecond.toFixed(1)} msg/sec`)

        if (messagesPerSecond > 10 && !stream.isBatchModeEnabled()) {
          console.log('🚀 High volume detected! Switching to batch mode...')
          stream.enableBatchMode(25, 200)
          console.log(`   Batch config: ${JSON.stringify(stream.getBatchConfig())}`)
        } else if (messagesPerSecond < 5 && stream.isBatchModeEnabled()) {
          console.log('🐌 Low volume detected! Switching to single mode...')
          stream.disableBatchMode()
          console.log(`   Batch config: ${JSON.stringify(stream.getBatchConfig())}`)
        }

        messageCount = 0
        lastCountTime = currentTime
      }
    } catch (e) {
      console.error('   Failed to parse event:', e.message)
    }
  })

  stream.on('error', (error) => {
    console.error('❌ Stream error:', error)
  })

  // Run for 30 seconds
  setTimeout(() => {
    console.log('\n⏰ Stopping adaptive stream...')
    stream.unsubscribe()
    console.log('✅ Adaptive processing completed\n')
  }, 30000)
}

/**
 * Example 3: Batch processing with custom business logic
 * Use case: Order processing, transaction batching
 */
async function businessLogicExample() {
  console.log('💼 Example 3: Business Logic Batch Processing')
  console.log('Use case: Order processing with transaction batching\n')

  const kafkaClient = new KafkaClient(config)
  const consumer = kafkaClient.createConsumer({
    groupId: 'order-processor',
    enableAutoCommit: true,
  })

  await consumer.subscribe([{ topic: 'orders' }])

  try {
    console.log('💳 Processing orders in batches for efficiency...')

    let batchNumber = 1

    while (batchNumber <= 5) { // Process 5 batches
      console.log(`\n📦 Processing batch ${batchNumber}:`)

      // Smaller batch size for business transactions
      const messages = await consumer.recvBatch(10, 2000)

      if (messages.length === 0) {
        console.log('   No more orders to process')
        break
      }

      // Parse orders
      const orders = []
      for (const message of messages) {
        try {
          const order = JSON.parse(message.payload.toString())
          orders.push({
            ...order,
            kafkaMetadata: {
              partition: message.partition,
              offset: message.offset,
              timestamp: message.timestamp,
            },
          })
        } catch (e) {
          console.error('   ❌ Failed to parse order:', e.message)
        }
      }

      // Business logic: process orders in batch
      console.log(`   📋 Received ${orders.length} orders`)

      // Group by customer for batch processing
      const ordersByCustomer = orders.reduce((acc, order) => {
        if (!acc[order.customerId]) acc[order.customerId] = []
        acc[order.customerId].push(order)
        return acc
      }, {})

      // Process each customer's orders as a unit
      for (const [customerId, customerOrders] of Object.entries(ordersByCustomer)) {
        const totalAmount = customerOrders.reduce((sum, order) => sum + order.amount, 0)

        console.log(`   💰 Customer ${customerId}: ${customerOrders.length} orders, total: $${totalAmount.toFixed(2)}`)

        // Simulate batch database operations
        // - Insert orders
        // - Update inventory
        // - Process payments
        // - Send notifications

        if (totalAmount > 1000) {
          console.log(`   🎁 VIP customer discount applied!`)
        }
      }

      console.log(`   ✅ Batch ${batchNumber} processed successfully`)
      batchNumber++
    }
  } catch (error) {
    console.error('❌ Error:', error)
  } finally {
    consumer.unsubscribe()
  }

  console.log('\n✅ Business logic processing completed\n')
}

/**
 * Example 4: Performance comparison in real-time
 * Use case: Demonstrating the performance difference
 */
async function performanceComparisonExample() {
  console.log('⚡ Example 4: Real-time Performance Comparison')
  console.log('Use case: Side-by-side performance demonstration\n')

  const kafkaClient = new KafkaClient(config)

  // Create two consumers for comparison
  const singleConsumer = kafkaClient.createConsumer({
    groupId: 'single-mode-demo',
    enableAutoCommit: true,
  })

  const batchConsumer = kafkaClient.createConsumer({
    groupId: 'batch-mode-demo',
    enableAutoCommit: true,
  })

  await singleConsumer.subscribe([{ topic: 'performance-test' }])
  await batchConsumer.subscribe([{ topic: 'performance-test' }])

  console.log('🏃‍♂️ Starting performance comparison...')
  console.log('   Single mode vs Batch mode processing\n')

  // Single message processing
  const singleStart = Date.now()
  let singleCount = 0

  console.log('🐌 Single mode processing:')
  try {
    for (let i = 0; i < 50; i++) {
      const message = await singleConsumer.recv()
      if (message) {
        singleCount++
        // Simulate processing
        const _data = JSON.parse(message.payload.toString())
        process.stdout.write(`\r   Processed: ${singleCount} messages`)
      }
    }
  } catch {
    console.log(`\n   ⚠️  Single mode completed with ${singleCount} messages`)
  }

  const singleTime = Date.now() - singleStart
  const singleThroughput = (singleCount / singleTime) * 1000

  console.log(
    `\n   📊 Single mode: ${singleCount} messages in ${singleTime}ms (${singleThroughput.toFixed(1)} msg/sec)\n`,
  )

  // Batch processing
  const batchStart = Date.now()
  let batchCount = 0
  let batches = 0

  console.log('🚀 Batch mode processing:')
  try {
    while (batchCount < 50) {
      const messages = await batchConsumer.recvBatch(10, 500)
      if (messages.length === 0) break

      batches++
      batchCount += messages.length

      // Process batch
      for (const message of messages) {
        const _data = JSON.parse(message.payload.toString())
        // Batch processing is more efficient
      }

      process.stdout.write(`\r   Processed: ${batchCount} messages in ${batches} batches`)
    }
  } catch {
    console.log(`\n   ⚠️  Batch mode completed with ${batchCount} messages`)
  }

  const batchTime = Date.now() - batchStart
  const batchThroughput = (batchCount / batchTime) * 1000

  console.log(`\n   📊 Batch mode: ${batchCount} messages in ${batchTime}ms (${batchThroughput.toFixed(1)} msg/sec)`)

  // Comparison
  if (batchThroughput > singleThroughput) {
    const improvement = ((batchThroughput - singleThroughput) / singleThroughput) * 100
    console.log(`\n🏆 Batch mode is ${improvement.toFixed(1)}% faster!`)
  }

  singleConsumer.unsubscribe()
  batchConsumer.unsubscribe()

  console.log('\n✅ Performance comparison completed\n')
}

/**
 * Main example runner
 */
async function runExamples() {
  console.log('🦀 Kafka Crab JS - Batch Processing Examples')
  console.log('='.repeat(50))
  console.log('Practical examples of batch processing usage\n')

  const examples = [
    { name: 'High-Throughput Processing', fn: highThroughputExample },
    { name: 'Adaptive Stream Processing', fn: adaptiveStreamExample },
    { name: 'Business Logic Batching', fn: businessLogicExample },
    { name: 'Performance Comparison', fn: performanceComparisonExample },
  ]

  for (const example of examples) {
    try {
      await example.fn()
      await new Promise(resolve => setTimeout(resolve, 1000)) // Wait between examples
    } catch (error) {
      console.error(`❌ ${example.name} failed:`, error)
    }
  }

  console.log('🎉 All examples completed!')
}

// Run examples if Kafka is available
if (process.env.KAFKA_AVAILABLE === 'true') {
  runExamples().catch(console.error)
} else {
  console.log('🦀 Kafka Crab JS - Batch Processing Examples')
  console.log('='.repeat(50))
  console.log('')
  console.log('💡 To run these examples:')
  console.log('')
  console.log('1. Start Kafka:')
  console.log('   docker run -p 9092:9092 apache/kafka:latest')
  console.log('')
  console.log('2. Create topics:')
  console.log('   kafka-topics --create --topic application-logs --bootstrap-server localhost:9092')
  console.log('   kafka-topics --create --topic user-events --bootstrap-server localhost:9092')
  console.log('   kafka-topics --create --topic orders --bootstrap-server localhost:9092')
  console.log('   kafka-topics --create --topic performance-test --bootstrap-server localhost:9092')
  console.log('')
  console.log('3. Produce some test data to the topics')
  console.log('')
  console.log('4. Run examples:')
  console.log('   KAFKA_AVAILABLE=true node example/batch-usage-examples.mjs')
  console.log('')
  console.log('📋 Examples included:')
  console.log('   • High-throughput log processing')
  console.log('   • Adaptive batch processing based on load')
  console.log('   • Business logic with transaction batching')
  console.log('   • Real-time performance comparison')
  console.log('')
}
