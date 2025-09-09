<div align="center">

# 🦀 Kafka Crab JS 🦀

A lightweight, flexible, and reliable Kafka client for JavaScript/TypeScript. It is built using Rust and librdkafka, providing a high-performance and feature-rich Kafka client.

[![npm version](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
</div>

---

## What's New in Version 2.0.0

### BREAKING CHANGES ⚠️

This major version includes important breaking changes that improve API consistency and memory management:

1. **Stream Lifecycle Management**:
   - Stream consumers now properly implement Node.js stream lifecycle methods (`_destroy()`, `_final()`)
   - **Memory leak prevention**: Streams now automatically disconnect Kafka consumers during destruction
   - Better resource cleanup ensures no hanging connections or memory leaks
   - Error handling during stream destruction is improved with proper error propagation

2. **Stream Error Handling**:
   - Stream errors now use `destroy(error)` instead of `emit('error')` for proper stream termination
   - This ensures streams are properly closed when errors occur, preventing resource leaks
   - Error propagation follows Node.js stream standards more closely

3. **Rust Configuration Simplification**:
   - Removed `MAX_BATCH_TIMEOUT` constant - now uses only `DEFAULT_BATCH_TIMEOUT` (1000ms)
   - Simplified timeout validation logic in Rust layer
   - Invalid timeout values (< 1ms) now log warnings and use default timeout

4. **Code Quality Improvements**:
   - Fixed linting violations: short variable names and `any` type usage
   - Better TypeScript typing throughout the codebase
   - Removed redundant validation functions for cleaner code

### Migration Guide

- **No API changes required** - all public interfaces remain the same
- **Stream memory management** - streams now automatically clean up resources
- **Error handling** - streams will properly terminate on errors (existing error handling code still works)
- **Performance** - simplified timeout logic may provide marginal performance improvements

---

## What's New in Version 1.10.0

### Major Updates:

1. **Performance Benchmark Suite**:
   - Added comprehensive benchmark suite comparing performance against `kafkajs`, `node-rdkafka`, and `@platformatic/rdkafka`
   - Benchmark scripts with automated setup and data generation
   - Performance testing for both serial and batch processing modes
   - Supports configurable iterations and message sizes for realistic testing scenarios

2. **Enhanced Stream Consumer Configuration**:
   - Stream consumer constructor now accepts optional `ReadableOptions` parameter
   - Default `objectMode: true` for better stream processing ergonomics
   - Improved batch processing configuration with proper defaults

3. **Flexible Configuration System**:
   - **BREAKING**: Configuration properties now accept `Record<string, any>` instead of `Record<string, string>`
   - Enhanced type flexibility for complex configuration values (numbers, booleans, objects)
   - Better support for advanced librdkafka configuration options

4. **Enhanced Topic Management**:
   - Added `createTopic` and `numPartitions` options to `TopicPartitionConfig`
   - Improved topic creation control during consumer subscription
   - Better handling of partition assignment and topic setup

5. **Build System Improvements**:
   - Updated TypeScript configuration with modular `tsconfig.base.json`
   - Added `typecheck` script for standalone type checking
   - Enhanced build pipeline with better declaration file generation
   - Updated to pnpm@10.15.1 for improved dependency management

6. **Development Tools Enhancement**:
   - Enhanced oxlint configuration with benchmark-specific rules
   - Added support for benchmark directory linting and formatting
   - Improved development workflow with better error handling rules

### Previous Version (1.9.0) Updates:
7. **Consumer Topic Creation Control**:
   - Added fine-grained control over consumer topic creation behavior
   - Enhanced consistency in topic management and cleanup processes
   - Improved handling of topic creation inconsistencies during consumer operations

8. **Enhanced Manual Commit Testing**:
   - Consolidated manual commit tests for better reliability and coverage
   - Added comprehensive integration tests with PostRebalance event handling
   - Tests now verify offset persistence across consumer restarts and batch processing

### Previous Version (1.8.0) Breaking Changes:
9. **Async Consumer Commit**:
   - **BREAKING**: The `consumer.commit()` method is now async and must be awaited
   - **Before**: `consumer.commit(topic, partition, offset, 'Sync')`
   - **After**: `await consumer.commit(topic, partition, offset, 'Sync')`
   - This change improves performance by using `spawn_blocking` for non-blocking operations

---

## Features

- 🦀 Simple and intuitive API
- 🚀 High-performance message processing
- 🔄 Automatic reconnection handling
- 🎯 Type-safe interfaces (TypeScript support)
- ⚡ Async/await support
- 🛠️ Configurable consumer and producer options
- 📊 Stream processing support with configurable stream options
- 📦 Message batching capabilities
- 🔍 Comprehensive error handling
- 📈 Performance benchmarking suite
- 🔧 Flexible configuration system supporting complex data types

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Consumer Examples](#basic-consumer-setup)
4. [Producer Examples](#producer-examples)
5. [Stream Processing](#stream-processing)
6. [Configuration](#configuration)
7. [Performance Benchmarks](#performance-benchmarks)
8. [Best Practices](#best-practices)
9. [Contributing](#contributing)
10. [License](#license)

## Installation

```bash
npm install kafka-crab-js
# or
yarn add kafka-crab-js
# or
pnpm add kafka-crab-js
```

## Quick Start

### Basic Consumer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';
async function run() {
  const kafkaClient = new KafkaClient({
    brokers: 'localhost:29092',
    clientId: 'foo-client',
    logLevel: 'debug',
    brokerAddressFamily: 'v4',
  });

  // Create consumer with topic creation control
  const consumer = kafkaClient.createConsumer({
    groupId: 'foo-group',
    configuration: {
      'auto.offset.reset': 'earliest',
      'enable.auto.commit': false, // Use manual commit for better control
    }
  });

  // Subscribe with topic creation options
  await consumer.subscribe([{ 
    topic: 'foo',
    createTopic: true,
    numPartitions: 3
  }]);

  const message = await consumer.recv();
  const { payload, partition, offset, topic } = message;
  console.log({
    topic,
    partition,
    offset,
    value: payload.toString()
  });

  // Manual commit (v1.8.0+: now requires await)
  await consumer.commit(topic, partition, offset + 1, 'Sync');

  consumer.unsubscribe();
}

await run();
```

### Basic Producer Setup

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

// Producer configuration is now optional with sensible defaults
const producer = kafkaClient.createProducer({ 
  configuration: { 
    'message.timeout.ms': 5000,  // Now supports number values
    'batch.size': 16384,
    'compression.type': 'snappy'
  } 
});

const message = {
  id: 1,
  name: "Sample Message",
  timestamp: new Date().toISOString()
};

const result = await producer.send({
  topic: 'my-topic',
  messages: [{
    payload: Buffer.from(JSON.stringify(message))
  }]
});

const errors = result.map(r => r.error).filter(Boolean);
if (errors.length > 0) {
  console.error('Error sending message:', errors);
} else {
  console.log('Message sent. Offset:', result);
}
```

## Stream Processing

### Enhanced Stream Consumer Example

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

// Stream consumer with custom ReadableOptions (v1.10.0+)
const kafkaStream = kafkaClient.createStreamConsumer({
  groupId: `my-group-id`,
  enableAutoCommit: true,
}, {
  objectMode: true,  // Default in v1.10.0+
  highWaterMark: 1024,
  encoding: null
});

await kafkaStream.subscribe([
  { topic: 'foo', createTopic: true }, 
  { topic: 'bar', createTopic: true }
]);

kafkaStream.on('data', (message) => {
  console.log('>>> Message received:', { 
    payload: message.payload.toString(), 
    offset: message.offset, 
    partition: message.partition, 
    topic: message.topic 
  });
  
  if (message.offset > 10) {
    kafkaStream.destroy();
  }
});

kafkaStream.on('close', () => {
  kafkaStream.unsubscribe();
  console.log('Stream ended');
});
```

## Producer Examples

### Batch Message Production

```javascript
const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  brokerAddressFamily: 'v4',
});

// Enhanced producer with flexible configuration
const producer = kafkaClient.createProducer({
  configuration: {
    'batch.size': 50000,      // Number value supported
    'linger.ms': 10,          // Number value supported  
    'compression.type': 'lz4',
    'enable.idempotence': true  // Boolean value supported
  }
});

const messages = Array.from({ length: 100 }, (_, i) => ({
  payload: Buffer.from(JSON.stringify({
    _id: i,
    name: `Batch Message ${i}`,
    timestamp: new Date().toISOString()
  }))
}));

try {
  const result = await producer.send({
    topic: 'my-topic',
    messages
  });
  console.log('Batch sent. Offset:', result);
  console.assert(result.length === 100);
} catch (error) {
  console.error('Batch error:', error);
}
```

### Producer with Keys and Headers

```javascript
async function produceWithMetadata() {
  const producer = kafkaClient.createProducer({
    configuration: {
      'acks': 'all',
      'retries': 5,
      'max.in.flight.requests.per.connection': 1
    }
  });

  try {
    await producer.send({
      topic: 'user-events',
      messages: [{
        key: 'user-123',
        payload: Buffer.from(JSON.stringify({
          userId: 123,
          action: 'update'
        })),
        headers: {
          'correlation-id': 'txn-123',
          'source': 'user-service'
        }
      }]
    });
  } catch (error) {
    console.error('Error:', error);
  }
}
```

### Reconnecting Kafka Consumer

```javascript
import { KafkaClient } from 'kafka-crab-js'

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'reconnect-test',
  logLevel: 'debug',
  brokerAddressFamily: 'v4',
  configuration: {
    'auto.offset.reset': 'earliest',
    'session.timeout.ms': 30000,
    'heartbeat.interval.ms': 10000
  },
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: 'reconnect-test',
    enableAutoCommit: true,
  }, {
    highWaterMark: 100,
    objectMode: true
  })
  
  await kafkaStream.subscribe([
    { topic: 'foo', createTopic: true },
    { topic: 'bar', createTopic: true },
  ])
  return kafkaStream
}

/**
 * Starts a Kafka consumer with auto-restart capability
 */
async function startConsumer() {
  let counter = 0
  let retryCount = 0
  const MAX_RETRIES = 5
  const RETRY_DELAY = 5000 // 5 seconds

  async function handleRetry() {
    if (retryCount < MAX_RETRIES) {
      retryCount++
      console.log(
        `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`,
      )
      setTimeout(setupConsumerWithRetry, RETRY_DELAY)
    } else {
      console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`)
      process.exit(1)
    }
  }

  async function setupConsumerWithRetry() {
    try {
      const kafkaStream = await createConsumer()
      retryCount = 0 // Reset retry count on successful connection

      console.log('Starting consumer')

      kafkaStream.on('data', (message) => {
        counter++
        console.log('>>> Message received:', {
          counter,
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        })
      })

      kafkaStream.on('error', async (error) => {
        console.error('Stream error:', error)
        handleRetry()
      })

      kafkaStream.on('close', () => {
        console.log('Stream ended')
        try {
          kafkaStream.unsubscribe()
        } catch (unsubError) {
          console.error('Error unsubscribing:', unsubError)
        }
      })
    } catch (error) {
      console.error('Error setting up consumer:', error)
      handleRetry()
    }
  }

  await setupConsumerWithRetry()
}

await startConsumer()
```

### Examples

You can find some examples on the [example](https://github.com/flash-tecnologia/kafka-crab-js/tree/main/example) folder of this project.

## Configuration

### Configuration properties

### KafkaConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `brokers` | `string` || List of brokers to connect to |
| `clientId` | `string` || Client id to use for the connection |
| `securityProtocol` | `SecurityProtocol` || Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `logLevel` | `string` | `info`  | Log level for the client |
| `brokerAddressFamily` | `string` | `"v4"` | Address family to use for the connection (v4, v6) |
| `configuration` | `Record<string, any>` | `{}` | Additional configuration options for the client. **v1.10.0+**: Now supports any value type (string, number, boolean, object) |

### ConsumerConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `groupId` | `string` || Consumer group ID |
| `createTopic` | `boolean` | `false` | **v1.10.0+**: Whether to create topic if it doesn't exist |
| `enableAutoCommit` | `boolean` | `true` | Enable automatic offset commits |
| `configuration` | `Record<string, any>` | `{}` | Additional consumer configuration options |
| `fetchMetadataTimeout` | `number` | `60000` | Timeout for fetching metadata (ms) |
| `maxBatchMessages` | `number` | `1000` | Maximum messages in a batch operation |

### ProducerConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `queueTimeout` | `number` | `5000` | Queue timeout in milliseconds |
| `autoFlush` | `boolean` | `true` | Enable automatic message flushing |
| `configuration` | `Record<string, any>` | `{}` | Additional producer configuration options |

### TopicPartitionConfig

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `topic` | `string` || Topic name |
| `allOffsets` | `OffsetModel` || Offset configuration for all partitions |
| `partitionOffset` | `Array<PartitionOffset>` || Per-partition offset configuration |
| `createTopic` | `boolean` | `false` | **v1.10.0+**: Create topic if it doesn't exist |
| `numPartitions` | `number` | `1` | **v1.10.0+**: Number of partitions when creating topic |

You can see the available options here: [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).

## Performance Benchmarks

### Running Benchmarks

kafka-crab-js v1.10.0+ includes a comprehensive benchmark suite to compare performance against other popular Kafka clients:

```bash
# Set up benchmark environment (requires Kafka running locally)
pnpm add -D tsx  # For running TypeScript files directly
npx tsx benchmark/utils/setup.ts

# Run consumer performance benchmarks
node benchmark/consumer.ts
```

### Benchmark Results

*Benchmarks run on macOS with Apple M1 chip processing 50,000 messages*

```
╔════════════════════════╤═════════╤══════════════════╤═══════════╤══════════════════════════╗
║ Slower tests           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafkajs                │   50000 │   2566.33 op/sec │ ±  0.89 % │                          ║
║ kafka-crab-js (serial) │   50000 │  35773.21 op/sec │ ±  2.92 % │ + 1293.94 %              ║
║ node-rdkafka (stream)  │   50000 │  48405.52 op/sec │ ± 17.73 % │ + 35.31 %                ║
║ node-rdkafka (evented) │  112979 │  93454.86 op/sec │ ± 74.43 % │ + 93.07 %                ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ Fastest test           │ Samples │           Result │ Tolerance │ Difference with previous ║
╟────────────────────────┼─────────┼──────────────────┼───────────┼──────────────────────────╢
║ kafka-crab-js (batch)  │   50000 │ 135533.29 op/sec │ ± 14.75 % │ + 45.03 %                ║
╚════════════════════════╧═════════╧══════════════════╧═══════════╧══════════════════════════╝
```

The benchmark suite compares:
- **kafka-crab-js (serial)**: Single message processing - **35,773 ops/sec**
- **kafka-crab-js (batch)**: Batch message processing - **135,533 ops/sec** (fastest)
- **node-rdkafka (evented)**: Event-based processing - **93,455 ops/sec**
- **node-rdkafka (stream)**: Stream-based processing - **48,406 ops/sec**
- **kafkajs**: Official KafkaJS client - **2,566 ops/sec**

Performance characteristics:
- **13x faster than kafkajs** in serial mode, **53x faster in batch mode**
- **High throughput**: Batch processing provides 3.8x performance improvement over serial mode
- **Low latency**: Optimized for both single and batch message processing
- **Memory efficient**: Lock-free data structures minimize memory overhead
- **Concurrent processing**: Zero-contention concurrent operations

### Benchmark Configuration

You can customize benchmark parameters in `benchmark/utils/definitions.ts`:

```typescript
export const topic = 'benchmarks'
export const brokers = ['localhost:9092', 'localhost:9093', 'localhost:9094']

// Benchmark parameters can be adjusted in consumer.ts:
const iterations = 10_000  // Number of messages to process
const maxBytes = 200       // Maximum message size
```

## Best Practices

### Error Handling
- Always wrap async operations in try-catch blocks
- Implement proper error logging and monitoring
- Handle both operational and programming errors separately

### Performance
- Use batch operations for high-throughput scenarios
- Configure appropriate batch sizes and compression settings
- Monitor and tune consumer group performance
- Leverage the benchmark suite to optimize your specific use case

### Configuration (v1.10.0+)
- Use the flexible configuration system with proper data types:
  ```javascript
  const config = {
    'batch.size': 16384,           // number
    'compression.type': 'snappy',  // string  
    'enable.idempotence': true,    // boolean
    'retries': 5                   // number
  }
  ```

### Message Processing
- Validate message formats before processing
- Implement proper serialization/deserialization
- Handle message ordering when required
- Use topic creation options for better topic management

### Stream Processing (v1.10.0+)
- Configure appropriate `ReadableOptions` for your use case
- Use `objectMode: true` for structured message processing
- Set appropriate `highWaterMark` based on memory constraints

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run the benchmark suite to ensure performance isn't degraded
4. Commit your changes (`git commit -m 'Add some amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Development Commands

```bash
# Build the project
pnpm build

# Run type checking
pnpm typecheck

# Run linting
pnpm lint

# Format code
pnpm fmt

# Run benchmarks
npx tsx benchmark/utils/setup.ts
npx tsx benchmark/consumer.ts
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

[![Built with Rust](https://img.shields.io/badge/Built%20with-Rust-orange)](https://www.rust-lang.org/)

</div>