<div align="center">

# 🦀 Kafka Crab JS 🦀

A lightweight, flexible, and reliable Kafka client for JavaScript/TypeScript. It is built using Rust and librdkafka, providing a high-performance and feature-rich Kafka client.

[![npm version](https://img.shields.io/npm/v/kafka-crab-js.svg)](https://www.npmjs.com/package/kafka-crab-js)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
</div>

---

## What's New in Version 1.8.0

### 🚨 Breaking Changes:
1. **Async Consumer Commit**:
   - **BREAKING**: The `consumer.commit()` method is now async and must be awaited
   - **Before**: `consumer.commit(topic, partition, offset, 'Sync')`
   - **After**: `await consumer.commit(topic, partition, offset, 'Sync')`
   - This change improves performance by using `spawn_blocking` for non-blocking operations

### Major Updates:
2. **Enhanced Manual Commit Testing**:
   - Added comprehensive integration tests for manual commit functionality
   - Tests now use PostRebalance events for proper timing instead of arbitrary timeouts
   - Covers both sync and async commit modes with offset verification
   - Tests offset persistence across consumer restarts and batch processing scenarios

3. **Improved Error Messages**:
   - Standardized all error messages to use consistent "Failed to [action]" format
   - Enhanced error clarity and specificity throughout the codebase
   - Fixed grammar and spelling issues in error messages

4. **Updated Dependencies**:
   - Upgraded all dependencies to their latest versions for improved performance and security
   - Updated TypeScript bindings and CommonJS exports
   - Synchronized pnpm-lock.yaml with new dependency versions

5. **Arc-based Consumer Architecture**:
   - Improved internal architecture using `Arc<StreamConsumer>` for better memory management
   - Enhanced thread safety for commit operations

---

## Features

- 🦀 Simple and intuitive API
- 🚀 High-performance message processing
- 🔄 Automatic reconnection handling
- 🎯 Type-safe interfaces (TypeScript support)
- ⚡ Async/await support
- 🛠️ Configurable consumer and producer options
- 📊 Stream processing support
- 📦 Message batching capabilities
- 🔍 Comprehensive error handling

## Table of Contents

1. [Installation](#installation)
2. [Quick Start](#quick-start)
3. [Consumer Examples](#basic-consumer-setup)
4. [Producer Examples](#producer-examples)
5. [Stream Processing](#stream-processing)
6. [Configuration](#configuration)
7. [Best Practices](#best-practices)
8. [Contributing](#contributing)
9. [License](#license)

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

  // Create consumer
  const consumer = kafkaClient.createConsumer({
    groupId: 'foo-group',
  });

  await consumer.subscribe([{ topic: 'foo' }]);

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

const producer = kafkaClient.createProducer({ configuration: { 'message.timeout.ms': '5000' } });

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

### Stream Consumer Example

```javascript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
});

const kafkaStream = kafkaClient.createStreamConsumer({
  groupId: `my-groud-id`,
  enableAutoCommit: true,
});

await kafkaStream.subscribe([{ topic: 'foo' }, { topic: 'bar' }])

kafkaStream.on('data', (message) => {
  console.log('>>> Message received:', { payload: message.payload.toString(), offset: message.offset, partition: message.partition, topic: message.topic })
  if (message.offset > 10) {
    kafkaStream.destroy();
  }
})

kafkaStream.on('close', () => {
  kafkaStream.unsubscribe();
  console.log('Stream ended')
})
```

## Producer Examples

### Batch Message Production

```javascript
const kafkaClient = new KafkaClient({
  brokers: 'localhost:29092',
  clientId: 'my-client-id',
  brokerAddressFamily: 'v4',
});
const producer = kafkaClient.createProducer({});

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
  const producer = await kafkaCrab.createProducer({ config });

  try {
    await producer.send({
      topic,
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
  },
})

/**
 * Creates and configures a new Kafka stream consumer
 */
async function createConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: 'reconnect-test',
    enableAutoCommit: true,
  })
  await kafkaStream.subscribe([
    { topic: 'foo' },
    { topic: 'bar' },
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

You can find some examples on the [example](https://github.com/mthssdrbrg/kafka-crab-js/tree/main/example) folder of this project.

## Configuration

### Configuration properties

### KafkaConfiguration

| Property | Type | Default | Description |
| --- | --- | --- | --- |
| `brokers` | `string` || List of brokers to connect to |
| `clientId` | `string` || Client id to use for the connection |
| `securityProtocol` | `SecurityProtocol` || Security protocol to use (PLAINTEXT, SSL, SASL_PLAINTEXT, SASL_SSL) |
| `logLevel` | `string` | `info`  | Client id to use for the connection |
| `brokerAddressFamily` | `string` | `"v4"` | Address family to use for the connection (v4, v6) |
| `configuration` | `Map<string, string>` | `{}` | Additional configuration options for the client. See [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html) |

You can see the available options here: [librdkafka](https://docs.confluent.io/platform/current/clients/librdkafka/html/md_CONFIGURATION.html).



## Best Practices

### Error Handling
- Always wrap async operations in try-catch blocks
- Implement proper error logging and monitoring
- Handle both operational and programming errors separately

### Performance
- Use batch operations for high-throughput scenarios
- Configure appropriate batch sizes and compression
- Monitor and tune consumer group performance

### Message Processing
- Validate message formats before processing
- Implement proper serialization/deserialization
- Handle message ordering when required

## Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

<div align="center">

[![Built with Rust](https://img.shields.io/badge/Built%20with-Rust-orange)](https://www.rust-lang.org/)

</div>
