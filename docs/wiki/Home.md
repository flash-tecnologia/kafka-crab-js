# Kafka Crab JS Documentation

A Node.js native binding for Apache Kafka using Rust, providing high performance and type safety.

## Installation

```bash
pnpm install kafka-crab-js
```

## Basic Usage

### Creating a Kafka Client

```typescript
import { KafkaClient } from 'kafka-crab-js';

const kafkaClient = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'my-app',
  logLevel: 'info',
  brokerAddressFamily: 'v4',
  // Optional additional configuration
  configuration: {
    'auto.offset.reset': 'earliest',
  },
  otel: {
    serviceName: 'my-app-service',
  },
});
```

### Producer Example

```typescript
async function produceMessages() {
  const producer = kafkaClient.createProducer({
    topic: 'my-topic',
    configuration: {
      'message.timeout.ms': '5000'
    }
  });

  try {
    const result = await producer.send({
      topic: 'my-topic',
      messages: [{
        key: Buffer.from('message-key'),
        headers: { 'correlation-id': Buffer.from('correlation-123') },
        payload: Buffer.from(JSON.stringify({ id: 1, name: 'Test Message' })),
      }],
    });

    console.log('Message sent. Offset:', result);
  } catch (error) {
    console.error('Error sending message', error);
  }
}
```

### Basic Consumer Example

```typescript
async function consumeMessages() {
  const consumer = kafkaClient.createConsumer({
    topic: 'my-topic',
    groupId: 'my-group-id',
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  });

  await consumer.subscribe('my-topic');

  try {
    while (true) {
      const message = await consumer.recv();
      if (!message) {
        console.log('Consumer disconnected');
        break;
      }

      console.log('Received message:', {
        payload: message.payload.toString(),
        partition: message.partition,
        offset: message.offset,
        headers: Object.entries(message.headers)
          .map(([k, v]) => ({ [k]: v.toString() })),
      });
    }
  } finally {
    await consumer.disconnect();
  }
}
```

### Stream Consumer Example

```typescript
async function streamConsumer() {
  const kafkaStream = kafkaClient.createStreamConsumer({
    groupId: 'my-stream-group',
    enableAutoCommit: true,
    configuration: {
      'auto.offset.reset': 'earliest',
    },
  });

  await kafkaStream.subscribe([
    { topic: 'my-topic' },
    // Or for specific offsets:
    // { topic: 'my-topic', allOffsets: { position: 'Beginning' } }
  ]);

  kafkaStream.on('data', (message) => {
    console.log('Message received:', {
      payload: message.payload.toString(),
      offset: message.offset,
      partition: message.partition,
      topic: message.topic,
    });
  });

  kafkaStream.on('error', (error) => {
    console.error('Stream error:', error);
  });

  kafkaStream.on('close', () => {
    console.log('Stream ended');
    kafkaStream.unsubscribe();
  });
}

### Enabling OpenTelemetry Instrumentation

Kafka Crab JS provides built-in OpenTelemetry support for producers, consumers, and stream consumers.

```typescript
import { NodeTracerProvider } from '@opentelemetry/sdk-trace-node'
import { SimpleSpanProcessor } from '@opentelemetry/sdk-trace-base'
import { AsyncHooksContextManager } from '@opentelemetry/context-async-hooks'
import { context } from '@opentelemetry/api'

const provider = new NodeTracerProvider()
provider.addSpanProcessor(new SimpleSpanProcessor(exporter))
provider.register()

context.setGlobalContextManager(new AsyncHooksContextManager().enable())

const client = new KafkaClient({
  brokers: 'localhost:9092',
  clientId: 'otel-example',
  otel: {
    serviceName: 'otel-example-service',
    messageHook: (span, message) => {
      span.setAttribute('app.message.topic', message.topic)
    },
  },
})

const producer = client.createProducer()
await producer.send({
  topic: 'otel-topic',
  messages: [{
    payload: Buffer.from('hello world'),
    headers: { 'custom-header': 'value' },
  }],
})

const consumer = client.createConsumer({ groupId: 'otel-group', enableAutoCommit: false })
await consumer.subscribe([{ topic: 'otel-topic', allOffsets: { position: 'Beginning' } }])
const message = await consumer.recv()

console.log(message.headers?.['custom-header']?.toString()) // -> value
```

Instrumentation automatically injects trace context (`traceparent`/`tracestate`) while preserving custom headers (Buffer/string). Set `otel: false` if you need to turn tracing off.
```

### Consumer with Events Handling

```typescript
function consumerWithEvents() {
  const consumer = kafkaClient.createConsumer({
    topic: 'my-topic',
    groupId: 'my-group-id',
  });

  consumer.onEvents((err, event) => {
    if (err) {
      console.error('Event error:', err);
      return;
    }

    switch (event.name) {
      case 'CommitCallback': {
        const offsetCommitted = event.payload.tpl
          .filter(it => it.partitionOffset.find(it => it.offset.offset))
          .flatMap(p => p.partitionOffset.map(it => ({
            topic: p.topic,
            partition: it.partition,
            offset: it.offset.offset
          })));
        console.log('Offset committed:', offsetCommitted);
        break;
      }
      default: {
        console.log(
          'Event:',
          event.name,
          event.payload.tpl.map(it =>
            `Topic: ${it.topic}, ${it.partitionOffset.map(po =>
              `partition: ${po.partition}`).join(',')}`
          ),
        );
      }
    }
  });

  consumer.subscribe('my-topic');

  // Continue with message consumption...
}
```

### Consumer with Retry Logic

```typescript
async function consumerWithRetry() {
  const MAX_RETRIES = 5;
  const RETRY_DELAY = 5000; // 5 seconds
  let retryCount = 0;

  async function createConsumer() {
    const kafkaStream = kafkaClient.createStreamConsumer({
      groupId: 'retry-example-group',
      enableAutoCommit: true,
      configuration: {
        'auto.offset.reset': 'earliest',
      },
    });

    await kafkaStream.subscribe([
      { topic: 'my-topic' },
    ]);

    return kafkaStream;
  }

  async function handleRetry() {
    if (retryCount < MAX_RETRIES) {
      retryCount++;
      console.log(
        `Attempting to restart consumer (attempt ${retryCount}/${MAX_RETRIES}) in ${RETRY_DELAY / 1000} seconds...`
      );
      setTimeout(setupConsumerWithRetry, RETRY_DELAY);
    } else {
      console.error(`Maximum retry attempts (${MAX_RETRIES}) reached. Stopping consumer.`);
      process.exit(1);
    }
  }

  async function setupConsumerWithRetry() {
    try {
      const kafkaStream = await createConsumer();
      retryCount = 0; // Reset retry count on successful connection

      kafkaStream.on('data', (message) => {
        console.log('Message received:', {
          payload: message.payload.toString(),
          offset: message.offset,
          partition: message.partition,
          topic: message.topic,
        });
      });

      kafkaStream.on('error', (error) => {
        console.error('Stream error:', error);
        handleRetry();
      });

      kafkaStream.on('close', () => {
        console.log('Stream ended');
        try {
          kafkaStream.unsubscribe();
        } catch (unsubError) {
          console.error('Error unsubscribing:', unsubError);
        }
      });
    } catch (error) {
      console.error('Error setting up consumer:', error);
      handleRetry();
    }
  }

  await setupConsumerWithRetry();
}
```

## API Reference

### KafkaClient

Main client class that creates producers and consumers.

#### Constructor
```typescript
new KafkaClient(kafkaConfiguration: KafkaConfiguration)
```

#### Configuration Options
```typescript
interface KafkaConfiguration {
  brokers: string;                // Comma-separated list of brokers
  clientId?: string;              // Client identifier
  logLevel?: 'debug' | 'info' | 'warning' | 'error';  // Default: 'info'
  brokerAddressFamily?: 'v4' | 'v6'; // IP version, default: 'v4'
  securityProtocol?: string;      // Default: 'Plaintext'
  configuration?: {               // Additional librdkafka configuration
    [key: string]: string;
  }
}
```

#### Methods

- **createProducer(config: ProducerConfiguration): KafkaProducer**
  Creates a Kafka producer

- **createConsumer(config: ConsumerConfiguration): KafkaConsumer**
  Creates a Kafka consumer

- **createStreamConsumer(config: ConsumerConfiguration): KafkaStreamReadable**
  Creates a stream-based Kafka consumer that implements Node.js Readable interface

### KafkaProducer

#### Configuration
```typescript
interface ProducerConfiguration {
  topic?: string;             // Default topic (optional)
  configuration?: {           // Additional producer configuration
    [key: string]: string;
  }
}
```

#### Methods

- **send(record: ProducerRecord): Promise<RecordMetadata[]>**
  Sends messages to Kafka and returns offset information

```typescript
interface ProducerRecord {
  topic: string;
  messages: Array<{
    key?: Buffer;
    payload: Buffer;
    headers?: Record<string, Buffer>;
  }>;
}
```

### KafkaConsumer

#### Configuration
```typescript
interface ConsumerConfiguration {
  topic?: string;             // Topic to consume (can also be set with subscribe())
  groupId: string;            // Consumer group ID
  enableAutoCommit?: boolean; // Whether to auto-commit offsets
  configuration?: {           // Additional consumer configuration
    'auto.offset.reset'?: 'earliest' | 'latest';
    [key: string]: string;
  }
}
```

#### Methods

- **subscribe(topics: string | TopicPartitionConfig[]): Promise<void>**
  Subscribe to Kafka topics

- **recv(): Promise<Message | null>**
  Receive next message (returns null when disconnected)

- **disconnect(): Promise<void>**
  Disconnect the consumer

- **onEvents(callback: (error?: Error, event?: KafkaEvent) => void): void**
  Register callback for consumer events

- **seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number): void**
  Seek to a specific offset

- **commit(topic: string, partition: number, offset: number, commit: CommitMode): void**
  Manually commit an offset

### KafkaStreamReadable

Extends Node.js Readable stream interface for Kafka consumption.

#### Methods

- **subscribe(topics: string | TopicPartitionConfig[]): Promise<void>**
  Subscribe to Kafka topics

- **seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number): void**
  Seek to a specific offset

- **commit(topic: string, partition: number, offset: number, commit: CommitMode): void**
  Manually commit an offset

- **unsubscribe(): void**
  Unsubscribe from topics

- **disconnect(): Promise<void>**
  Disconnect the consumer

- **rawConsumer(): KafkaConsumer**
  Get the underlying KafkaConsumer instance

#### Events

- **data**: Emitted for each Kafka message
- **error**: Emitted on errors
- **close**: Emitted when the stream ends

### Message Format

```typescript
interface Message {
  topic: string;
  partition: number;
  offset: number;
  timestamp: number;
  payload: Buffer;
  key?: Buffer;
  headers: Record<string, Buffer>;
}
```

## Best Practices

1. **Resource Management**
   - Always call `disconnect()` when done with a consumer
   - Use try/finally blocks to ensure proper cleanup
   - Handle process signals (SIGINT, etc.) to gracefully shut down

2. **Error Handling**
   - Implement proper error handling and retry mechanisms
   - For critical applications, consider using the retry pattern shown above
   - Check for null returns from `recv()` to detect disconnections

3. **Performance Tuning**
   - For high-throughput applications, use the stream-based consumer
   - Configure batch sizes and commit intervals appropriately
   - Monitor memory usage, especially when processing large messages

4. **Offset Management**
   - Use `enableAutoCommit: true` for simple use cases
   - For more control, set to false and manually commit offsets
   - Be careful about commit frequency - too frequent commits can impact performance

## Common Issues and Solutions

1. **Connection Issues**
   - Verify broker addresses and network connectivity
   - Check security settings if using TLS/SSL
   - Ensure the specified topics exist

2. **Performance Issues**
   - Increase batch size for better throughput
   - Adjust commit frequency
   - Consider using stream-based consumption for higher throughput

3. **Message Ordering**
   - Remember that ordering is only guaranteed within a partition
   - Use appropriate partition strategies when producing messages that need ordering

4. **Resource Leaks**
   - Always disconnect consumers and producers when done
   - Handle process termination signals properly
