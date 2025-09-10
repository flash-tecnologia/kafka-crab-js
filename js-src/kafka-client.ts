import type { ReadableOptions } from 'node:stream'
import {
  type ConsumerConfiguration,
  KafkaClientConfig,
  type KafkaConfiguration,
  type ProducerConfiguration,
} from '../js-binding.js'

import { KafkaBatchStreamReadable } from './streams/kafka-batch-stream-readable.js'
import { KafkaStreamReadable } from './streams/kafka-stream-readable.js'

export interface StreamConsumerConfiguration extends ConsumerConfiguration {
  batchSize?: number // Default 1 (single mode), > 1 enables batch mode
  batchTimeout?: number // Default 100ms, only used when batchSize > 1
  streamOptions?: ReadableOptions
}

/**
 * KafkaClient class
 */
export class KafkaClient {
  private readonly kafkaClientConfig: KafkaClientConfig
  /**
   * Creates a KafkaClient instance
   * @throws {Error} If the configuration is invalid
   */
  constructor(private readonly kafkaConfiguration: KafkaConfiguration) {
    this.kafkaClientConfig = new KafkaClientConfig(this.kafkaConfiguration)
  }

  /**
   * Creates a KafkaProducer instance
   * @param {ProducerConfiguration} [producerConfiguration] - Optional producer configuration
   * @returns {KafkaProducer} A KafkaProducer instance
   */
  createProducer(producerConfiguration?: ProducerConfiguration) {
    if (producerConfiguration) {
      return this.kafkaClientConfig.createProducer(producerConfiguration)
    }
    return this.kafkaClientConfig.createProducer({})
  }

  /**
   * Creates a KafkaConsumer instance
   * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
   * @returns {KafkaConsumer} A KafkaConsumer instance
   * @throws {Error} If the configuration is invalid
   */
  createConsumer(consumerConfiguration: ConsumerConfiguration) {
    return this.kafkaClientConfig.createConsumer(consumerConfiguration)
  }

  /**
   * Creates a stream consumer instance
   * @param {StreamConsumerConfiguration} streamConfiguration - Stream consumer configuration including batch mode and stream options
   * @returns {KafkaStreamReadable | KafkaBatchStreamReadable} A stream consumer instance
   * @throws {Error} If the configuration is invalid
   */
  createStreamConsumer(
    streamConfiguration: StreamConsumerConfiguration,
  ): KafkaStreamReadable | KafkaBatchStreamReadable {
    const { batchSize, batchTimeout, streamOptions, ...consumerConfiguration } = streamConfiguration
    const kafkaConsumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)
    const opts = streamOptions ?? { objectMode: true }

    // Return appropriate class based on batch configuration
    if (batchSize && batchSize > 1) {
      return new KafkaBatchStreamReadable({ kafkaConsumer, batchSize, batchTimeout, ...opts })
    }

    return new KafkaStreamReadable({ kafkaConsumer, ...opts })
  }
}
