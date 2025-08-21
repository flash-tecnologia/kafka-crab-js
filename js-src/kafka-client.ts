import {
  type ConsumerConfiguration,
  KafkaClientConfig,
  type KafkaConfiguration,
  type ProducerConfiguration,
} from '../js-binding.js'

import { KafkaStreamReadable } from './kafka-stream-readable.js'

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
   * Creates a KafkaStreamReadable instance
   * @param {ConsumerConfiguration} consumerConfiguration - Consumer configuration
   * @returns {KafkaStreamReadable} A KafkaStreamReadable instance
   * @throws {Error} If the configuration is invalid
   */
  createStreamConsumer(consumerConfiguration: ConsumerConfiguration) {
    const consumer = this.kafkaClientConfig.createConsumer(consumerConfiguration)
    return new KafkaStreamReadable(consumer)
  }
}
