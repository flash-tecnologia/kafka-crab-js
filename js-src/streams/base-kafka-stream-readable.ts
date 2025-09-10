import { Readable, type ReadableOptions } from 'node:stream'

import type { CommitMode, KafkaConsumer, OffsetModel, TopicPartitionConfig } from '../../js-binding.js'

export interface KafkaStreamReadableOptions extends ReadableOptions<Readable> {
  kafkaConsumer: KafkaConsumer
}

/**
 * Abstract base class for Kafka stream readers
 * @extends Readable
 */
export abstract class BaseKafkaStreamReadable extends Readable {
  private _kafkaConsumer: KafkaConsumer

  /**
   * Creates a BaseKafkaStreamReadable instance
   */
  constructor(
    streamOptions: KafkaStreamReadableOptions,
  ) {
    const { kafkaConsumer, ...opts } = streamOptions

    super(opts)

    if (!kafkaConsumer) {
      throw new Error('A valid KafkaConsumer instance is required.')
    }

    this._kafkaConsumer = kafkaConsumer
  }

  get kafkaConsumer(): KafkaConsumer {
    return this._kafkaConsumer
  }

  /**
   * Checks if the stream is currently paused
   * @returns {boolean} True if the stream is paused
   */
  isPaused(): boolean {
    return this.readableFlowing === false
  }

  /**
   * Subscribes to topics
   */
  async subscribe(topics: string | Array<TopicPartitionConfig>) {
    if (!topics || (Array.isArray(topics) && topics.length === 0)) {
      throw new Error('Topics must be a non-empty string or array.')
    }
    await this.kafkaConsumer.subscribe(topics)
  }

  seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number) {
    this.kafkaConsumer.seek(topic, partition, offsetModel, timeout)
  }

  async commit(topic: string, partition: number, offset: number, commitMode: CommitMode) {
    return this.kafkaConsumer.commit(topic, partition, offset, commitMode)
  }

  /**
   * Unsubscribe from topics
   */
  unsubscribe() {
    this.kafkaConsumer.unsubscribe()
  }

  /**
   * Disconnects the Kafka consumer
   */
  async disconnect() {
    await this.kafkaConsumer.disconnect()
  }

  /**
   * Returns the raw Kafka consumer
   * @returns {KafkaConsumer} The Kafka consumer instance
   */
  rawConsumer() {
    return this.kafkaConsumer
  }

  /**
   * Called when the stream is being destroyed
   * Ensures proper cleanup of the Kafka consumer
   * @private
   */
  _destroy(error: Error | null, callback: (error?: Error | null) => void): void {
    // Always unsubscribe first to stop receiving messages
    try {
      this.kafkaConsumer.unsubscribe()
    } catch {
      // Silently ignore unsubscribe errors during cleanup - they're non-critical
      // Common when consumer is already disconnected or in invalid state
    }

    // Disconnect the consumer
    this.kafkaConsumer
      .disconnect()
      .then(() => {
        // Success: pass through the original error (if any)
        callback(error)
      })
      // eslint-disable-next-line unicorn/catch-error-name
      .catch((disconnectError) => {
        // Failed to disconnect: combine errors
        const combinedError = error
          ? new Error(`Stream error: ${error.message}; Disconnect error: ${disconnectError.message}`)
          : disconnectError
        callback(combinedError)
      })
  }

  /**
   * Called when the stream is finishing (no more data will be written)
   * Ensures proper cleanup when stream ends normally
   * @private
   */
  _final(callback: (error?: Error | null) => void): void {
    // For readable streams, _final is called when we push(null)
    // Ensure consumer is properly cleaned up
    this.kafkaConsumer
      .disconnect()
      .then(() => callback())
      .catch((error) => callback(error))
  }

  /**
   * Abstract method that must be implemented by concrete classes
   * @private
   */
  abstract _read(): void
}
