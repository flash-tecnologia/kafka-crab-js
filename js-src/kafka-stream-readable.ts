import { Readable, type ReadableOptions } from 'node:stream'

import type { CommitMode, KafkaConsumer, OffsetModel, TopicPartitionConfig } from '../js-binding.js'

// Constants for batch timeout validation
const DEFAULT_BATCH_TIMEOUT_MS = 100
const DEFAULT_BATCH_SIZE = 10
const MAX_BATCH_TIMEOUT_MS = 30_000

/**
 * KafkaStreamReadable class
 * @extends Readable
 */
export class KafkaStreamReadable extends Readable {
  private useBatchMode = false
  private batchSize = DEFAULT_BATCH_SIZE
  private batchTimeoutMs: number = DEFAULT_BATCH_TIMEOUT_MS

  /**
   * Creates a KafkaStreamReadable instance
   */
  constructor(private readonly kafkaConsumer: KafkaConsumer, opts: ReadableOptions = { objectMode: true }) {
    super(opts)
    if (!kafkaConsumer) {
      throw new Error('A valid KafkaConsumer instance is required.')
    }
  }

  /**
   * Enables batch processing mode for higher throughput
   * @param {number} [batchSize=10] - Number of messages to batch (validated by consumer config)
   * @param {number} [batchTimeoutMs=100] - Batch timeout in milliseconds (1-30000, invalid values use default)
   * @returns {KafkaStreamReadable} This instance for chaining
   */
  enableBatchMode(
    batchSize: number = DEFAULT_BATCH_SIZE,
    batchTimeoutMs: number = DEFAULT_BATCH_TIMEOUT_MS,
  ): KafkaStreamReadable {
    this.batchSize = batchSize

    // Use fallback validation like Rust layer
    if (batchTimeoutMs >= 1 && batchTimeoutMs <= MAX_BATCH_TIMEOUT_MS) {
      this.batchTimeoutMs = batchTimeoutMs
    } else {
      this.batchTimeoutMs = DEFAULT_BATCH_TIMEOUT_MS // Default fallback value
    }

    this.useBatchMode = true
    return this
  }

  /**
   * Disables batch processing mode (returns to single message mode)
   * @returns {KafkaStreamReadable} This instance for chaining
   */
  disableBatchMode(): KafkaStreamReadable {
    this.useBatchMode = false
    return this
  }

  /**
   * Checks if batch mode is currently enabled
   * @returns {boolean} True if batch mode is enabled
   */
  isBatchModeEnabled(): boolean {
    return this.useBatchMode
  }

  /**
   * Gets current batch configuration
   * @returns {object} Current batch settings
   */
  getBatchConfig(): { enabled: boolean; batchSize: number; batchTimeoutMs: number } {
    return {
      enabled: this.useBatchMode,
      batchSize: this.batchSize,
      batchTimeoutMs: this.batchTimeoutMs,
    }
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
   * Internal method called by the Readable stream to fetch data
   * @private
   */
  async _read() {
    if (this.useBatchMode) {
      await this._readBatch()
    } else {
      await this._readSingle()
    }
  }

  /**
   * Single message processing (original behavior)
   * @private
   */
  private async _readSingle() {
    try {
      const message = await this.kafkaConsumer.recv()
      if (message) {
        this.push(message)
      } else {
        this.push(null) // No more data, end of stream
      }
    } catch (error) {
      if (error instanceof Error) {
        this.destroy(error)
      } else {
        this.destroy()
      }
    }
  }

  /**
   * Batch message processing (high-performance mode)
   * @private
   */
  private async _readBatch() {
    try {
      const messages = await this.kafkaConsumer.recvBatch(this.batchSize, this.batchTimeoutMs)

      if (messages.length === 0) {
        this.push(null) // No more data, end of stream
        return
      }

      // Push all messages from the batch
      for (const message of messages) {
        // Check if the stream can accept more data (backpressure handling)
        if (!this.push(message)) {
          // Stream is full, stop pushing for now
          // The next _read() call will continue processing
          break
        }
      }
    } catch {
      // Fallback to single message recv if batch fails
      await this._readSingle()
    }
  }
}
