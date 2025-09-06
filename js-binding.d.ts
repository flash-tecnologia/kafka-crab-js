export declare class KafkaClientConfig {
  constructor(kafkaConfiguration: KafkaConfiguration)
  get configuration(): KafkaConfiguration
  createProducer(producerConfiguration: ProducerConfiguration): KafkaProducer
  createConsumer(consumerConfiguration: ConsumerConfiguration): KafkaConsumer
}

export declare class KafkaConsumer {
  getConfig(): ConsumerConfiguration
  getSubscription(): Array<TopicPartition>
  onEvents(callback: (error: Error | undefined, event: KafkaEvent) => void): void
  subscribe(topicConfigs: string | Array<TopicPartitionConfig>): Promise<void>
  pause(): void
  resume(): void
  unsubscribe(): void
  disconnect(): Promise<void>
  seek(topic: string, partition: number, offsetModel: OffsetModel, timeout?: number | undefined | null): void
  assignment(): Array<TopicPartition>
  recv(): Promise<Message | null>
  /**
   * Receives multiple messages in a single call for higher throughput
   *
   * This method provides 2-5x better performance than calling recv() multiple times
   * by batching message retrieval and reducing function call overhead.
   *
   * @param max_messages Maximum number of messages to retrieve (1-configured max, default 1000)
   * @param timeout_ms Timeout in milliseconds (1-30000)
   * @returns Array of messages (may be fewer than max_messages)
   */
  recvBatch(maxMessages: number, timeoutMs: number): Promise<Array<Message>>
  commit(topic: string, partition: number, offset: number, commit: CommitMode): Promise<void>
}

export declare class KafkaProducer {
  inFlightCount(): number
  flush(): Promise<Array<RecordMetadata>>
  send(producerRecord: ProducerRecord): Promise<Array<RecordMetadata>>
}

export type CommitMode =  'Sync'|
'Async';

export interface ConsumerConfiguration {
  groupId: string
  createTopic?: boolean
  enableAutoCommit?: boolean
  configuration?: Record<string, any>
  fetchMetadataTimeout?: number
  maxBatchMessages?: number
}

export interface KafkaConfiguration {
  brokers: string
  clientId: string
  securityProtocol?: SecurityProtocol
  configuration?: Record<string, any>
  logLevel?: string
  brokerAddressFamily?: string
}

export interface KafkaCrabError {
  code: number
  message: string
}

export interface KafkaEvent {
  name: KafkaEventName
  payload: KafkaEventPayload
}

export type KafkaEventName =  'PreRebalance'|
'PostRebalance'|
'CommitCallback';

export interface KafkaEventPayload {
  action?: string
  tpl: Array<TopicPartition>
  error?: string
}

export interface Message {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
  topic: string
  partition: number
  offset: number
}

export interface MessageProducer {
  payload: Buffer
  key?: Buffer
  headers?: Record<string, Buffer>
}

export interface OffsetModel {
  offset?: number
  position?: PartitionPosition
}

export interface PartitionOffset {
  partition: number
  offset: OffsetModel
}

export type PartitionPosition =  'Beginning'|
'End'|
'Stored'|
'Invalid';

export interface ProducerConfiguration {
  queueTimeout?: number
  autoFlush?: boolean
  configuration?: Record<string, any>
}

export interface ProducerRecord {
  topic: string
  messages: Array<MessageProducer>
}

export interface RecordMetadata {
  topic: string
  partition: number
  offset: number
  error?: KafkaCrabError
}

export type SecurityProtocol =  'Plaintext'|
'Ssl'|
'SaslPlaintext'|
'SaslSsl';

export interface TopicPartition {
  topic: string
  partitionOffset: Array<PartitionOffset>
}

export interface TopicPartitionConfig {
  topic: string
  allOffsets?: OffsetModel
  partitionOffset?: Array<PartitionOffset>
  createTopic?: boolean
  numPartitions?: number
}
