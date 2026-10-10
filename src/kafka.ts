import { randomUUID } from 'node:crypto'
import kafkaLib from '@confluentinc/kafka-javascript'
import type {
  KafkaConsumer as RdKafkaConsumer,
  KafkaJS as KafkaJSTypes,
  Message,
  Metadata
} from '@confluentinc/kafka-javascript'
import { generateAuthToken } from 'aws-msk-iam-sasl-signer-js'
import type { Config } from './config.js'
import { formatMessage, type FormattedMessage } from './format.js'

const { KafkaJS, KafkaConsumer } = kafkaLib

// Read-only by construction: this module never creates a producer and calls no admin operation that
// changes the cluster, and the consumer it creates assigns partitions directly and never commits, so
// it neither joins nor creates a consumer group. Writes live only in write.ts, loaded only when they
// are enabled. test/read-only.test.ts keeps it that way.

export type Position = 'latest' | 'earliest' | 'offset' | 'timestamp'

export interface ReadRequest {
  topic: string
  partition?: number
  position: Position
  offset?: number
  timestamp?: number
  limit: number
}

interface PartitionRange {
  partition: number
  start: number
  end: number
}

type RdKafkaConfig = Record<string, unknown>

export class KafkaReader {
  private readonly config: Config

  constructor(private readonly clients: KafkaClients) {
    this.config = clients.config
  }

  async describeCluster(): Promise<{ brokers: Metadata['brokers']; topicCount: number }> {
    const metadata = await this.withConsumer(
      (consumer) =>
        new Promise<Metadata>((resolve, reject) =>
          consumer.getMetadata({ allTopics: true, timeout: this.config.timeoutMs }, (err, data) =>
            err ? reject(err) : resolve(data)
          )
        )
    )
    return {
      brokers: [...metadata.brokers].sort((a, b) => a.id - b.id),
      topicCount: metadata.topics.filter((topic) => !topic.name.startsWith('__')).length
    }
  }

  async listTopics(includeInternal: boolean) {
    const admin = await this.clients.admin()
    const topics = await admin.fetchTopicMetadata({ timeout: this.config.timeoutMs })
    return topics
      .filter((topic) => includeInternal || !isInternal(topic))
      .map((topic) => ({
        name: topic.name,
        internal: isInternal(topic),
        partitions: topic.partitions.length,
        replicationFactor: topic.partitions[0]?.replicas.length ?? 0
      }))
      .sort((a, b) => a.name.localeCompare(b.name))
  }

  async describeTopic(topic: string) {
    const admin = await this.clients.admin()
    const [metadata] = await admin.fetchTopicMetadata({
      topics: [topic],
      timeout: this.config.timeoutMs
    })
    if (!metadata) throw new Error(`Topic ${topic} not found`)
    const offsets = await this.watermarks(topic)
    const partitions = metadata.partitions
      .map((partition) => {
        const watermark = offsets.get(partition.partitionId)
        return {
          partition: partition.partitionId,
          leader: partition.leader,
          replicas: partition.replicas,
          isr: partition.isr,
          lowOffset: watermark?.low ?? null,
          highOffset: watermark?.high ?? null,
          messages: watermark ? watermark.high - watermark.low : null
        }
      })
      .sort((a, b) => a.partition - b.partition)
    return {
      name: metadata.name,
      internal: isInternal(metadata),
      partitions,
      totalMessages: partitions.reduce((sum, partition) => sum + (partition.messages ?? 0), 0)
    }
  }

  async listConsumerGroups() {
    const admin = await this.clients.admin()
    const { groups } = await admin.listGroups({ timeout: this.config.timeoutMs })
    return groups
      .map((group) => ({
        groupId: group.groupId,
        state: stateName(group.state),
        protocolType: group.protocolType,
        simple: group.isSimpleConsumerGroup
      }))
      .sort((a, b) => a.groupId.localeCompare(b.groupId))
  }

  async describeConsumerGroup(groupId: string) {
    const admin = await this.clients.admin()
    const { groups } = await admin.describeGroups([groupId], { timeout: this.config.timeoutMs })
    const group = groups[0]
    if (!group || group.error) {
      throw new Error(group?.error?.message ?? `Consumer group ${groupId} not found`)
    }
    const committed = await admin.fetchOffsets({ groupId, timeout: this.config.timeoutMs })
    const topics = await Promise.all(
      committed.map(async ({ topic, partitions }) => {
        const watermarks = await this.watermarks(topic)
        const rows = partitions
          .map((p) => {
            const committedOffset = Number(p.offset)
            const high = watermarks.get(p.partition)?.high ?? null
            const lag = high !== null && committedOffset >= 0 ? high - committedOffset : null
            return {
              partition: p.partition,
              committedOffset: committedOffset >= 0 ? committedOffset : null,
              highOffset: high,
              lag
            }
          })
          .sort((a, b) => a.partition - b.partition)
        return { topic, lag: rows.reduce((sum, row) => sum + (row.lag ?? 0), 0), partitions: rows }
      })
    )
    return {
      groupId: group.groupId,
      state: stateName(group.state),
      protocolType: group.protocolType,
      partitionAssignor: group.partitionAssignor,
      coordinator: group.coordinator,
      members: group.members.map((member) => ({
        memberId: member.memberId,
        clientId: member.clientId,
        clientHost: member.clientHost,
        assignment: member.assignment?.topicPartitions ?? []
      })),
      totalLag: topics.reduce((sum, topic) => sum + topic.lag, 0),
      topics: topics.sort((a, b) => a.topic.localeCompare(b.topic))
    }
  }

  async readMessages(request: ReadRequest) {
    const limit = Math.min(request.limit, this.config.maxMessages)
    const watermarks = await this.watermarks(request.topic)
    if (request.partition !== undefined && !watermarks.has(request.partition)) {
      throw new Error(`Topic ${request.topic} has no partition ${request.partition}`)
    }
    const partitions =
      request.partition !== undefined ? [request.partition] : [...watermarks.keys()].sort()
    const starts = await this.startOffsets(request, partitions, watermarks, limit)
    const ranges: PartitionRange[] = partitions
      .map((partition) => {
        const { low, high } = watermarks.get(partition) as { low: number; high: number }
        const start = clamp(starts.get(partition) ?? high, low, high)
        return { partition, start, end: Math.min(high, start + limit) }
      })
      .filter((range) => range.start < range.end)

    const messages = ranges.length > 0 ? await this.consume(request.topic, ranges) : []
    const chronological = (a: Message, b: Message) =>
      (a.timestamp ?? 0) - (b.timestamp ?? 0) || a.partition - b.partition || a.offset - b.offset
    const ordered =
      request.position === 'latest'
        ? messages.sort((a, b) => chronological(b, a)).slice(0, limit)
        : messages.sort(chronological).slice(0, limit)
    return {
      topic: request.topic,
      position: request.position,
      limit,
      partitions: ranges.map(({ partition, start, end }) => ({ partition, from: start, to: end })),
      messages: ordered.map((message) => formatMessage(message, this.config.maxValueBytes))
    }
  }

  private async startOffsets(
    request: ReadRequest,
    partitions: number[],
    watermarks: Map<number, { low: number; high: number }>,
    limit: number
  ): Promise<Map<number, number>> {
    const starts = new Map<number, number>()
    switch (request.position) {
      case 'earliest':
        for (const p of partitions) starts.set(p, (watermarks.get(p) as { low: number }).low)
        break
      case 'latest':
        for (const p of partitions)
          starts.set(p, (watermarks.get(p) as { high: number }).high - limit)
        break
      case 'offset':
        if (request.partition === undefined || request.offset === undefined) {
          throw new Error('position "offset" needs both partition and offset')
        }
        starts.set(request.partition, request.offset)
        break
      case 'timestamp': {
        if (request.timestamp === undefined) {
          throw new Error('position "timestamp" needs timestamp (epoch milliseconds)')
        }
        const admin = await this.clients.admin()
        const found = await admin.fetchTopicOffsetsByTimestamp(request.topic, request.timestamp, {
          timeout: this.config.timeoutMs,
          isolationLevel: KafkaJS.IsolationLevel.READ_UNCOMMITTED
        })
        for (const entry of found) {
          const offset = Number(entry.offset)
          // -1: no message at or after the timestamp, so nothing to read from that partition.
          const high = watermarks.get(entry.partition)?.high ?? 0
          starts.set(entry.partition, offset >= 0 ? offset : high)
        }
        break
      }
    }
    return starts
  }

  private async consume(topic: string, ranges: PartitionRange[]): Promise<Message[]> {
    return this.withConsumer(async (consumer) => {
      consumer.assign(ranges.map(({ partition, start }) => ({ topic, partition, offset: start })))
      const remaining = new Map(ranges.map((range) => [range.partition, range]))
      const collected: Message[] = []
      const deadline = Date.now() + this.config.timeoutMs
      while (remaining.size > 0 && Date.now() < deadline) {
        const batch = await new Promise<Message[]>((resolve, reject) =>
          consumer.consume(500, (err, messages) => (err ? reject(err) : resolve(messages)))
        )
        for (const message of batch) {
          const range = remaining.get(message.partition)
          if (!range || message.offset < range.start || message.offset >= range.end) continue
          collected.push(message)
          if (message.offset >= range.end - 1) remaining.delete(message.partition)
        }
      }
      return collected
    })
  }

  private async withConsumer<T>(work: (consumer: RdKafkaConsumer) => Promise<T>): Promise<T> {
    const consumer: RdKafkaConsumer = new KafkaConsumer(
      {
        ...this.clients.rdkafkaConfig(),
        // librdkafka requires a group id for any consumer. It is never joined (partitions are
        // assigned directly) and never committed to, so the group is not created on the broker.
        'group.id': `kafka-mcp-${randomUUID()}`,
        'enable.auto.commit': false,
        'enable.auto.offset.store': false,
        'fetch.wait.max.ms': 200
      } as never,
      {}
    )
    await new Promise<void>((resolve, reject) =>
      consumer.connect({ timeout: this.config.timeoutMs }, (err) => (err ? reject(err) : resolve()))
    )
    try {
      return await work(consumer)
    } finally {
      await new Promise<void>((resolve) => consumer.disconnect(() => resolve()))
    }
  }

  private async watermarks(topic: string): Promise<Map<number, { low: number; high: number }>> {
    const admin = await this.clients.admin()
    const offsets = await admin.fetchTopicOffsets(topic, {
      timeout: this.config.timeoutMs,
      isolationLevel: KafkaJS.IsolationLevel.READ_UNCOMMITTED
    })
    return new Map(
      offsets.map((entry) => [
        entry.partition,
        { low: Number(entry.low), high: Number(entry.high) }
      ])
    )
  }
}

/** The connections reader and writer share: one admin client, and the config for the others. */
export class KafkaClients {
  private kafkaClient: KafkaJSTypes.Kafka | undefined
  private adminClient: KafkaJSTypes.Admin | undefined
  private adminConnecting: Promise<KafkaJSTypes.Admin> | undefined

  constructor(readonly config: Config) {}

  kafka(): KafkaJSTypes.Kafka {
    this.kafkaClient ??= new KafkaJS.Kafka({ kafkaJS: this.kafkaJSConfig() })
    return this.kafkaClient
  }

  admin(): Promise<KafkaJSTypes.Admin> {
    if (this.adminClient) return Promise.resolve(this.adminClient)
    this.adminConnecting ??= (async () => {
      const admin = this.kafka().admin()
      try {
        await admin.connect()
      } catch (err) {
        this.adminConnecting = undefined
        throw err
      }
      this.adminClient = admin
      return admin
    })()
    return this.adminConnecting
  }

  async close(): Promise<void> {
    const admin = this.adminClient
    this.adminClient = undefined
    this.adminConnecting = undefined
    await admin?.disconnect().catch(() => undefined)
  }

  private kafkaJSConfig(): KafkaJSTypes.KafkaConfig {
    const { auth } = this.config
    const base = {
      brokers: this.config.brokers,
      clientId: this.config.clientId,
      ssl: this.config.ssl,
      connectionTimeout: this.config.timeoutMs,
      // stdout carries the MCP protocol, so the client must never log there.
      logger: stderrLogger,
      logLevel: KafkaJS.logLevel.WARN
    }
    switch (auth.type) {
      case 'none':
        return base
      case 'aws-msk-iam':
        return {
          ...base,
          sasl: {
            mechanism: 'oauthbearer',
            oauthBearerProvider: async () => {
              const { token, expiryTime } = await generateAuthToken({ region: auth.region })
              return { value: token, lifetime: expiryTime, principal: '' }
            }
          }
        }
      default:
        return {
          ...base,
          sasl: { mechanism: auth.type, username: auth.username, password: auth.password }
        }
    }
  }

  rdkafkaConfig(): RdKafkaConfig {
    const { auth, ssl } = this.config
    const sasl = auth.type !== 'none'
    const config: RdKafkaConfig = {
      'bootstrap.servers': this.config.brokers.join(','),
      'client.id': this.config.clientId,
      'security.protocol': `${sasl ? 'sasl_' : ''}${ssl ? 'ssl' : 'plaintext'}`,
      'socket.timeout.ms': this.config.timeoutMs,
      log_level: 3
    }
    switch (auth.type) {
      case 'none':
        break
      case 'aws-msk-iam':
        config['sasl.mechanisms'] = 'OAUTHBEARER'
        config['oauthbearer_token_refresh_cb'] = async () => {
          const { token, expiryTime } = await generateAuthToken({ region: auth.region })
          return { tokenValue: token, lifetime: expiryTime, principal: '' }
        }
        break
      default:
        config['sasl.mechanisms'] = auth.type.toUpperCase()
        config['sasl.username'] = auth.username
        config['sasl.password'] = auth.password
    }
    return config
  }
}

const stderrLogger: KafkaJSTypes.Logger = {
  info: () => undefined,
  error: (message) => process.stderr.write(`[kafka] ERROR ${message}\n`),
  warn: (message) => process.stderr.write(`[kafka] WARN ${message}\n`),
  debug: () => undefined,
  namespace: () => stderrLogger,
  setLogLevel: () => undefined
}

// Kafka's own internal topics say so; double-underscore topics are internal by convention too
// (MSK's __amazon_msk_canary reports isInternal false).
function isInternal(topic: { name: string; isInternal?: boolean }): boolean {
  return topic.isInternal === true || topic.name.startsWith('__')
}

function stateName(state: number): string {
  const entry = Object.entries(KafkaJS.ConsumerGroupStates).find(([, value]) => value === state)
  return entry ? entry[0] : String(state)
}

function clamp(value: number, min: number, max: number): number {
  return Math.min(Math.max(value, min), max)
}

export type { FormattedMessage }
