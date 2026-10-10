import { randomUUID } from 'node:crypto'
import kafkaLib from '@confluentinc/kafka-javascript'
import type {
  KafkaConsumer as RdKafkaConsumer,
  KafkaJS as KafkaJSTypes
} from '@confluentinc/kafka-javascript'
import type { KafkaClients } from './kafka.js'

const { KafkaJS, KafkaConsumer } = kafkaLib

// Every write the server can make lives here, and this module is only loaded when
// KAFKA_MCP_ALLOW_WRITES=true. test/read-only.test.ts forbids writes in every other file.

export interface ProduceRequest {
  topic: string
  value: string
  valueEncoding: 'utf8' | 'base64'
  key?: string
  partition?: number
  headers?: Record<string, string>
}

export type ResetTarget =
  | { to: 'earliest' | 'latest' }
  | { to: 'offset'; offset: number }
  | { to: 'timestamp'; timestamp: number }

export class KafkaWriter {
  private producer: KafkaJSTypes.Producer | undefined
  private producerConnecting: Promise<KafkaJSTypes.Producer> | undefined

  constructor(private readonly clients: KafkaClients) {}

  async produce(request: ProduceRequest) {
    const producer = await this.getProducer()
    const [record] = await producer.send({
      topic: request.topic,
      messages: [
        {
          value: decodeValue(request.value, request.valueEncoding),
          key: request.key,
          partition: request.partition,
          headers: request.headers
        }
      ]
    })
    const offset = record?.baseOffset ?? record?.offset
    return {
      topic: request.topic,
      partition: record?.partition ?? null,
      offset: offset !== undefined && offset !== null ? Number(offset) : null
    }
  }

  async createTopic(topic: string, partitions: number, replicationFactor: number | undefined) {
    const admin = await this.clients.admin()
    // MSK rejects -1 (the broker default) here, so an unset factor is chosen explicitly: 3, or the
    // broker count on a smaller cluster.
    const factor = replicationFactor ?? Math.min(3, await this.clients.brokerCount())
    const created = await admin.createTopics({
      timeout: this.clients.config.timeoutMs,
      topics: [{ topic, numPartitions: partitions, replicationFactor: factor }]
    })
    if (!created) throw new Error(`Topic ${topic} already exists`)
    return { topic, partitions, replicationFactor: factor, created }
  }

  async deleteTopic(topic: string, confirm: string) {
    requireConfirmation(topic, confirm)
    const admin = await this.clients.admin()
    await admin.deleteTopics({ topics: [topic], timeout: this.clients.config.timeoutMs })
    return { topic, deleted: true }
  }

  async resetConsumerGroupOffsets(
    groupId: string,
    topic: string,
    target: ResetTarget,
    partition?: number
  ) {
    const admin = await this.clients.admin()
    const { timeoutMs } = this.clients.config
    const { groups } = await admin.describeGroups([groupId], { timeout: timeoutMs })
    const group = groups[0]
    if (!group || group.error)
      throw new Error(group?.error?.message ?? `Group ${groupId} not found`)
    // Kafka only accepts offset commits from outside a group while it has no members.
    if (group.members.length > 0) {
      throw new Error(
        `Group ${groupId} has ${group.members.length} active member(s); stop its consumers first`
      )
    }

    const watermarks = await admin.fetchTopicOffsets(topic, {
      timeout: timeoutMs,
      isolationLevel: KafkaJS.IsolationLevel.READ_UNCOMMITTED
    })
    const partitions = watermarks.filter(
      (w) => partition === undefined || w.partition === partition
    )
    if (partitions.length === 0) throw new Error(`Topic ${topic} has no partition ${partition}`)
    if (target.to === 'offset' && partition === undefined) {
      throw new Error('to "offset" needs a partition')
    }
    const byTimestamp =
      target.to === 'timestamp'
        ? new Map(
            (
              await admin.fetchTopicOffsetsByTimestamp(topic, target.timestamp, {
                timeout: timeoutMs,
                isolationLevel: KafkaJS.IsolationLevel.READ_UNCOMMITTED
              })
            ).map((entry) => [entry.partition, Number(entry.offset)])
          )
        : undefined
    const offsets = partitions.map((w) => {
      const low = Number(w.low)
      const high = Number(w.high)
      let offset: number
      switch (target.to) {
        case 'earliest':
          offset = low
          break
        case 'latest':
          offset = high
          break
        case 'offset':
          offset = Math.min(Math.max(target.offset, low), high)
          break
        case 'timestamp': {
          const found = byTimestamp?.get(w.partition) ?? -1
          offset = found >= 0 ? found : high
          break
        }
      }
      return { topic, partition: w.partition, offset }
    })

    await this.commitAs(groupId, offsets)
    return {
      groupId,
      topic,
      offsets: offsets.map(({ partition, offset }) => ({ partition, offset }))
    }
  }

  async deleteConsumerGroup(groupId: string, confirm: string) {
    requireConfirmation(groupId, confirm)
    const admin = await this.clients.admin()
    try {
      const [result] = await admin.deleteGroups([groupId], {
        timeout: this.clients.config.timeoutMs
      })
      if (result?.error) throw new Error(result.error.message)
    } catch (err) {
      // The client throws "Error in DeleteGroups" and keeps the per-group cause in `groups`.
      const groups = (err as { groups?: Array<{ error?: { message?: string } }> }).groups
      const cause = groups?.map((g) => g.error?.message).find(Boolean)
      if (cause) {
        throw new Error(`Could not delete consumer group ${groupId}: ${cause}`, { cause: err })
      }
      throw err
    }
    return { groupId, deleted: true }
  }

  async close(): Promise<void> {
    const producer = this.producer
    this.producer = undefined
    this.producerConnecting = undefined
    await producer?.disconnect().catch(() => undefined)
  }

  private async commitAs(
    groupId: string,
    offsets: Array<{ topic: string; partition: number; offset: number }>
  ) {
    const consumer: RdKafkaConsumer = new KafkaConsumer(
      {
        ...this.clients.rdkafkaConfig(),
        'group.id': groupId,
        'client.id': `${this.clients.config.clientId}-reset-${randomUUID().slice(0, 8)}`,
        'enable.auto.commit': false
      } as never,
      {}
    )
    await new Promise<void>((resolve, reject) =>
      consumer.connect({ timeout: this.clients.config.timeoutMs }, (err) =>
        err ? reject(err) : resolve()
      )
    )
    try {
      consumer.commitSync(offsets)
    } finally {
      await new Promise<void>((resolve) => consumer.disconnect(() => resolve()))
    }
  }

  private getProducer(): Promise<KafkaJSTypes.Producer> {
    if (this.producer) return Promise.resolve(this.producer)
    this.producerConnecting ??= (async () => {
      // Idempotence off: a plain produce needs only WriteData, not WriteDataIdempotently.
      const producer = this.clients.kafka().producer({ 'enable.idempotence': false } as never)
      try {
        await producer.connect()
      } catch (err) {
        this.producerConnecting = undefined
        throw err
      }
      this.producer = producer
      return producer
    })()
    return this.producerConnecting
  }
}

function requireConfirmation(name: string, confirm: string): void {
  if (confirm !== name) {
    throw new Error(`Refused: confirm must repeat the exact name "${name}"`)
  }
}

const base64 = /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/

// Node decodes base64 leniently (it accepts the URL-safe alphabet and drops invalid characters), so a
// malformed value would be produced as something other than what the caller sent.
function decodeValue(value: string, encoding: 'utf8' | 'base64'): Buffer {
  if (encoding === 'base64' && !base64.test(value)) {
    throw new Error('value is not valid standard base64 (A-Z, a-z, 0-9, +, /, = padding)')
  }
  return Buffer.from(value, encoding)
}
