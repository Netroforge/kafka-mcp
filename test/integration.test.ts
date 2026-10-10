import { randomUUID } from 'node:crypto'
import kafkaLib from '@confluentinc/kafka-javascript'
import type { KafkaConsumer as RdKafkaConsumer } from '@confluentinc/kafka-javascript'
import { Client } from '@modelcontextprotocol/sdk/client/index.js'
import { StdioClientTransport } from '@modelcontextprotocol/sdk/client/stdio.js'
import { InMemoryTransport } from '@modelcontextprotocol/sdk/inMemory.js'
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js'
import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'
import { KafkaReader } from '../src/kafka.js'
import { createServer } from '../src/tools.js'

const { KafkaJS, KafkaConsumer } = kafkaLib
const brokers = process.env.KAFKA_TEST_BROKERS

// Runs against a real broker (compose.test.yaml, or the CI service container). The fixtures are
// written here with the full client; the server under test only ever reads them.
describe.skipIf(!brokers)('kafka-mcp against a broker', () => {
  const run = randomUUID().slice(0, 8)
  const topic = `kafka-mcp-test-${run}`
  const blobs = `kafka-mcp-test-blobs-${run}`
  const group = `kafka-mcp-test-group-${run}`
  const base = Date.now() - 600_000
  const kafka = new KafkaJS.Kafka({ kafkaJS: { brokers: (brokers ?? '').split(',') } })
  const admin = kafka.admin()
  let client: Client
  let reader: KafkaReader

  async function call(name: string, args: Record<string, unknown> = {}) {
    const result = (await client.callTool({ name, arguments: args })) as CallToolResult
    const text = result.content.map((c) => (c.type === 'text' ? c.text : '')).join('')
    return { isError: result.isError === true, text, json: () => JSON.parse(text) }
  }

  async function groupIds(): Promise<string[]> {
    return (await admin.listGroups()).groups.map((g) => g.groupId).sort()
  }

  beforeAll(async () => {
    await admin.connect()
    await admin.createTopics({
      topics: [
        { topic, numPartitions: 2, replicationFactor: 1 },
        { topic: blobs, numPartitions: 1, replicationFactor: 1 }
      ]
    })
    const producer = kafka.producer()
    await producer.connect()
    // 20 messages, n=0..19: even n on partition 0, odd n on partition 1, one second apart.
    await producer.send({
      topic,
      messages: Array.from({ length: 20 }, (_, n) => ({
        key: `k${n}`,
        value: JSON.stringify({ n }),
        partition: n % 2,
        timestamp: String(base + n * 1000),
        headers: { source: 'test' }
      }))
    })
    await producer.send({
      topic: blobs,
      messages: [{ value: Buffer.from([0xff, 0xfe, 0x00, 0x01]) }, { value: 'y'.repeat(10_000) }]
    })
    await producer.disconnect()

    // A consumer group that committed partition 0 up to offset 3 and partition 1 up to 5.
    const committer: RdKafkaConsumer = new KafkaConsumer(
      { 'bootstrap.servers': brokers, 'group.id': group, 'enable.auto.commit': false } as never,
      {}
    )
    await new Promise<void>((resolve, reject) =>
      committer.connect({}, (err) => (err ? reject(err) : resolve()))
    )
    committer.commitSync([
      { topic, partition: 0, offset: 3 },
      { topic, partition: 1, offset: 5 }
    ])
    await new Promise<void>((resolve) => committer.disconnect(() => resolve()))

    const config = loadConfig({ KAFKA_BROKERS: brokers, KAFKA_MCP_MAX_VALUE_BYTES: '1024' })
    reader = new KafkaReader(config)
    const server = createServer(reader, 'test', config.maxMessages)
    const [clientTransport, serverTransport] = InMemoryTransport.createLinkedPair()
    await server.connect(serverTransport)
    client = new Client({ name: 'kafka-mcp-test', version: 'test' })
    await client.connect(clientTransport)
  })

  afterAll(async () => {
    await client?.close()
    await reader?.close()
    await admin.deleteTopics({ topics: [topic, blobs] }).catch(() => undefined)
    await admin.deleteGroups([group]).catch(() => undefined)
    await admin.disconnect()
  })

  it('offers exactly the six read-only tools', async () => {
    const { tools } = await client.listTools()
    expect(tools.map((t) => t.name).sort()).toEqual([
      'describe_cluster',
      'describe_consumer_group',
      'describe_topic',
      'list_consumer_groups',
      'list_topics',
      'read_messages'
    ])
    for (const tool of tools) expect(tool.annotations?.readOnlyHint).toBe(true)
  })

  it('describes the cluster', async () => {
    const cluster = (await call('describe_cluster')).json()
    expect(cluster.brokers.length).toBeGreaterThanOrEqual(1)
    expect(cluster.topicCount).toBeGreaterThanOrEqual(2)
  })

  it('lists topics, hiding internal ones unless asked', async () => {
    const topics = (await call('list_topics')).json()
    expect(topics).toContainEqual({
      name: topic,
      internal: false,
      partitions: 2,
      replicationFactor: 1
    })
    expect(topics.map((t: { name: string }) => t.name)).not.toContain('__consumer_offsets')
    const all = (await call('list_topics', { includeInternal: true })).json()
    expect(all.map((t: { name: string }) => t.name)).toContain('__consumer_offsets')
  })

  it('describes a topic with its offsets', async () => {
    const described = (await call('describe_topic', { topic })).json()
    expect(described.partitions.map((p: { partition: number }) => p.partition)).toEqual([0, 1])
    expect(described.partitions[0]).toMatchObject({ lowOffset: 0, highOffset: 10, messages: 10 })
    expect(described.partitions[1]).toMatchObject({ lowOffset: 0, highOffset: 10, messages: 10 })
    expect(described.totalMessages).toBe(20)
  })

  it('reports an unknown topic as an error', async () => {
    const result = await call('describe_topic', { topic: `missing-${run}` })
    expect(result.isError).toBe(true)
  })

  it('reads the newest messages, newest first', async () => {
    const read = (await call('read_messages', { topic, position: 'latest', limit: 3 })).json()
    expect(
      read.messages.map((m: { value: { data: string } }) => JSON.parse(m.value.data).n)
    ).toEqual([19, 18, 17])
    expect(read.messages[0]).toMatchObject({
      partition: 1,
      offset: 9,
      timestamp: base + 19_000,
      key: { encoding: 'utf8', data: 'k19' },
      headers: { source: { encoding: 'utf8', data: 'test' } }
    })
  })

  it('reads the oldest messages, oldest first', async () => {
    const read = (await call('read_messages', { topic, position: 'earliest', limit: 4 })).json()
    expect(
      read.messages.map((m: { value: { data: string } }) => JSON.parse(m.value.data).n)
    ).toEqual([0, 1, 2, 3])
  })

  it('reads one partition from an offset', async () => {
    const read = (
      await call('read_messages', { topic, position: 'offset', partition: 1, offset: 2, limit: 2 })
    ).json()
    expect(read.messages.map((m: { offset: number }) => m.offset)).toEqual([2, 3])
    expect(
      read.messages.map((m: { value: { data: string } }) => JSON.parse(m.value.data).n)
    ).toEqual([5, 7])
  })

  it('needs a partition to read from an offset', async () => {
    const result = await call('read_messages', { topic, position: 'offset', offset: 2 })
    expect(result.isError).toBe(true)
    expect(result.text).toMatch(/partition and offset/)
  })

  it('reads from a timestamp', async () => {
    const read = (
      await call('read_messages', {
        topic,
        position: 'timestamp',
        timestamp: base + 10_000,
        limit: 2
      })
    ).json()
    expect(
      read.messages.map((m: { value: { data: string } }) => JSON.parse(m.value.data).n)
    ).toEqual([10, 11])
  })

  it('returns binary as base64 and cuts large values', async () => {
    const read = (
      await call('read_messages', { topic: blobs, position: 'earliest', limit: 2 })
    ).json()
    expect(read.messages[0].value).toEqual({
      encoding: 'base64',
      data: '//4AAQ==',
      bytes: 4,
      truncated: false
    })
    expect(read.messages[1].value).toMatchObject({
      encoding: 'utf8',
      bytes: 10_000,
      truncated: true
    })
    expect(read.messages[1].value.data).toHaveLength(1024)
  })

  it('lists and describes a consumer group with its lag', async () => {
    const groups = (await call('list_consumer_groups')).json()
    expect(groups.map((g: { groupId: string }) => g.groupId)).toContain(group)
    const described = (await call('describe_consumer_group', { groupId: group })).json()
    expect(described.topics).toEqual([
      {
        topic,
        lag: 12,
        partitions: [
          { partition: 0, committedOffset: 3, highOffset: 10, lag: 7 },
          { partition: 1, committedOffset: 5, highOffset: 10, lag: 5 }
        ]
      }
    ])
    expect(described.totalLag).toBe(12)
  })

  it('serves the same tools over stdio from the built entry point, with a clean stdout', async () => {
    // Spawns dist/index.js the way an MCP host does. Any stray write to stdout would corrupt the
    // protocol stream and fail the handshake or the call.
    const stdio = new Client({ name: 'kafka-mcp-stdio-test', version: 'test' })
    await stdio.connect(
      new StdioClientTransport({
        command: process.execPath,
        args: ['dist/index.js'],
        env: { PATH: process.env.PATH ?? '', KAFKA_BROKERS: brokers ?? '' },
        stderr: 'ignore'
      })
    )
    try {
      expect((await stdio.listTools()).tools).toHaveLength(6)
      const result = (await stdio.callTool({
        name: 'describe_topic',
        arguments: { topic }
      })) as CallToolResult
      expect(result.isError).not.toBe(true)
      const text = result.content.map((c) => (c.type === 'text' ? c.text : '')).join('')
      expect(JSON.parse(text).totalMessages).toBe(20)
    } finally {
      await stdio.close()
    }
  })

  it('reads without creating a consumer group or moving committed offsets', async () => {
    const before = await groupIds()
    for (const position of ['latest', 'earliest', 'timestamp'] as const) {
      const result = await call('read_messages', { topic, position, timestamp: base, limit: 5 })
      expect(result.isError).toBe(false)
    }
    expect(await groupIds()).toEqual(before)
    const described = (await call('describe_consumer_group', { groupId: group })).json()
    expect(described.totalLag).toBe(12)
  })
})
