import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import type { CallToolResult } from '@modelcontextprotocol/sdk/types.js'
import { z } from 'zod'
import type { KafkaReader } from './kafka.js'

// The read tools only read. The annotations tell clients so; the reader enforces it.
const readOnly = { readOnlyHint: true, destructiveHint: false, idempotentHint: true }

/**
 * `extend` registers more tools on the same server: index.ts passes the write tools (write-tools.ts)
 * only when KAFKA_MCP_ALLOW_WRITES=true, so a read-only server does not even list them.
 */
export function createServer(
  reader: KafkaReader,
  version: string,
  maxMessages: number,
  extend?: (server: McpServer) => void
): McpServer {
  const server = new McpServer({ name: 'kafka-mcp', version })

  server.registerTool(
    'describe_cluster',
    {
      title: 'Describe cluster',
      description: 'Brokers of the Kafka cluster and how many (non-internal) topics it holds.',
      annotations: { ...readOnly, openWorldHint: false }
    },
    () => run(() => reader.describeCluster())
  )

  server.registerTool(
    'list_topics',
    {
      title: 'List topics',
      description: 'All topics with their partition count and replication factor.',
      inputSchema: {
        includeInternal: z
          .boolean()
          .optional()
          .describe('Include internal topics such as __consumer_offsets (default false)')
      },
      annotations: readOnly
    },
    ({ includeInternal }) => run(() => reader.listTopics(includeInternal ?? false))
  )

  server.registerTool(
    'describe_topic',
    {
      title: 'Describe topic',
      description:
        'Partitions of a topic: leader, replicas, in-sync replicas, and the low/high offsets that ' +
        'bound the messages it currently holds.',
      inputSchema: { topic: z.string().min(1).describe('Topic name') },
      annotations: readOnly
    },
    ({ topic }) => run(() => reader.describeTopic(topic))
  )

  server.registerTool(
    'list_consumer_groups',
    {
      title: 'List consumer groups',
      description: 'All consumer groups with their state and protocol type.',
      annotations: readOnly
    },
    () => run(() => reader.listConsumerGroups())
  )

  server.registerTool(
    'describe_consumer_group',
    {
      title: 'Describe consumer group',
      description:
        'A consumer group: its members and their assignments, and for every partition it has ' +
        'committed to, the committed offset, the high offset and the lag between them.',
      inputSchema: { groupId: z.string().min(1).describe('Consumer group id') },
      annotations: readOnly
    },
    ({ groupId }) => run(() => reader.describeConsumerGroup(groupId))
  )

  server.registerTool(
    'read_messages',
    {
      title: 'Read messages',
      description:
        'Read messages from a topic without joining a consumer group or committing offsets. ' +
        'position "latest" returns the newest messages, "earliest" the oldest still retained, ' +
        '"offset" the messages from an offset of one partition, and "timestamp" the first ' +
        `messages at or after a time. At most ${maxMessages} messages; large keys, values and ` +
        'headers are truncated, and binary data is returned as base64.',
      inputSchema: {
        topic: z.string().min(1).describe('Topic name'),
        position: z
          .enum(['latest', 'earliest', 'offset', 'timestamp'])
          .default('latest')
          .describe('Where to start reading (default latest)'),
        partition: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe('Only this partition. Required for position "offset"'),
        offset: z.number().int().min(0).optional().describe('Start offset, for position "offset"'),
        timestamp: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe('Epoch milliseconds, for position "timestamp"'),
        limit: z
          .number()
          .int()
          .min(1)
          .max(maxMessages)
          .default(Math.min(10, maxMessages))
          .describe(`How many messages (default 10, at most ${maxMessages})`)
      },
      annotations: readOnly
    },
    (args) => run(() => reader.readMessages(args))
  )

  extend?.(server)

  return server
}

export async function run(work: () => Promise<unknown>): Promise<CallToolResult> {
  try {
    const result = await work()
    return { content: [{ type: 'text', text: JSON.stringify(result, null, 2) }] }
  } catch (err) {
    return { isError: true, content: [{ type: 'text', text: `Error: ${describeError(err)}` }] }
  }
}

// The client aggregates per-item failures (e.g. "Topic creation errors") and keeps the causes in
// `errors`; without them the message says nothing about what went wrong.
function describeError(err: unknown): string {
  if (!(err instanceof Error)) return String(err)
  const causes = (err as Error & { errors?: unknown[] }).errors
  if (!Array.isArray(causes) || causes.length === 0) return err.message
  return `${err.message}: ${causes.map((cause) => describeError(cause)).join('; ')}`
}
