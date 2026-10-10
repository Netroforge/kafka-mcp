import type { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js'
import { z } from 'zod'
import { run } from './tools.js'
import type { KafkaWriter } from './write.js'

// The write tools. Like write.ts, this module is loaded only when KAFKA_MCP_ALLOW_WRITES=true.

const write = { readOnlyHint: false, destructiveHint: false, idempotentHint: false }
const destructive = { readOnlyHint: false, destructiveHint: true, idempotentHint: false }

export function registerWriteTools(server: McpServer, writer: KafkaWriter): void {
  server.registerTool(
    'produce_message',
    {
      title: 'Produce message',
      description:
        'WRITE: produce one message to a topic. Returns the partition and offset it got.',
      inputSchema: {
        topic: z.string().min(1).describe('Topic name'),
        value: z.string().describe('Message value'),
        valueEncoding: z
          .enum(['utf8', 'base64'])
          .default('utf8')
          .describe('How value is encoded (default utf8); base64 for binary payloads'),
        key: z.string().optional().describe('Message key'),
        partition: z.number().int().min(0).optional().describe('Partition (default: by key)'),
        headers: z.record(z.string(), z.string()).optional().describe('Message headers')
      },
      annotations: write
    },
    (args) => run(() => writer.produce(args))
  )

  server.registerTool(
    'create_topic',
    {
      title: 'Create topic',
      description: 'WRITE: create a topic.',
      inputSchema: {
        topic: z.string().min(1).describe('Topic name'),
        partitions: z.number().int().min(1).max(1000).default(1).describe('Partition count'),
        replicationFactor: z
          .number()
          .int()
          .min(1)
          .optional()
          .describe('Replication factor (default 3, or the broker count on a smaller cluster)')
      },
      annotations: write
    },
    ({ topic, partitions, replicationFactor }) =>
      run(() => writer.createTopic(topic, partitions, replicationFactor))
  )

  server.registerTool(
    'delete_topic',
    {
      title: 'Delete topic',
      description:
        'DESTRUCTIVE: delete a topic and every message in it. confirm must repeat the topic name.',
      inputSchema: {
        topic: z.string().min(1).describe('Topic name'),
        confirm: z.string().describe('The topic name again, to confirm')
      },
      annotations: destructive
    },
    ({ topic, confirm }) => run(() => writer.deleteTopic(topic, confirm))
  )

  server.registerTool(
    'reset_consumer_group_offsets',
    {
      title: 'Reset consumer group offsets',
      description:
        "DESTRUCTIVE: move a consumer group's committed offsets on a topic to the earliest or " +
        'latest offset, an offset of one partition, or a timestamp. The group must have no ' +
        'active members.',
      inputSchema: {
        groupId: z.string().min(1).describe('Consumer group id'),
        topic: z.string().min(1).describe('Topic name'),
        to: z.enum(['earliest', 'latest', 'offset', 'timestamp']).describe('Where to move to'),
        partition: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe('Only this partition. Required for to "offset"'),
        offset: z.number().int().min(0).optional().describe('Target offset, for to "offset"'),
        timestamp: z
          .number()
          .int()
          .min(0)
          .optional()
          .describe('Epoch milliseconds, for to "timestamp"')
      },
      annotations: destructive
    },
    ({ groupId, topic, to, partition, offset, timestamp }) =>
      run(() => {
        if (to === 'offset' && offset === undefined) throw new Error('to "offset" needs offset')
        if (to === 'timestamp' && timestamp === undefined) {
          throw new Error('to "timestamp" needs timestamp')
        }
        const target =
          to === 'offset'
            ? { to, offset: offset as number }
            : to === 'timestamp'
              ? { to, timestamp: timestamp as number }
              : { to }
        return writer.resetConsumerGroupOffsets(groupId, topic, target, partition)
      })
  )

  server.registerTool(
    'delete_consumer_group',
    {
      title: 'Delete consumer group',
      description:
        'DESTRUCTIVE: delete a consumer group and its committed offsets. The group must have no ' +
        'active members. confirm must repeat the group id.',
      inputSchema: {
        groupId: z.string().min(1).describe('Consumer group id'),
        confirm: z.string().describe('The group id again, to confirm')
      },
      annotations: destructive
    },
    ({ groupId, confirm }) => run(() => writer.deleteConsumerGroup(groupId, confirm))
  )
}
