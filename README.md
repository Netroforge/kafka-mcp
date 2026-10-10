# kafka-mcp

A [Model Context Protocol](https://modelcontextprotocol.io) server for Apache Kafka. It lets an AI
agent look at a cluster — topics, partitions, consumer groups and their lag, and the messages
themselves. It is **read-only unless you turn writes on**: by default it cannot produce, change a
topic, or move a consumer group's offsets.

It authenticates with AWS MSK IAM, SASL/PLAIN, SASL/SCRAM or no auth, and speaks MCP over stdio.

## Tools

| Tool                      | What it returns                                                                          |
| ------------------------- | ---------------------------------------------------------------------------------------- |
| `describe_cluster`        | Brokers, and how many topics the cluster holds                                           |
| `list_topics`             | Every topic with its partition count and replication factor                              |
| `describe_topic`          | A topic's partitions: leader, replicas, ISR, and the low/high offsets                    |
| `list_consumer_groups`    | Every consumer group with its state                                                      |
| `describe_consumer_group` | A group's members, and per partition the committed offset, high offset and lag           |
| `read_messages`           | Messages from the `latest`, the `earliest`, an `offset` of a partition, or a `timestamp` |

## Write mode (off by default)

`KAFKA_MCP_ALLOW_WRITES=true` adds five tools. Without it they are not listed at all, and the
modules that implement them (`src/write.ts`, `src/write-tools.ts`) are never loaded.

| Tool                           | What it does                                                          |
| ------------------------------ | --------------------------------------------------------------------- |
| `produce_message`              | Produces one message (UTF-8 or base64 value, key, partition, headers) |
| `create_topic`                 | Creates a topic                                                       |
| `delete_topic`                 | Deletes a topic; `confirm` must repeat its name                       |
| `reset_consumer_group_offsets` | Moves a group's offsets to earliest, latest, an offset or a timestamp |
| `delete_consumer_group`        | Deletes a group; `confirm` must repeat its id                         |

An offset reset is refused while the group has active members, as Kafka requires. The credentials
still decide what reaches the broker: with MSK IAM, writes also need `kafka-cluster:WriteData`,
`CreateTopic`, `DeleteTopic`, `AlterGroup` and `DeleteGroup`.

## Limitations

- Topic and broker configs are not available: `@confluentinc/kafka-javascript` has no
  DescribeConfigs API yet ([confluent-kafka-javascript#444](https://github.com/confluentinc/confluent-kafka-javascript/issues/444)).

## Read-only, by construction

- With writes off, no producer is ever created and no admin operation that changes the cluster is
  called. `test/read-only.test.ts` fails the build if one appears in `src/` outside the two write
  modules, or if those are loaded anywhere but behind the flag.
- `read_messages` assigns partitions directly and never commits, so it neither joins nor creates a
  consumer group, and no group's committed offsets move. The integration tests check both.
- Reads are bounded (`KAFKA_MCP_MAX_MESSAGES`), and keys, values and headers are cut at
  `KAFKA_MCP_MAX_VALUE_BYTES`. Data that is not valid UTF-8 comes back as base64.

For a hard guarantee, also give the server credentials that can only read. For MSK IAM:
`kafka-cluster:Connect`, `DescribeCluster`, `DescribeClusterDynamicConfiguration`, `DescribeTopic`,
`DescribeTopicDynamicConfiguration`, `ReadData` and `DescribeGroup` — no `Write*`, `Create*`,
`Alter*` or `Delete*`.

## Run

The package is not published to npm; run it straight from GitHub, pinned to a commit:

```json
{
  "mcpServers": {
    "kafka": {
      "command": "npx",
      "args": ["-y", "github:Netroforge/kafka-mcp#<commit-sha>"],
      "env": {
        "KAFKA_BROKERS": "b-1.example.kafka.us-east-1.amazonaws.com:9098",
        "KAFKA_AUTH": "aws-msk-iam",
        "AWS_REGION": "us-east-1"
      }
    }
  }
}
```

npm builds the TypeScript on install (`prepare`), so the first start takes a little longer. The
Kafka client is [`@confluentinc/kafka-javascript`](https://github.com/confluentinc/confluent-kafka-javascript),
which downloads a prebuilt librdkafka binary for the platform at install time (Linux glibc and musl,
x64 and arm64, macOS, Windows).

## Configuration

| Variable                    | Default     | Notes                                                                      |
| --------------------------- | ----------- | -------------------------------------------------------------------------- |
| `KAFKA_BROKERS`             | (required)  | Comma-separated `host:port` list                                           |
| `KAFKA_AUTH`                | `none`      | `none`, `plain`, `scram-sha-256`, `scram-sha-512` or `aws-msk-iam`         |
| `KAFKA_SSL`                 | see notes   | `true`/`false`; defaults to `true` for `aws-msk-iam`, otherwise `false`    |
| `KAFKA_SASL_USERNAME`       |             | For `plain` and `scram-*`                                                  |
| `KAFKA_SASL_PASSWORD`       |             | For `plain` and `scram-*`                                                  |
| `AWS_REGION`                |             | Required for `aws-msk-iam`; credentials come from the default AWS chain    |
| `KAFKA_CLIENT_ID`           | `kafka-mcp` |                                                                            |
| `KAFKA_MCP_MAX_MESSAGES`    | `50`        | Upper bound for `read_messages` `limit`                                    |
| `KAFKA_MCP_MAX_VALUE_BYTES` | `4096`      | Keys, values and headers are cut at this size                              |
| `KAFKA_MCP_TIMEOUT_MS`      | `15000`     | Per Kafka request                                                          |
| `KAFKA_MCP_ALLOW_WRITES`    | `false`     | `true` adds the write tools (see [Write mode](#write-mode-off-by-default)) |

With `aws-msk-iam` the credentials come from the standard AWS provider chain: environment
variables, a web identity token (EKS IRSA / Pod Identity), or an instance profile.

## Develop

```bash
npm install
npm run type-check && npm run lint && npm run format:check
# integration tests against a real broker, in Node 24 on Alpine:
docker compose -f compose.test.yaml up --build --exit-code-from test
```

## License

[Apache-2.0](LICENSE)
