# Changelog

## 0.2.1

- `create_topic` without `replicationFactor` uses 3, or the broker count on a smaller cluster,
  instead of the broker default (-1), which MSK rejects.
- Tool errors include the underlying causes the Kafka client aggregates (e.g. why a topic could
  not be created), not just its summary.

## 0.2.0

- Optional write mode, off by default. `KAFKA_MCP_ALLOW_WRITES=true` registers five more tools:
  `produce_message`, `create_topic`, `delete_topic`, `reset_consumer_group_offsets` and
  `delete_consumer_group`. Without the flag they are not listed, and their code is not loaded.
- The deletes require the name repeated in `confirm`; offset resets are refused while the group
  has active members.
- Double-underscore topics count as internal even when the cluster says otherwise (MSK's
  `__amazon_msk_canary`).

## 0.1.0

- Read-only MCP server for Apache Kafka over stdio: `describe_cluster`, `list_topics`,
  `describe_topic`, `list_consumer_groups`, `describe_consumer_group`, `read_messages`.
- Authentication: none, SASL/PLAIN, SASL/SCRAM-SHA-256/512, AWS MSK IAM (OAUTHBEARER).
- `read_messages` reads from the latest, the earliest, an offset or a timestamp without joining a
  consumer group or committing offsets; output is bounded and binary data is base64.
