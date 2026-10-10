# Changelog

## 0.1.0

- Read-only MCP server for Apache Kafka over stdio: `describe_cluster`, `list_topics`,
  `describe_topic`, `list_consumer_groups`, `describe_consumer_group`, `read_messages`.
- Authentication: none, SASL/PLAIN, SASL/SCRAM-SHA-256/512, AWS MSK IAM (OAUTHBEARER).
- `read_messages` reads from the latest, the earliest, an offset or a timestamp without joining a
  consumer group or committing offsets; output is bounded and binary data is base64.
