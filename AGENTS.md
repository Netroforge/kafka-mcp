# AGENTS.md

Guidance for AI coding agents working in this repository.

## Project

A read-only MCP server for Apache Kafka (TypeScript, Node ≥22, ESM). Entry `src/index.ts` (stdio
transport), tools in `src/tools.ts`, all Kafka access in `src/kafka.ts`, configuration from the
environment in `src/config.ts`.

## The rule that matters

**The server never writes to a cluster.** No producer, no admin mutation (create/delete/alter topics,
partitions, configs, groups, records), no offset commit, no `subscribe` (it joins a group). Reads use
the librdkafka-style `KafkaConsumer` with `assign()` and auto-commit off. `test/read-only.test.ts`
lists the forbidden calls and fails on any of them in `src/` — extend the list when the client gains
a new write, never loosen it. Test fixtures may write; they live in `test/`.

stdout is the MCP protocol stream. Nothing may print to it: the Kafka client logs through
`stderrLogger`, and `src/index.ts` routes `console.log` to stderr.

## Commands

- `npm run type-check`, `npm run lint`, `npm run format:check`, `npm run build`
- Unit tests: `npm test` (integration tests skip without `KAFKA_TEST_BROKERS`)
- Everything, against a real broker, in Node 24 on Alpine:
  `docker compose -f compose.test.yaml up --build --exit-code-from test`

The Kafka client is a native module. `node_modules` installed inside the compose container is built
for musl and will not load on a glibc host — reinstall before running locally outside Docker.

## Conventions

- Prettier: single quotes, no semicolons, width 100, no trailing commas.
- Dependencies are pinned to exact versions; `npm-shrinkwrap.json` pins the whole tree for consumers
  who install from git (a dependency's `package-lock.json` is ignored by npm, a shrinkwrap is not).
- Consumers run `npx -y github:Netroforge/kafka-mcp#<sha>`; the package is not published to npm.
- Dependabot opens up to 10 PRs a week for npm and GitHub Actions.
