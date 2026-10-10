#!/usr/bin/env node
import { readFileSync } from 'node:fs'
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js'
import { loadConfig } from './config.js'
import { KafkaClients, KafkaReader } from './kafka.js'
import { createServer } from './tools.js'
import type { KafkaWriter } from './write.js'

// stdout is the MCP protocol stream: anything else written there corrupts it, so route stray
// console output to stderr before any dependency gets a chance to print.
console.log = console.info = console.debug = (...args: unknown[]) => console.error(...args)

const { version } = JSON.parse(
  readFileSync(new URL('../package.json', import.meta.url), 'utf8')
) as { version: string }

async function main(): Promise<void> {
  const config = loadConfig(process.env)
  const clients = new KafkaClients(config)
  // The write modules are not even loaded unless writes are enabled.
  let writer: KafkaWriter | undefined
  let extend: Parameters<typeof createServer>[3]
  if (config.allowWrites) {
    const { KafkaWriter: Writer } = await import('./write.js')
    const { registerWriteTools } = await import('./write-tools.js')
    const enabled = new Writer(clients)
    writer = enabled
    extend = (server) => registerWriteTools(server, enabled)
  }
  const server = createServer(new KafkaReader(clients), version, config.maxMessages, extend)

  const shutdown = async () => {
    await server.close().catch(() => undefined)
    await writer?.close()
    await clients.close()
    process.exit(0)
  }
  process.on('SIGINT', shutdown)
  process.on('SIGTERM', shutdown)
  process.stdin.on('end', shutdown)

  await server.connect(new StdioServerTransport())
  console.error(
    `kafka-mcp ${version} ready (${config.brokers.length} brokers, auth ${config.auth.type}, writes ${config.allowWrites ? 'ENABLED' : 'off'})`
  )
}

main().catch((err: unknown) => {
  console.error(err instanceof Error ? err.message : err)
  process.exit(1)
})
