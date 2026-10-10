import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

// Every write the client library offers (producer, admin mutations, offset commits). None may appear
// in src/ outside write.ts, and write.ts is loaded only when KAFKA_MCP_ALLOW_WRITES=true.
const forbidden = [
  /\.producer\s*\(/,
  /\bProducer\b\s*\(/,
  /\bHighLevelProducer\b/,
  /\bcreateTopics?\b/,
  /\bdeleteTopics?\b/,
  /\bdeleteTopicRecords\b/,
  /\bcreatePartitions\b/,
  /\bdeleteGroups\b/,
  /\bsetOffsets\b/,
  /\bresetOffsets\b/,
  /\bcommitOffsets\b/,
  /\bcommitSync\b/,
  /\.commit\s*\(/,
  /\bcommitMessage\b/,
  /\balterConfigs\b/,
  /\.subscribe\s*\(/
]

function sources(dir: string): string[] {
  return readdirSync(dir, { withFileTypes: true }).flatMap((entry) =>
    entry.isDirectory() ? sources(join(dir, entry.name)) : [join(dir, entry.name)]
  )
}

describe('read-only guard', () => {
  const src = join(import.meta.dirname, '..', 'src')
  const all = sources(src).filter((f) => f.endsWith('.ts'))
  const writeModules = ['write.ts', 'write-tools.ts']
  const isWriteModule = (file: string) => writeModules.some((name) => file.endsWith(`/${name}`))
  const files = all.filter((f) => !isWriteModule(f))

  it('loads the write modules only through the gated dynamic imports in index.ts', () => {
    for (const file of files) {
      const lines = readFileSync(file, 'utf8').split('\n')
      const valueImports = lines.filter(
        (line) => /^import\s/.test(line) && !/^import\s+type\b/.test(line) && /\.\/write/.test(line)
      )
      expect(valueImports, file).toEqual([])
      const dynamic = lines.filter((line) => /import\('\.\/write/.test(line))
      if (!file.endsWith('/index.ts')) expect(dynamic, file).toEqual([])
    }
    const index = readFileSync(join(src, 'index.ts'), 'utf8')
    const start = index.indexOf('  if (config.allowWrites) {\n')
    const gate = index.slice(start, index.indexOf('\n  }\n', start))
    expect(start).toBeGreaterThan(0)
    const dynamicImports = index.match(/import\('\.\/write[^']*'\)/g) ?? []
    expect(dynamicImports.sort()).toEqual(["import('./write-tools.js')", "import('./write.js')"])
    for (const dynamicImport of dynamicImports) expect(gate).toContain(dynamicImport)
  })

  it('scans the server sources', () => {
    expect(files.length).toBeGreaterThan(0)
  })

  for (const pattern of forbidden) {
    it(`never calls ${pattern.source}`, () => {
      const hits = files.filter((file) => pattern.test(readFileSync(file, 'utf8')))
      expect(hits).toEqual([])
    })
  }
})
