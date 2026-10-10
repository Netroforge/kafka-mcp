import { readdirSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'

// The server must never be able to change a cluster. These are every write the client library
// offers (producer, admin mutations, offset commits); none may appear in src/.
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
  const files = sources(join(import.meta.dirname, '..', 'src')).filter((f) => f.endsWith('.ts'))

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
