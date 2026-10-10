import { describe, expect, it } from 'vitest'
import { loadConfig } from '../src/config.js'
import { formatData } from '../src/format.js'

describe('loadConfig', () => {
  it('defaults to no auth, plaintext and bounded reads', () => {
    const config = loadConfig({ KAFKA_BROKERS: 'a:9092, b:9092' })
    expect(config.brokers).toEqual(['a:9092', 'b:9092'])
    expect(config.auth).toEqual({ type: 'none' })
    expect(config.ssl).toBe(false)
    expect(config.maxMessages).toBe(50)
    expect(config.maxValueBytes).toBe(4096)
    expect(config.allowWrites).toBe(false)
  })

  it('enables writes only on an explicit true', () => {
    expect(
      loadConfig({ KAFKA_BROKERS: 'a:9092', KAFKA_MCP_ALLOW_WRITES: 'true' }).allowWrites
    ).toBe(true)
    expect(
      loadConfig({ KAFKA_BROKERS: 'a:9092', KAFKA_MCP_ALLOW_WRITES: 'false' }).allowWrites
    ).toBe(false)
    expect(() => loadConfig({ KAFKA_BROKERS: 'a:9092', KAFKA_MCP_ALLOW_WRITES: 'yes' })).toThrow(
      /KAFKA_MCP_ALLOW_WRITES/
    )
  })

  it('turns SSL on for MSK IAM unless told otherwise', () => {
    const msk = { KAFKA_BROKERS: 'b-1:9098', KAFKA_AUTH: 'aws-msk-iam', AWS_REGION: 'us-east-1' }
    expect(loadConfig(msk).ssl).toBe(true)
    expect(loadConfig({ ...msk, KAFKA_SSL: 'false' }).ssl).toBe(false)
  })

  it('refuses MSK IAM without a region', () => {
    expect(() => loadConfig({ KAFKA_BROKERS: 'b-1:9098', KAFKA_AUTH: 'aws-msk-iam' })).toThrow(
      /AWS_REGION/
    )
  })

  it('refuses SASL without credentials', () => {
    expect(() => loadConfig({ KAFKA_BROKERS: 'a:9092', KAFKA_AUTH: 'scram-sha-512' })).toThrow(
      /KAFKA_SASL_USERNAME/
    )
  })

  it('refuses a missing broker list', () => {
    expect(() => loadConfig({})).toThrow(/KAFKA_BROKERS/)
  })
})

describe('formatData', () => {
  it('keeps text as text', () => {
    expect(formatData('héllo', 100)).toEqual({
      encoding: 'utf8',
      data: 'héllo',
      bytes: 6,
      truncated: false
    })
  })

  it('returns binary as base64', () => {
    const result = formatData(Buffer.from([0xff, 0xfe, 0x00]), 100)
    expect(result).toMatchObject({ encoding: 'base64', data: '//4A', bytes: 3, truncated: false })
  })

  it('cuts large values and says so', () => {
    const result = formatData('x'.repeat(1000), 100)
    expect(result).toMatchObject({ encoding: 'utf8', bytes: 1000, truncated: true })
    expect(result?.data).toHaveLength(100)
  })

  it('treats null as absent', () => {
    expect(formatData(null, 100)).toBeNull()
  })
})
