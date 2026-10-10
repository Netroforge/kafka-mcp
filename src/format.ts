import type { Message } from '@confluentinc/kafka-javascript'

const utf8 = new TextDecoder('utf-8', { fatal: true })

export interface FormattedData {
  encoding: 'utf8' | 'base64'
  data: string
  bytes: number
  truncated: boolean
}

export interface FormattedMessage {
  partition: number
  offset: number
  timestamp: number
  key: FormattedData | null
  value: FormattedData | null
  headers: Record<string, FormattedData>
}

/**
 * Text stays text; anything that is not valid UTF-8 is returned as base64. Both are cut at
 * `maxBytes` so a single large record cannot flood the caller's context.
 */
export function formatData(
  input: Buffer | string | null | undefined,
  maxBytes: number
): FormattedData | null {
  if (input === null || input === undefined) return null
  const buffer = typeof input === 'string' ? Buffer.from(input, 'utf8') : input
  const truncated = buffer.length > maxBytes
  const slice = truncated ? buffer.subarray(0, maxBytes) : buffer
  if (isUtf8(buffer)) {
    // The cut can split a multi-byte character, so the slice decodes leniently.
    const data = new TextDecoder('utf-8').decode(slice)
    return { encoding: 'utf8', data, bytes: buffer.length, truncated }
  }
  return { encoding: 'base64', data: slice.toString('base64'), bytes: buffer.length, truncated }
}

function isUtf8(buffer: Buffer): boolean {
  try {
    utf8.decode(buffer)
    return true
  } catch {
    return false
  }
}

export function formatMessage(message: Message, maxBytes: number): FormattedMessage {
  const headers: Record<string, FormattedData> = {}
  for (const header of message.headers ?? []) {
    for (const [name, value] of Object.entries(header)) {
      const formatted = formatData(value, maxBytes)
      if (formatted) headers[name] = formatted
    }
  }
  return {
    partition: message.partition,
    offset: message.offset,
    timestamp: message.timestamp ?? 0,
    key: formatData(message.key, maxBytes),
    value: formatData(message.value, maxBytes),
    headers
  }
}
