import { z } from 'zod'

const list = z
  .string()
  .transform((value) =>
    value
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
  )
  .pipe(z.array(z.string()).min(1))

const flag = z.enum(['true', 'false']).transform((value) => value === 'true')

const envSchema = z
  .object({
    KAFKA_BROKERS: list,
    KAFKA_CLIENT_ID: z.string().min(1).default('kafka-mcp'),
    KAFKA_AUTH: z
      .enum(['none', 'plain', 'scram-sha-256', 'scram-sha-512', 'aws-msk-iam'])
      .default('none'),
    KAFKA_SSL: flag.optional(),
    KAFKA_SASL_USERNAME: z.string().optional(),
    KAFKA_SASL_PASSWORD: z.string().optional(),
    AWS_REGION: z.string().optional(),
    KAFKA_MCP_MAX_MESSAGES: z.coerce.number().int().min(1).max(1000).default(50),
    KAFKA_MCP_MAX_VALUE_BYTES: z.coerce.number().int().min(64).max(1_048_576).default(4096),
    KAFKA_MCP_TIMEOUT_MS: z.coerce.number().int().min(1000).max(120_000).default(15_000)
  })
  .superRefine((env, ctx) => {
    if (env.KAFKA_AUTH === 'aws-msk-iam' && !env.AWS_REGION) {
      ctx.addIssue({ code: 'custom', path: ['AWS_REGION'], message: 'required for aws-msk-iam' })
    }
    const sasl = ['plain', 'scram-sha-256', 'scram-sha-512'].includes(env.KAFKA_AUTH)
    if (sasl && !(env.KAFKA_SASL_USERNAME && env.KAFKA_SASL_PASSWORD)) {
      ctx.addIssue({
        code: 'custom',
        path: ['KAFKA_SASL_USERNAME'],
        message: `KAFKA_SASL_USERNAME and KAFKA_SASL_PASSWORD are required for ${env.KAFKA_AUTH}`
      })
    }
  })

export type Auth =
  | { type: 'none' }
  | { type: 'plain' | 'scram-sha-256' | 'scram-sha-512'; username: string; password: string }
  | { type: 'aws-msk-iam'; region: string }

export interface Config {
  brokers: string[]
  clientId: string
  auth: Auth
  ssl: boolean
  maxMessages: number
  maxValueBytes: number
  timeoutMs: number
}

export function loadConfig(env: NodeJS.ProcessEnv): Config {
  const parsed = envSchema.safeParse(env)
  if (!parsed.success) {
    const problems = parsed.error.issues.map((issue) => `${issue.path.join('.')}: ${issue.message}`)
    throw new Error(`Invalid configuration: ${problems.join('; ')}`)
  }
  const e = parsed.data
  let auth: Auth
  switch (e.KAFKA_AUTH) {
    case 'none':
      auth = { type: 'none' }
      break
    case 'aws-msk-iam':
      auth = { type: 'aws-msk-iam', region: e.AWS_REGION as string }
      break
    default:
      auth = {
        type: e.KAFKA_AUTH,
        username: e.KAFKA_SASL_USERNAME as string,
        password: e.KAFKA_SASL_PASSWORD as string
      }
  }
  return {
    brokers: e.KAFKA_BROKERS,
    clientId: e.KAFKA_CLIENT_ID,
    auth,
    // MSK IAM only exists on TLS listeners, so it implies SSL unless told otherwise.
    ssl: e.KAFKA_SSL ?? auth.type === 'aws-msk-iam',
    maxMessages: e.KAFKA_MCP_MAX_MESSAGES,
    maxValueBytes: e.KAFKA_MCP_MAX_VALUE_BYTES,
    timeoutMs: e.KAFKA_MCP_TIMEOUT_MS
  }
}
