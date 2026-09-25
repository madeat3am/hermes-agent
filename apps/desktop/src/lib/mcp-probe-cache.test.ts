import { describe, expect, it } from 'vitest'

import type { McpTestResult } from '@/hermes'

import { classifyProbe, freshProbe, PROBE_TTL_MS, probeCache, probeKey } from './mcp-probe-cache'

const result = (over: Partial<McpTestResult> = {}): McpTestResult => ({ ok: true, tools: [], ...over })

describe('classifyProbe', () => {
  it('classifies a successful probe as ok', () => {
    expect(classifyProbe(result())).toBe('ok')
  })

  it.each([
    'HTTP 401 Unauthorized',
    'invalid_token: The access token expired',
    'OAuth authorization required',
    'authentication failed'
  ])('classifies "%s" as needs-auth', error => {
    expect(classifyProbe(result({ ok: false, error }))).toBe('needs-auth')
  })

  it.each([
    'Connection timed out after 10s (raise connect_timeout, or complete an OAuth login also by oauth.timeout)',
    "Failed to connect to MCP server 'cluster-research': CancelledError"
  ])('classifies timeout "%s" as error, not needs-auth', error => {
    expect(classifyProbe(result({ ok: false, error }))).toBe('error')
  })

  it('classifies other failures as error', () => {
    expect(classifyProbe(result({ ok: false, error: 'ECONNREFUSED 127.0.0.1:3845' }))).toBe('error')
  })

  it('classifies a failure without an error string as error', () => {
    expect(classifyProbe(result({ ok: false }))).toBe('error')
  })
})

describe('probeKey', () => {
  it('scopes by profile, name, and connection-relevant config', () => {
    const server = { url: 'https://api.githubcopilot.com/mcp/' }
    expect(probeKey('github', server, 'default')).not.toBe(probeKey('github', server, 'work'))
    expect(probeKey('github', server, 'default')).not.toBe(probeKey('gh2', server, 'default'))
    expect(probeKey('github', server, 'default')).not.toBe(
      probeKey('github', { url: 'https://other.example/mcp' }, 'default')
    )
  })

  it('ignores non-connection fields so cosmetic edits still hit the cache', () => {
    const server = { url: 'https://api.example/mcp' }
    expect(probeKey('s', server, 'default')).toBe(probeKey('s', { ...server, description: 'hi' }, 'default'))
  })
})

describe('freshProbe', () => {
  it('returns a cached result inside the TTL and null after it', () => {
    const key = probeKey('ttl-test', { url: 'https://x' }, 'default')
    const cached = result()
    probeCache.set(key, { at: 1_000, result: cached })

    expect(freshProbe(key, 1_000 + PROBE_TTL_MS - 1)).toBe(cached)
    expect(freshProbe(key, 1_000 + PROBE_TTL_MS)).toBeNull()
    expect(freshProbe('missing', 0)).toBeNull()
    probeCache.delete(key)
  })
})
