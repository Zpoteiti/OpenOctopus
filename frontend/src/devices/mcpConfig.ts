import type { McpServerConfig, ServerMcpServerConfig } from '../api/types'

export type EditableMcpServer = McpServerConfig | ServerMcpServerConfig

function lines(value: FormDataEntryValue | null): string[] {
  return String(value ?? '')
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean)
}

function parseKeyValues(value: FormDataEntryValue | null): Record<string, string> {
  return Object.fromEntries(lines(value).map((line) => {
    const separator = line.indexOf('=')
    return separator < 0 ? [line, ''] : [line.slice(0, separator).trim(), line.slice(separator + 1)]
  }))
}

export function formatMcpSecrets(value: Record<string, string> | undefined): string {
  return Object.entries(value ?? {}).map(([key, item]) => `${key}=${item}`).join('\n')
}

export function mcpCapabilityMode(value: string[] | null | undefined): 'all' | 'none' | 'exact' {
  if (value == null) return 'none'
  return value.length === 0 ? 'all' : 'exact'
}

export function mcpSecretMap(server: EditableMcpServer | null): Record<string, string> {
  if (!server) return {}
  return server.transport === 'stdio' ? server.env ?? {} : server.headers ?? {}
}

function sameSink(left: EditableMcpServer, right: EditableMcpServer): boolean {
  if (left.name !== right.name || left.transport !== right.transport) return false
  if (left.transport === 'stdio' && right.transport === 'stdio') {
    return left.command === right.command
      && JSON.stringify(left.args ?? []) === JSON.stringify(right.args ?? [])
      && (left.cwd ?? null) === (right.cwd ?? null)
  }
  return left.transport !== 'stdio' && right.transport !== 'stdio' && left.url === right.url
}

function readEnabledCapabilities(data: FormData): string[] | null | 'empty' {
  const mode = String(data.get('capability_mode'))
  if (mode === 'none') return null
  if (mode === 'all') return []
  const values = data.getAll('enabled_capabilities')
    .flatMap((value) => String(value).split(','))
    .map((value) => value.trim())
    .filter(Boolean)
  return values.length ? [...new Set(values)] : 'empty'
}

export function readMcpForm(
  data: FormData,
  existing: EditableMcpServer | null,
  serverSide: boolean,
  translate: (key: string, options?: Record<string, unknown>) => string,
): EditableMcpServer | string {
  const enabled = readEnabledCapabilities(data)
  if (enabled === 'empty') return translate('mcp.exactRequired')
  const transport = String(data.get('transport')) as 'stdio' | 'streamable_http' | 'sse'
  const common = {
    name: String(data.get('name')),
    enabled_capabilities: enabled,
    ...(serverSide ? { max_concurrent_calls: Number(data.get('max_concurrent_calls')) } : {}),
  }
  const secrets = parseKeyValues(data.get('secrets'))
  let candidate: EditableMcpServer
  if (transport === 'stdio') {
    candidate = {
      ...common,
      transport,
      command: String(data.get('command')),
      args: lines(data.get('args')),
      cwd: String(data.get('cwd') ?? '').trim() || null,
      env: secrets,
    } as EditableMcpServer
  } else {
    candidate = {
      ...common,
      transport,
      url: String(data.get('url')),
      headers: secrets,
    } as EditableMcpServer
  }

  if (existing && Object.keys(mcpSecretMap(existing)).length) {
    if (!sameSink(existing, candidate)) {
      if (!Object.keys(secrets).length || Object.values(secrets).some((value) => value === '<redacted>' || value === '')) {
        return translate('mcp.reenterSecrets')
      }
    } else if (!data.get('editing')) {
      const merged = { ...mcpSecretMap(existing), ...secrets }
      candidate = candidate.transport === 'stdio' ? { ...candidate, env: merged } : { ...candidate, headers: merged }
    }
  }
  return candidate
}
