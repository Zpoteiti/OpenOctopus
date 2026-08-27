import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { type FormEvent, type ReactNode, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate, useParams } from 'react-router-dom'

import { apiJson } from '../api/client'
import type {
  Device,
  DeviceConfig,
  DeviceConfigPatch,
  DeviceMcpServerConfigView,
  McpServerConfig,
} from '../api/types'
import { Card, ErrorNotice, PageHeader, StatusBadge } from '../components/Page'
import {
  type EditableMcpServer,
  formatMcpSecrets,
  mcpCapabilityMode,
  mcpSecretMap,
  readMcpForm,
} from './mcpConfig'

const DEVICES_KEY = ['devices'] as const
const MAX_AUTOMATIC_REFRESHES = 6

function deviceListQuery() {
  return {
    queryKey: DEVICES_KEY,
    queryFn: () => apiJson<Device[]>('/api/devices'),
    refetchInterval: (query: { state: { dataUpdateCount: number } }) => query.state.dataUpdateCount < MAX_AUTOMATIC_REFRESHES ? 15_000 : false,
    refetchIntervalInBackground: false,
    staleTime: 10_000,
  }
}

function lines(value: FormDataEntryValue | null): string[] {
  return String(value ?? '')
    .split('\n')
    .map((item) => item.trim())
    .filter(Boolean)
}

export function DeviceListPage(): ReactNode {
  const { t } = useTranslation()
  const client = useQueryClient()
  const devices = useQuery(deviceListQuery())
  const [showCreate, setShowCreate] = useState(false)
  const [showDownload, setShowDownload] = useState(false)
  const [issuedToken, setIssuedToken] = useState<{ name: string; token: string } | null>(null)
  const createDevice = useMutation({
    mutationFn: (body: Record<string, unknown>) => apiJson<{ token: string; device: Device }>('/api/devices', {
      method: 'POST',
      body: JSON.stringify(body),
    }),
    onSuccess: (result) => {
      setIssuedToken({ name: result.device.name, token: result.token })
      setShowCreate(false)
      void client.invalidateQueries({ queryKey: DEVICES_KEY })
    },
  })

  const submit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    createDevice.mutate({
      name: String(data.get('name') ?? ''),
      workspace_path: String(data.get('workspace_path') ?? '~/openoctopus/workspace'),
      restrict_to_workspace: data.get('restrict_to_workspace') === 'on',
    })
  }

  return (
    <div className="page-scroll">
      <PageHeader
        eyebrow={t('devices.eyebrow')}
        title={t('devices.title')}
        description={t('devices.description')}
        actions={(
          <>
            <button type="button" className="secondary-button" onClick={() => setShowDownload((current) => !current)}>{t('devices.downloadClient')}</button>
            <button className="secondary-button" data-testid="devices-refresh" onClick={() => void devices.refetch()}>{t('common.refresh')}</button>
            <button className="primary-button" aria-label={t('devices.add')} onClick={() => setShowCreate(true)}>＋ {t('devices.add')}</button>
          </>
        )}
      />
      {showDownload ? <p className="form-notice" role="status">{t('devices.downloadPlaceholder')}</p> : null}
      {issuedToken ? (
        <Card title={t('devices.tokenTitle', { name: issuedToken.name })} description={t('devices.tokenOnce')}>
          <div className="secret-once">
            <code>{issuedToken.token}</code>
            <button className="secondary-button" onClick={() => setIssuedToken(null)}>{t('devices.savedToken')}</button>
          </div>
        </Card>
      ) : null}
      {showCreate ? (
        <Card title={t('devices.addTitle')} description={t('devices.slugHelp')}>
          <form className="form-grid" onSubmit={submit}>
            <label>{t('devices.name')}<input name="name" required placeholder={t('devices.nameExample')} /></label>
            <label>
              {t('devices.workspacePath')}
              <input name="workspace_path" defaultValue="~/openoctopus/workspace" required />
            </label>
            <label className="toggle-field">
              <input name="restrict_to_workspace" type="checkbox" defaultChecked />
              {t('devices.restrict')}
            </label>
            <p className="field-help full-row">{t('devices.guardrailHelp')}</p>
            <ErrorNotice error={createDevice.error} />
            <div className="form-actions full-row">
              <button type="button" className="secondary-button" onClick={() => setShowCreate(false)}>{t('common.cancel')}</button>
              <button className="primary-button" disabled={createDevice.isPending}>{t('devices.create')}</button>
            </div>
          </form>
        </Card>
      ) : null}
      <ErrorNotice error={devices.error} />
      <div className="card-grid">
        {devices.data?.map((device) => (
          <Link className="device-card" to={`/devices/${device.name}`} key={device.id}>
            <div className="device-card-top">
              <span className="device-glyph" aria-hidden="true">PC</span>
              <StatusBadge tone={device.online ? 'success' : 'neutral'}>{device.online ? t('common.online') : t('common.offline')}</StatusBadge>
            </div>
            <h2>{device.name}</h2>
            <p>{device.workspace_path}</p>
            <dl className="compact-stats">
              <div><dt>MCP</dt><dd>{device.mcp_config_count}</dd></div>
              <div><dt>{t('devices.providerVisible')}</dt><dd>{device.mcp_provider_visible_capability_count}</dd></div>
              <div><dt>{t('devices.configVersion')}</dt><dd>{device.config_revision}</dd></div>
            </dl>
          </Link>
        ))}
      </div>
      {devices.isPending ? <p className="empty-state">{t('devices.loading')}</p> : null}
      {!devices.isPending && !devices.data?.length ? <p className="empty-state">{t('devices.empty')}</p> : null}
    </div>
  )
}

export function DeviceDetailPage(): ReactNode {
  const { t, i18n } = useTranslation()
  const { name = '' } = useParams()
  const navigate = useNavigate()
  const client = useQueryClient()
  const devices = useQuery(deviceListQuery())
  const config = useQuery({
    queryKey: ['device-config', name],
    queryFn: () => apiJson<DeviceConfig>(`/api/devices/${encodeURIComponent(name)}/config`),
    enabled: Boolean(name),
  })
  const device = devices.data?.find((candidate) => candidate.name === name)
  const [issuedToken, setIssuedToken] = useState<string | null>(null)
  const [confirmRegenerate, setConfirmRegenerate] = useState(false)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [policyRevision, setPolicyRevision] = useState<number | null>(null)

  const save = useMutation({
    mutationFn: (body: DeviceConfigPatch) => apiJson<DeviceConfig>(`/api/devices/${encodeURIComponent(name)}/config`, {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),
    onSuccess: async (result) => {
      client.setQueryData(['device-config', name], result)
      setPolicyRevision(null)
      await client.invalidateQueries({ queryKey: DEVICES_KEY })
      if (result.device.name !== name) navigate(`/devices/${result.device.name}`, { replace: true })
    },
  })
  const regenerate = useMutation({
    mutationFn: () => apiJson<{ token: string }>(`/api/devices/${encodeURIComponent(name)}/regenerate-token`, { method: 'POST' }),
    onSuccess: (result) => {
      setIssuedToken(result.token)
      setConfirmRegenerate(false)
      void client.invalidateQueries({ queryKey: DEVICES_KEY })
    },
  })
  const remove = useMutation({
    mutationFn: () => apiJson(`/api/devices/${encodeURIComponent(name)}`, { method: 'DELETE' }),
    onSuccess: () => navigate('/devices', { replace: true }),
  })

  if (devices.isPending || config.isPending) return <p className="page-status">{t('devices.loadingConfig')}</p>
  if (!device) return <div className="page-scroll"><PageHeader title={t('devices.notFound')} /><ErrorNotice error={devices.error} /></div>

  const markDirty = (): void => {
    setPolicyRevision((current) => current ?? device.config_revision)
  }
  const submit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    save.mutate({
      base_config_revision: policyRevision ?? device.config_revision,
      name: String(data.get('name') ?? device.name),
      workspace_path: String(data.get('workspace_path') ?? ''),
      restrict_to_workspace: data.get('restrict_to_workspace') === 'on',
      shell_timeout_max: Number(data.get('shell_timeout_max')),
      env_allowlist: lines(data.get('env_allowlist')).flatMap((item) => item.split(',').map((part) => part.trim()).filter(Boolean)),
      ssrf_denylist: lines(data.get('ssrf_denylist')),
    })
  }

  return (
    <div className="page-scroll">
      <PageHeader
        eyebrow={t('devices.eyebrow')}
        title={device.name}
        description={t('devices.routeDescription')}
        actions={<StatusBadge tone={device.online ? 'success' : 'neutral'}>{device.online ? t('common.online') : t('common.offline')}</StatusBadge>}
      />
      {issuedToken ? (
        <Card title={t('devices.newToken')} description={t('devices.oldTokenInvalid')}>
          <div className="secret-once"><code>{issuedToken}</code><button className="secondary-button" onClick={() => setIssuedToken(null)}>{t('devices.savedToken')}</button></div>
        </Card>
      ) : null}
      <Card title={t('devices.information')}>
        <dl className="detail-grid">
          <div><dt>{t('devices.tokenHint')}</dt><dd><code>{device.token_hint}</code></dd></div>
          <div><dt>{t('devices.configVersion')}</dt><dd>{config.data?.device.config_revision ?? device.config_revision}</dd></div>
          <div><dt>{t('devices.createdAt')}</dt><dd>{new Date(device.created_at).toLocaleString(i18n.language)}</dd></div>
        </dl>
      </Card>
      <Card title={t('devices.workspaceExec')} description={t('devices.workspaceExecHelp')}>
        <form key={policyRevision ?? device.config_revision} className="form-grid" onChange={markDirty} onFocusCapture={markDirty} onSubmit={submit}>
          <label>{t('devices.name')}<input name="name" defaultValue={device.name} required /></label>
          <label>
            <span className="label-with-help">
              {t('devices.workspacePath')}
              <button type="button" className="info-button" aria-label={t('devices.pathExamples')} title={t('devices.pathExamplesTitle')}>i</button>
            </span>
            <input name="workspace_path" defaultValue={device.workspace_path} required />
          </label>
          <label className="toggle-field full-row"><input name="restrict_to_workspace" type="checkbox" defaultChecked={device.restrict_to_workspace} />{t('devices.restrictShort')}</label>
          <label>{t('devices.shellTimeout')}<input name="shell_timeout_max" type="number" min="0" max="86400" defaultValue={device.shell_timeout_max} required /></label>
          <label>{t('devices.inheritedEnv')}<textarea name="env_allowlist" rows={3} defaultValue={device.env_allowlist.join(', ')} /></label>
          <label className="full-row">{t('devices.webFetchDenylist')}<textarea name="ssrf_denylist" rows={5} defaultValue={device.ssrf_denylist.join('\n')} /></label>
          <p className="field-help full-row">{t('devices.networkHelp')}</p>
          <ErrorNotice error={save.error} />
          <div className="form-actions full-row"><button className="primary-button" disabled={save.isPending}>{t('devices.saveConfig')}</button></div>
        </form>
      </Card>
      <Card
        title={t('devices.mcpTitle')}
        description={t('devices.mcpSummary', { configs: device.mcp_config_count, capabilities: device.mcp_provider_visible_capability_count })}
        actions={<Link className="secondary-button" to={`/devices/${device.name}/mcp`}>{t('devices.manageMcp')}</Link>}
      ><p className="field-help">{t('devices.mcpValidation')}</p></Card>
      <Card title={t('devices.tokenAndDevice')} tone="danger">
        <div className="danger-actions">
          {confirmRegenerate ? (
            <>
              <span>{t('devices.regenerateWarning')}</span>
              <button className="danger-button" onClick={() => regenerate.mutate()} disabled={regenerate.isPending}>{t('devices.confirmRegenerate')}</button>
              <button className="secondary-button" onClick={() => setConfirmRegenerate(false)} disabled={regenerate.isPending}>{t('common.cancel')}</button>
            </>
          ) : (
            <button className="secondary-button" onClick={() => setConfirmRegenerate(true)}>{t('devices.regenerateToken')}</button>
          )}
          {confirmDelete ? (
            <><span>{t('devices.deleteWarning')}</span><button className="danger-button" onClick={() => remove.mutate()}>{t('devices.confirmDelete')}</button></>
          ) : <button className="danger-button" onClick={() => setConfirmDelete(true)}>{t('devices.deleteDevice')}</button>}
        </div>
        <ErrorNotice error={regenerate.error ?? remove.error} />
      </Card>
    </div>
  )
}

type DiscoveredCapability = { raw_name: string; final_name: string; enabled: boolean }
export type DiscoveredServer = {
  tools: DiscoveredCapability[]
  resources: DiscoveredCapability[]
  resource_templates: DiscoveredCapability[]
  prompts: DiscoveredCapability[]
}

function deviceMcpInput(view: DeviceMcpServerConfigView): McpServerConfig {
  if (view.transport === 'stdio') {
    return {
      name: view.name,
      transport: 'stdio',
      command: view.command,
      args: view.args,
      cwd: view.cwd,
      env: view.env,
      enabled_capabilities: view.enabled_capabilities,
    }
  }
  return {
    name: view.name,
    transport: view.transport,
    url: view.url,
    headers: view.headers,
    enabled_capabilities: view.enabled_capabilities,
  }
}

type McpDraft = { baseRevision: number; servers: McpServerConfig[] }
type McpEditing = { baseRevision: number; server: McpServerConfig; catalog?: DiscoveredServer }

export function DeviceMcpPage(): ReactNode {
  const { t } = useTranslation()
  const { name = '' } = useParams()
  const client = useQueryClient()
  const config = useQuery({
    queryKey: ['device-config', name],
    queryFn: () => apiJson<DeviceConfig>(`/api/devices/${encodeURIComponent(name)}/config`),
    refetchInterval: (query) => query.state.dataUpdateCount < MAX_AUTOMATIC_REFRESHES ? 15_000 : false,
    refetchIntervalInBackground: false,
  })
  const [draft, setDraft] = useState<McpDraft | null>(null)
  const [editing, setEditing] = useState<McpEditing | null>(null)
  const [formError, setFormError] = useState<string | null>(null)
  const servers = draft?.servers ?? config.data?.mcp_servers.map(deviceMcpInput) ?? []
  const save = useMutation({
    mutationFn: (current: McpDraft) => apiJson<DeviceConfig>(`/api/devices/${encodeURIComponent(name)}/config`, {
      method: 'PATCH',
      body: JSON.stringify({ base_config_revision: current.baseRevision, mcp_servers: current.servers }),
    }),
    onSuccess: (result) => {
      client.setQueryData(['device-config', name], result)
      setDraft(null)
      setEditing(null)
      void client.invalidateQueries({ queryKey: DEVICES_KEY })
    },
  })
  const updateDraft = (nextServers: McpServerConfig[], baseRevision?: number): void => {
    if (!config.data) return
    setDraft((current) => ({
      baseRevision: current?.baseRevision ?? baseRevision ?? config.data.device.config_revision,
      servers: nextServers,
    }))
  }
  const add = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    if (!config.data) return
    const data = new FormData(event.currentTarget)
    const existing = editing?.server ?? servers.find((item) => item.name === String(data.get('name'))) ?? null
    const result = readMcpForm(data, existing, false, t)
    if (typeof result === 'string') {
      setFormError(result)
      return
    }
    setFormError(null)
    updateDraft([...servers.filter((item) => item.name !== result.name && item.name !== editing?.server.name), result as McpServerConfig], editing?.baseRevision)
    setEditing(null)
  }

  return (
    <div className="page-scroll">
      <PageHeader
        eyebrow={t('devices.mcpTitle')}
        title={`${name} · MCP`}
        description={t('mcp.deviceDescription')}
        actions={(
          <>
            <button className="secondary-button" data-testid="device-mcp-refresh" onClick={() => void config.refetch()}>{t('common.refresh')}</button>
            <button className="primary-button" disabled={!draft || save.isPending} onClick={() => draft && save.mutate(draft)}>{t('mcp.saveDevice')}</button>
          </>
        )}
      />
      <ErrorNotice error={config.error ?? save.error} />
      <div className="settings-stack">
        {servers.map((server) => {
          const view = config.data?.mcp_servers.find((item) => item.name === server.name)
          const catalog = config.data?.mcp_discovered[server.name]
          return (
            <Card
              key={server.name}
              title={server.name}
              description={server.transport}
              actions={(
                <>
                  <button className="secondary-button" aria-label={t('mcp.editNamed', { name: server.name })} onClick={() => {
                    setEditing({ server, baseRevision: draft?.baseRevision ?? config.data?.device.config_revision ?? 0, catalog })
                    setFormError(null)
                  }}>{t('common.edit')}</button>
                  <button className="danger-link" aria-label={t('mcp.deleteNamed', { name: server.name })} onClick={() => updateDraft(servers.filter((item) => item.name !== server.name))}>{t('common.delete')}</button>
                </>
              )}
            >
              <div className="mcp-summary">
                <code>{server.transport === 'stdio' ? server.command : server.url}</code>
                {view?.effective_status === 'shadowed_by_server' ? <StatusBadge tone="warning">{t('mcp.shadowed', { name: view.shadowed_by })}</StatusBadge> : <StatusBadge tone="success">{t('mcp.active')}</StatusBadge>}
                <span>{formatCapabilityMode(server.enabled_capabilities, t)}</span>
              </div>
              {catalog ? <CapabilityCatalog catalog={catalog} /> : null}
            </Card>
          )
        })}
        <Card title={editing ? t('mcp.editNamed', { name: editing.server.name }) : t('mcp.addOrReplace')} description={t('mcp.replaceHelp')}>
          <McpForm
            key={editing?.server.name ?? 'new'}
            onSubmit={add}
            initial={editing?.server}
            catalog={editing?.catalog}
            error={formError}
            onCancel={editing ? () => { setEditing(null); setFormError(null) } : undefined}
          />
        </Card>
      </div>
    </div>
  )
}

export function McpForm({
  onSubmit,
  serverSide = false,
  initial,
  catalog,
  error,
  onCancel,
}: {
  onSubmit: (event: FormEvent<HTMLFormElement>) => void
  serverSide?: boolean
  initial?: EditableMcpServer
  catalog?: DiscoveredServer
  error?: string | null
  onCancel?: () => void
}): ReactNode {
  const { t } = useTranslation()
  const [transport, setTransport] = useState<'stdio' | 'streamable_http' | 'sse'>(initial?.transport ?? 'streamable_http')
  const [capabilityModeValue, setCapabilityModeValue] = useState(mcpCapabilityMode(initial?.enabled_capabilities))
  const initialSecrets = formatMcpSecrets(mcpSecretMap(initial ?? null))
  const secrets = transport === initial?.transport ? initialSecrets : ''
  const initialConcurrency = initial && 'max_concurrent_calls' in initial ? initial.max_concurrent_calls : transport === 'stdio' ? 1 : 8
  return (
    <form className="form-grid" onSubmit={onSubmit}>
      {initial ? <input type="hidden" name="editing" value="true" /> : null}
      <label>{t('mcp.serviceName')}<input name="name" required pattern="[a-z][a-z0-9_]{0,31}" placeholder="company_search" defaultValue={initial?.name ?? ''} /></label>
      <label>{t('mcp.transport')}<select name="transport" value={transport} onChange={(event) => setTransport(event.target.value as typeof transport)}><option value="streamable_http">Streamable HTTP</option><option value="sse">SSE</option><option value="stdio">stdio</option></select></label>
      {transport === 'stdio' ? (
        <>
          <label>{t('mcp.executable')}<input name="command" required placeholder="python" defaultValue={initial?.transport === 'stdio' ? initial.command : ''} /></label>
          <label>{t('mcp.args')}<textarea name="args" rows={3} placeholder={'-m\ncalculator_mcp'} defaultValue={initial?.transport === 'stdio' ? (initial.args ?? []).join('\n') : ''} /></label>
          <label>{t('mcp.workingDirectory')}<input name="cwd" placeholder={t('mcp.optional')} defaultValue={initial?.transport === 'stdio' ? initial.cwd ?? '' : ''} /></label>
        </>
      ) : <label className="full-row">{t('mcp.url')}<input name="url" type="url" required placeholder="https://mcp.example.com/mcp" defaultValue={initial && initial.transport !== 'stdio' ? initial.url : ''} /></label>}
      <label className="full-row">{transport === 'stdio' ? t('mcp.environment') : t('mcp.headers')} {t('mcp.keyValueHelp')}<textarea key={transport} name="secrets" rows={3} defaultValue={secrets} /></label>
      <fieldset className="choice-field full-row">
        <legend>{t('mcp.capabilities')}</legend>
        <label><input type="radio" name="capability_mode" value="all" checked={capabilityModeValue === 'all'} onChange={(event) => setCapabilityModeValue(event.target.value as typeof capabilityModeValue)} />{t('mcp.enableAll')}</label>
        <label><input type="radio" name="capability_mode" value="none" checked={capabilityModeValue === 'none'} onChange={(event) => setCapabilityModeValue(event.target.value as typeof capabilityModeValue)} />{t('mcp.disableAll')}</label>
        <label><input type="radio" name="capability_mode" value="exact" checked={capabilityModeValue === 'exact'} onChange={(event) => setCapabilityModeValue(event.target.value as typeof capabilityModeValue)} />{t('mcp.exact')}</label>
      </fieldset>
      {capabilityModeValue === 'exact' ? (
        catalog ? <CapabilityChoices catalog={catalog} selected={initial?.enabled_capabilities ?? []} /> : <label className="full-row">{t('mcp.exactNames')}<textarea name="enabled_capabilities" rows={2} placeholder="mcp_company_search_search" /></label>
      ) : null}
      {serverSide ? <label>{t('mcp.maxConcurrency')}<input name="max_concurrent_calls" type="number" min="1" max="32" defaultValue={initialConcurrency} required /></label> : null}
      {error ? <p className="form-error full-row" role="alert">{error}</p> : null}
      <div className="form-actions full-row">
        {onCancel ? <button type="button" className="secondary-button" onClick={onCancel}>{t('common.cancel')}</button> : null}
        <button className="secondary-button">{initial ? t('common.save') : t('mcp.addDraft')}</button>
      </div>
    </form>
  )
}

function capabilityGroups(catalog: DiscoveredServer): Array<[string, DiscoveredCapability[]]> {
  return [
    ['mcp.tools', catalog.tools],
    ['mcp.resources', catalog.resources],
    ['mcp.resourceTemplates', catalog.resource_templates],
    ['mcp.prompts', catalog.prompts],
  ]
}

export function CapabilityCatalog({ catalog }: { catalog: DiscoveredServer }): ReactNode {
  const { t } = useTranslation()
  return (
    <div>
      {capabilityGroups(catalog).map(([label, capabilities]) => capabilities.length ? (
        <div key={label}><strong>{t(label)}</strong><ul>{capabilities.map((capability) => <li key={capability.final_name}><code>{capability.final_name}</code></li>)}</ul></div>
      ) : null)}
    </div>
  )
}

function CapabilityChoices({ catalog, selected }: { catalog: DiscoveredServer; selected: string[] }): ReactNode {
  const { t } = useTranslation()
  return (
    <fieldset className="choice-field full-row">
      <legend>{t('mcp.exactNames')}</legend>
      {capabilityGroups(catalog).map(([label, capabilities]) => capabilities.length ? (
        <div key={label}><strong>{t(label)}</strong>{capabilities.map((capability) => (
          <label key={capability.final_name}><input name="enabled_capabilities" type="checkbox" value={capability.final_name} defaultChecked={selected.includes(capability.final_name)} />{capability.final_name}</label>
        ))}</div>
      ) : null)}
    </fieldset>
  )
}

function formatCapabilityMode(value: string[] | null | undefined, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (value == null) return t('mcp.disableAll')
  if (value.length === 0) return t('mcp.enableAll')
  return t('mcp.exactCount', { count: value.length })
}
