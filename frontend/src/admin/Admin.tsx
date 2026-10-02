import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { type FormEvent, type ReactNode, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate } from 'react-router-dom'

import { apiJson } from '../api/client'
import type {
  AdminConfig,
  AdminUser,
  ServerMcpResponse,
  ServerMcpServerConfig,
  ServerMcpServerConfigView,
} from '../api/types'
import type { components } from '../api/openapi'
import { useAuthenticatedUser } from '../auth/context'
import { Card, ErrorNotice, PageHeader, StatusBadge } from '../components/Page'
import { CapabilityCatalog, type DiscoveredServer, McpForm } from '../devices/Devices'
import { readMcpForm } from '../devices/mcpConfig'

const ADMIN_CONFIG_KEY = ['admin-config'] as const
const SERVER_MCP_KEY = ['server-mcp'] as const
const USER_PAGE_SIZE = 50
const MAX_RUNTIME_REFRESHES = 12

type JevStatus = components['schemas']['JevStatus']

function lines(value: FormDataEntryValue | null): string[] {
  return String(value ?? '').split('\n').map((item) => item.trim()).filter(Boolean)
}

function addText(body: Record<string, unknown>, key: string, value: FormDataEntryValue | null): void {
  const text = String(value ?? '').trim()
  if (text) body[key] = text
}

function addNumber(body: Record<string, unknown>, key: string, value: FormDataEntryValue | null): void {
  const text = String(value ?? '').trim()
  if (text) body[key] = Number(text)
}

export function AdminSettingsPage(): ReactNode {
  const { t } = useTranslation()
  const client = useQueryClient()
  const jevApiKeyInput = useRef<HTMLInputElement>(null)
  const config = useQuery({
    queryKey: ADMIN_CONFIG_KEY,
    queryFn: () => apiJson<AdminConfig>('/api/admin/config'),
  })
  const patchConfig = useMutation({
    mutationFn: (body: Record<string, unknown>) => apiJson<AdminConfig>('/api/admin/config', {
      method: 'PATCH',
      body: JSON.stringify(body),
    }),
    onSuccess: (result) => client.setQueryData(ADMIN_CONFIG_KEY, result),
  })
  const checkJev = useMutation({
    mutationFn: () => apiJson<JevStatus>('/api/admin/config/jev/check', { method: 'POST' }),
    onSuccess: (status) => client.setQueryData<AdminConfig>(ADMIN_CONFIG_KEY, (current) => current ? { ...current, jev_status: status } : current),
  })

  if (config.isPending) return <p className="page-status">{t('admin.loading')}</p>
  if (!config.data) return <div className="page-scroll"><PageHeader title={t('admin.settings')} /><ErrorNotice error={config.error} /></div>
  const value = config.data

  const saveProvider = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    const body: Record<string, unknown> = { llm_max_output_tokens: Number(data.get('llm_max_output_tokens')) }
    addText(body, 'llm_endpoint', data.get('llm_endpoint'))
    const apiKey = String(data.get('llm_api_key') ?? '')
    if (apiKey) body.llm_api_key = apiKey
    addText(body, 'llm_model', data.get('llm_model'))
    addNumber(body, 'llm_max_context_tokens', data.get('llm_max_context_tokens'))
    addNumber(body, 'llm_compaction_threshold_tokens', data.get('llm_compaction_threshold_tokens'))
    addNumber(body, 'llm_max_concurrent_requests', data.get('llm_max_concurrent_requests'))
    patchConfig.mutate(body)
  }
  const saveQuota = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    patchConfig.mutate({
      quota_bytes: Math.round(Number(data.get('quota_mib')) * 1024 * 1024),
      shared_workspace_quota_bytes: Math.round(Number(data.get('shared_quota_mib')) * 1024 * 1024),
    })
  }
  const saveFetch = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    patchConfig.mutate({ web_fetch_denylist: lines(new FormData(event.currentTarget).get('web_fetch_denylist')) })
  }
  const saveDefaultSoul = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    patchConfig.mutate({ default_soul: String(new FormData(event.currentTarget).get('default_soul') ?? '') })
  }
  const saveJev = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    const endpoint = String(data.get('jev_endpoint') ?? '').trim()
    const key = String(data.get('jev_api_key') ?? '')
    patchConfig.mutate({ jev_endpoint: endpoint || null, ...(key ? { jev_api_key: key } : {}) }, {
      onSuccess: () => { if (jevApiKeyInput.current) jevApiKeyInput.current.value = '' },
    })
  }
  const jevState = value.jev_status.state
  const status = value.jev_status
  const jevUnavailable = jevState !== 'available' && jevState !== 'unchecked'

  return (
    <div className="page-scroll">
      <PageHeader eyebrow={t('nav.admin')} title={t('admin.settings')} description={t('admin.settingsDescription')} actions={<AdminTabs />} />
      <ErrorNotice error={patchConfig.error} />
      <div className="settings-stack">
        <Card title={t('admin.defaultSoul')} description={t('admin.defaultSoulDescription')}>
          <form className="form-grid" onSubmit={saveDefaultSoul}>
            <label className="full-row">{t('admin.defaultSoul')}<textarea name="default_soul" rows={6} maxLength={32000} defaultValue={value.default_soul} required /></label>
            <div className="form-actions full-row"><button className="primary-button" disabled={patchConfig.isPending}>{t('admin.saveDefaultSoul')}</button></div>
          </form>
        </Card>
        <Card title={t('admin.provider')} description={t('admin.providerDescription')}>
          <form className="form-grid" onSubmit={saveProvider}>
            <label className="full-row">{t('admin.apiBaseUrl')}<input name="llm_endpoint" type="url" defaultValue={value.llm_endpoint ?? ''} placeholder="https://api.siliconflow.cn" /></label>
            <label>{t('admin.apiKey')}<input name="llm_api_key" type="password" placeholder={value.llm_api_key === '<redacted>' ? t('admin.apiKeyConfigured') : t('admin.apiKeyMissing')} autoComplete="off" /></label>
            <label>{t('admin.model')}<input name="llm_model" defaultValue={value.llm_model ?? ''} placeholder="Qwen/Qwen3.5-4B" /></label>
            <label>{t('admin.contextWindow')}<input name="llm_max_context_tokens" type="number" min="1" defaultValue={value.llm_max_context_tokens ?? ''} /></label>
            <label>{t('admin.compactionThreshold')}<input name="llm_compaction_threshold_tokens" type="number" min="4001" defaultValue={value.llm_compaction_threshold_tokens ?? ''} /></label>
            <label>
              {t('admin.maxConcurrent')}
              <input
                id="llm-max-concurrent"
                name="llm_max_concurrent_requests"
                type="number"
                min="0"
                max="1000000"
                defaultValue={value.llm_max_concurrent_requests ?? ''}
                aria-label={t('admin.maxConcurrent')}
                aria-describedby="llm-max-concurrent-help"
              />
              <small id="llm-max-concurrent-help">{t('admin.unlimited')}</small>
            </label>
            <label>{t('admin.maxOutput')}<input name="llm_max_output_tokens" type="number" min="1" max="1000000" defaultValue={value.llm_max_output_tokens} required /></label>
            <div className="form-actions full-row"><button className="primary-button" disabled={patchConfig.isPending}>{t('admin.saveProvider')}</button></div>
          </form>
        </Card>
        <Card title={t('admin.jevTitle')} description={t('admin.jevDescription')}>
          <form className="form-grid" onSubmit={saveJev}>
            <label className="full-row">{t('admin.jevEndpoint')}<input name="jev_endpoint" type="url" defaultValue={value.jev_endpoint ?? ''} placeholder="https://api.typesafe.ai" /></label>
            <label className="full-row">{t('admin.jevApiKey')}<input ref={jevApiKeyInput} name="jev_api_key" type="password" placeholder={value.jev_api_key === '<redacted>' ? t('admin.apiKeyConfigured') : t('admin.apiKeyMissing')} autoComplete="off" /></label>
            <p className="field-help full-row">{t('admin.jevAvailability', { state: t(`admin.jevState.${jevState}`) })} {t(`admin.jevReason.${jevState}`)}{status.checked_at ? ` ${t('admin.jevCheckedAt', { time: new Date(status.checked_at).toLocaleString() })}` : ''}</p>
            {jevUnavailable ? <p className="field-help full-row" role="status"><strong>{t('admin.dreamUnavailable')}</strong> {t(`admin.jevReason.${jevState}`)}</p> : null}
            {jevState === 'unchecked' ? <p className="field-help full-row">{t('admin.heartbeatJevCheckHelp')}</p> : jevState !== 'available' ? <p className="field-help full-row">{t('admin.heartbeatJevHelp')}</p> : null}
            <div className="form-actions full-row">
              <button className="primary-button" disabled={patchConfig.isPending || checkJev.isPending}>{t('admin.saveJev')}</button>
              <button className="secondary-button" type="button" disabled={checkJev.isPending || patchConfig.isPending || jevState === 'not_configured'} onClick={() => checkJev.mutate()}>{t('admin.checkJev')}</button>
            </div>
            {checkJev.error ? <p className="form-error full-row" role="alert">{t('admin.jevCheckFailed')}</p> : null}
          </form>
        </Card>
        <Card title={t('admin.quotas')} description={t('admin.quotasDescription')}>
          <form className="form-grid" onSubmit={saveQuota}>
            <label>{t('admin.personalQuota')}<input name="quota_mib" type="number" min={1 / 1024 / 1024} step="any" defaultValue={value.quota_bytes / 1024 / 1024} required /></label>
            <label>{t('admin.sharedQuota')}<input name="shared_quota_mib" type="number" min={1 / 1024 / 1024} step="any" defaultValue={value.shared_workspace_quota_bytes / 1024 / 1024} required /></label>
            <div className="form-actions full-row"><button className="primary-button" disabled={patchConfig.isPending}>{t('admin.saveQuotas')}</button></div>
          </form>
        </Card>
        <Card title={t('admin.webFetch')} description={t('admin.webFetchDescription')}>
          <form className="form-grid" onSubmit={saveFetch}>
            <label className="full-row">{t('admin.denylist')}<textarea name="web_fetch_denylist" rows={7} defaultValue={value.web_fetch_denylist.join('\n')} /></label>
            <p className="field-help full-row">{t('admin.denylistHelp')}</p>
            <div className="form-actions full-row"><button className="primary-button" disabled={patchConfig.isPending}>{t('admin.saveNetwork')}</button></div>
          </form>
        </Card>
      </div>
    </div>
  )
}

export function AdminUsersPage(): ReactNode {
  const { t } = useTranslation()
  const currentUser = useAuthenticatedUser()
  const navigate = useNavigate()
  const client = useQueryClient()
  const [offset, setOffset] = useState(0)
  const users = useQuery({
    queryKey: ['admin-users', offset],
    queryFn: () => apiJson<AdminUser[]>(`/api/admin/users?limit=${USER_PAGE_SIZE}&offset=${offset}`),
  })
  const [pendingDelete, setPendingDelete] = useState<string | null>(null)
  const remove = useMutation({
    mutationFn: (userId: string) => apiJson(`/api/admin/users/${encodeURIComponent(userId)}`, { method: 'DELETE' }),
    onSuccess: (_result, userId) => {
      setPendingDelete(null)
      if (userId === currentUser.id) {
        client.clear()
        navigate('/login', { replace: true })
        return
      }
      void client.invalidateQueries({ queryKey: ['admin-users'] })
    },
  })

  return (
    <div className="page-scroll">
      <PageHeader eyebrow={t('nav.admin')} title={t('admin.users')} description={t('admin.usersDescription')} actions={<AdminTabs />} />
      <ErrorNotice error={users.error ?? remove.error} />
      <Card title={users.data ? t('admin.pageUsers', { count: users.data.length }) : t('admin.users')} description={t('admin.usersApiHelp')}>
        <div className="table-wrap">
          <table>
            <thead><tr><th>{t('admin.userColumn')}</th><th>{t('admin.identity')}</th><th>{t('admin.workspaceColumn')}</th><th>{t('admin.status')}</th><th><span className="sr-only">{t('admin.action')}</span></th></tr></thead>
            <tbody>{users.data?.map((user) => (
              <tr key={user.id}>
                <td><strong>{user.name}</strong><small>{user.email}</small></td>
                <td>{user.is_admin ? t('admin.adminRole') : t('admin.memberRole')}</td>
                <td>{formatBytes(user.bytes_used)} / {formatBytes(user.quota_bytes)}</td>
                <td><StatusBadge tone={user.locked ? 'danger' : 'success'}>{user.locked ? t('admin.overQuota') : t('common.normal')}</StatusBadge></td>
                <td>{pendingDelete === user.id ? <button className="danger-button" onClick={() => remove.mutate(user.id)}>{t('admin.confirmDelete')}</button> : <button className="danger-link" onClick={() => setPendingDelete(user.id)}>{t('common.delete')}</button>}</td>
              </tr>
            ))}</tbody>
          </table>
        </div>
        <div className="form-actions">
          <button className="secondary-button" data-testid="users-previous-page" disabled={offset === 0 || users.isFetching} onClick={() => setOffset((value) => Math.max(0, value - USER_PAGE_SIZE))}>{t('admin.previousPage')}</button>
          <span>{t('admin.pageNumber', { page: Math.floor(offset / USER_PAGE_SIZE) + 1 })}</span>
          <button className="secondary-button" data-testid="users-next-page" disabled={(users.data?.length ?? 0) < USER_PAGE_SIZE || users.isFetching} onClick={() => setOffset((value) => value + USER_PAGE_SIZE)}>{t('admin.nextPage')}</button>
        </div>
      </Card>
    </div>
  )
}

function serverMcpInput(view: ServerMcpServerConfigView): ServerMcpServerConfig {
  if (view.transport === 'stdio') {
    return { name: view.name, transport: 'stdio', command: view.command, args: view.args, cwd: view.cwd, env: view.env, enabled_capabilities: view.enabled_capabilities, max_concurrent_calls: view.max_concurrent_calls }
  }
  return { name: view.name, transport: view.transport, url: view.url, headers: view.headers, enabled_capabilities: view.enabled_capabilities, max_concurrent_calls: view.max_concurrent_calls }
}

type ServerMcpDraft = { baseRevision: number; servers: ServerMcpServerConfig[] }
type ServerMcpEditing = { baseRevision: number; server: ServerMcpServerConfig; catalog?: DiscoveredServer }
type RuntimeSlotState = ServerMcpResponse['runtimes'][string]

export function AdminMcpPage(): ReactNode {
  const { t } = useTranslation()
  const client = useQueryClient()
  const config = useQuery({
    queryKey: SERVER_MCP_KEY,
    queryFn: () => apiJson<ServerMcpResponse>('/api/admin/server-mcp'),
    refetchInterval: (query) => query.state.dataUpdateCount < MAX_RUNTIME_REFRESHES ? 5_000 : false,
    refetchIntervalInBackground: false,
  })
  const [draft, setDraft] = useState<ServerMcpDraft | null>(null)
  const [editing, setEditing] = useState<ServerMcpEditing | null>(null)
  const [formError, setFormError] = useState<string | null>(null)
  const servers = draft?.servers ?? config.data?.mcp_servers.map(serverMcpInput) ?? []
  const save = useMutation({
    mutationFn: (current: ServerMcpDraft) => apiJson<ServerMcpResponse>('/api/admin/server-mcp', {
      method: 'PUT',
      body: JSON.stringify({ base_config_revision: current.baseRevision, mcp_servers: current.servers }),
    }),
    onSuccess: (result) => {
      client.setQueryData(SERVER_MCP_KEY, result)
      setDraft(null)
      setEditing(null)
    },
  })
  const updateDraft = (nextServers: ServerMcpServerConfig[], baseRevision?: number): void => {
    if (!config.data) return
    setDraft((current) => ({
      baseRevision: current?.baseRevision ?? baseRevision ?? config.data.config_revision,
      servers: nextServers,
    }))
  }
  const add = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    if (!config.data) return
    const data = new FormData(event.currentTarget)
    const existing = editing?.server ?? servers.find((item) => item.name === String(data.get('name'))) ?? null
    const result = readMcpForm(data, existing, true, t)
    if (typeof result === 'string') {
      setFormError(result)
      return
    }
    setFormError(null)
    updateDraft([...servers.filter((item) => item.name !== result.name && item.name !== editing?.server.name), result as ServerMcpServerConfig], editing?.baseRevision)
    setEditing(null)
  }
  const runtimeOnlyNames = Object.entries(config.data?.runtimes ?? {})
    .filter(([, runtime]) => !runtime.configured)
    .map(([runtimeName]) => runtimeName)

  return (
    <div className="page-scroll">
      <PageHeader
        eyebrow={t('nav.admin')}
        title={t('admin.sharedMcp')}
        description={t('admin.sharedMcpDescription')}
        actions={(
          <>
            <AdminTabs />
            <button className="secondary-button" data-testid="server-mcp-refresh" onClick={() => void config.refetch()}>{t('common.refresh')}</button>
            <button className="primary-button" disabled={draft === null || save.isPending} onClick={() => draft && save.mutate(draft)}>{t('admin.saveSharedMcp')}</button>
          </>
        )}
      />
      <ErrorNotice error={config.error ?? save.error} />
      <div className="settings-stack">
        {servers.map((server) => {
          const runtime = config.data?.runtimes[server.name]
          const catalog = config.data?.mcp_discovered[server.name]
          return (
            <Card
              key={server.name}
              title={server.name}
              description={server.transport}
              actions={(
                <>
                  <button className="secondary-button" aria-label={t('mcp.editNamed', { name: server.name })} onClick={() => {
                    setEditing({ server, baseRevision: draft?.baseRevision ?? config.data?.config_revision ?? 0, catalog })
                    setFormError(null)
                  }}>{t('common.edit')}</button>
                  <button className="danger-link" aria-label={t('mcp.deleteNamed', { name: server.name })} onClick={() => updateDraft(servers.filter((item) => item.name !== server.name))}>{t('common.delete')}</button>
                </>
              )}
            >
              <div className="mcp-summary"><code>{server.transport === 'stdio' ? server.command : server.url}</code><span>{t('mcp.maxConcurrency')}: {server.max_concurrent_calls}</span></div>
              {runtime ? <RuntimeSlot runtime={runtime} /> : null}
              {catalog ? <><p className="field-help">{t('admin.installationPreview')}</p><CapabilityCatalog catalog={catalog} /></> : null}
            </Card>
          )
        })}
        {runtimeOnlyNames.map((runtimeName) => {
          const runtime = config.data?.runtimes[runtimeName]
          return (
            <Card key={runtimeName} title={runtimeName} description={t('admin.runtimeUnconfigured')}>
              {runtime ? <RuntimeSlot runtime={runtime} /> : null}
            </Card>
          )
        })}
        <Card title={editing ? t('mcp.editNamed', { name: editing.server.name }) : t('admin.addSharedMcp')} description={t('admin.addSharedMcpHelp')}>
          <McpForm
            key={editing?.server.name ?? 'new'}
            onSubmit={add}
            serverSide
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

function RuntimeSlot({ runtime }: { runtime: RuntimeSlotState }): ReactNode {
  const { t } = useTranslation()
  return (
    <div className="mcp-summary">
      <span>{t('admin.runtimeStats', { active: runtime.active_sessions, idle: runtime.idle_sessions, closing: runtime.closing_sessions, calls: runtime.active_calls })}</span>
      {runtime.last_error ? <span className="form-error">{runtime.last_error.message} <code>{runtime.last_error.code}</code></span> : null}
    </div>
  )
}

function AdminTabs(): ReactNode {
  const { t } = useTranslation()
  return <nav className="inline-tabs" aria-label={t('admin.adminPages')}><Link to="/admin/settings">{t('admin.settings')}</Link><Link to="/admin/users">{t('admin.users')}</Link><Link to="/admin/mcp">{t('admin.sharedMcp')}</Link></nav>
}

function formatBytes(value: number): string {
  if (value < 1024 * 1024) return `${Math.round(value / 1024)} KiB`
  return `${(value / 1024 / 1024).toFixed(1)} MiB`
}
