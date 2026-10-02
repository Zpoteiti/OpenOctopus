import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthenticatedUserContext } from '../auth/context'
import i18n from '../i18n'
import { AdminMcpPage, AdminSettingsPage, AdminUsersPage } from './Admin'

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

const currentAdmin = {
  id: 'user-1', email: 'admin@example.com', name: 'Admin', is_admin: true,
  timezone: 'UTC',
  created_at: '2026-08-26T12:00:00Z',
}

function renderPage(node: React.ReactNode, path = '/'): QueryClient {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <AuthenticatedUserContext.Provider value={currentAdmin}>
        <MemoryRouter initialEntries={[path]}>{node}</MemoryRouter>
      </AuthenticatedUserContext.Provider>
    </QueryClientProvider>,
  )
  return client
}

afterEach(() => vi.unstubAllGlobals())

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

describe('admin pages', () => {
  it('uses structured provider fields and omits a blank API key', async () => {
    const patches: unknown[] = []
    const config = {
      quota_bytes: 524288000,
      shared_workspace_quota_bytes: 524288000,
      llm_endpoint: 'https://api.siliconflow.cn',
      llm_api_key: '<redacted>',
      llm_model: 'Qwen/Qwen3.5-4B',
      llm_max_context_tokens: 131072,
      llm_compaction_threshold_tokens: 16000,
      llm_max_concurrent_requests: 8,
      llm_max_output_tokens: 16384,
      default_soul: "You are OpenOctopus, the user's personal AI partner.",
      web_fetch_denylist: ['127.0.0.0/8'],
      jev_endpoint: null, jev_api_key: null, jev_status: { state: 'not_configured', checked_at: null },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json(config)
      }
      return json(config)
    }))

    renderPage(<AdminSettingsPage />)
    const user = userEvent.setup()
    expect(await screen.findByDisplayValue('https://api.siliconflow.cn')).toBeInTheDocument()
    expect(screen.getByLabelText('API Key')).toHaveValue('')
    expect(screen.getByLabelText('Compaction headroom')).toHaveAttribute('min', '4001')
    const maxConcurrent = screen.getByLabelText('Maximum concurrent requests')
    expect(maxConcurrent).toHaveAttribute('max', '1000000')
    expect(maxConcurrent.closest('label')).not.toBeNull()
    expect(screen.getByLabelText('Maximum output tokens')).toHaveAttribute('max', '1000000')
    expect(Number(screen.getByLabelText('Personal Workspace quota').getAttribute('min'))).toBeGreaterThan(0)
    await user.click(screen.getByRole('button', { name: 'Validate and save Provider' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).not.toHaveProperty('llm_api_key')
    expect(patches[0]).toMatchObject({ llm_endpoint: 'https://api.siliconflow.cn', llm_model: 'Qwen/Qwen3.5-4B' })
  })

  it('omits every blank optional Provider field instead of submitting null', async () => {
    const patches: Array<Record<string, unknown>> = []
    const config = {
      quota_bytes: 524288000,
      shared_workspace_quota_bytes: 524288000,
      llm_endpoint: null,
      llm_api_key: null,
      llm_model: null,
      llm_max_context_tokens: null,
      llm_compaction_threshold_tokens: null,
      llm_max_concurrent_requests: null,
      llm_max_output_tokens: 16384,
      default_soul: "You are OpenOctopus, the user's personal AI partner.",
      web_fetch_denylist: [],
      jev_endpoint: null, jev_api_key: null, jev_status: { state: 'not_configured', checked_at: null },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ ...config, llm_endpoint: 'https://provider.example', llm_api_key: '<redacted>', llm_model: 'model-1' })
      }
      return json(config)
    }))

    renderPage(<AdminSettingsPage />)
    const user = userEvent.setup()
    await user.type(await screen.findByLabelText('API base URL'), 'https://provider.example')
    await user.type(screen.getByLabelText('API Key'), 'secret')
    await user.type(screen.getByLabelText('Model'), 'model-1')
    await user.click(screen.getByRole('button', { name: 'Validate and save Provider' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toEqual({
      llm_endpoint: 'https://provider.example',
      llm_api_key: 'secret',
      llm_model: 'model-1',
      llm_max_output_tokens: 16384,
    })
  })

  it('shows Jev unavailable state, recovers after an explicit check, and retains the saved key on save', async () => {
    const patches: Array<Record<string, unknown>> = []
    const config = {
      quota_bytes: 524288000, shared_workspace_quota_bytes: 524288000,
      llm_endpoint: null, llm_api_key: null, llm_model: null, llm_max_context_tokens: null,
      llm_compaction_threshold_tokens: null, llm_max_concurrent_requests: null,
      llm_max_output_tokens: 16384, default_soul: 'Default identity', web_fetch_denylist: [],
      jev_endpoint: 'https://jev.example', jev_api_key: '<redacted>',
      jev_status: { state: 'unauthorized', checked_at: '2026-09-29T01:00:00Z' },
    }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/admin/config/jev/check' && init?.method === 'POST') return json({ state: 'available', checked_at: '2026-09-29T02:00:00Z' })
      if (url === '/api/admin/config' && init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ ...config, jev_status: { state: 'unchecked', checked_at: null } })
      }
      return json(config)
    }))

    renderPage(<AdminSettingsPage />)
    const user = userEvent.setup()
    expect(await screen.findByText('Dream is not available')).toBeInTheDocument()
    expect(screen.getAllByText(/Jev rejected the API key/).length).toBeGreaterThan(0)
    await user.click(screen.getByRole('button', { name: 'Check Jev connection' }))
    await waitFor(() => expect(screen.queryByText('Dream is not available')).not.toBeInTheDocument())
    await user.click(screen.getByRole('button', { name: 'Save Jev settings' }))
    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toEqual({ jev_endpoint: 'https://jev.example' })
    expect(patches[0]).not.toHaveProperty('jev_api_key')
    expect(patches[0]).not.toHaveProperty('jev_status')
    expect(screen.queryByText('Dream is not available')).not.toBeInTheDocument()
    expect(screen.getByText(/Check the Jev connection to confirm it is available/)).toBeInTheDocument()
  })

  it('saves the default SOUL independently from Provider configuration', async () => {
    const patches: unknown[] = []
    const config = {
      quota_bytes: 524288000,
      shared_workspace_quota_bytes: 524288000,
      llm_endpoint: null,
      llm_api_key: null,
      llm_model: null,
      llm_max_context_tokens: null,
      llm_compaction_threshold_tokens: null,
      llm_max_concurrent_requests: null,
      llm_max_output_tokens: 16384,
      default_soul: 'Default identity',
      web_fetch_denylist: [],
      jev_endpoint: null, jev_api_key: null, jev_status: { state: 'not_configured', checked_at: null },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ ...config, default_soul: 'Company-wide identity' })
      }
      return json(config)
    }))

    renderPage(<AdminSettingsPage />)
    const user = userEvent.setup()
    const soul = await screen.findByRole('textbox', { name: 'Default SOUL' })
    await user.clear(soul)
    await user.type(soul, 'Company-wide identity')
    await user.click(screen.getByRole('button', { name: 'Save default SOUL' }))

    await waitFor(() => expect(patches).toEqual([{ default_soul: 'Company-wide identity' }]))
  })

  it('describes user count as page-local and does not invent role controls', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => json([{
      id: 'user-1', email: 'member@example.com', name: 'Member', is_admin: false,
      created_at: '2026-08-26T12:00:00Z', quota_bytes: 500, bytes_used: 100, locked: false,
    }])))
    renderPage(<AdminUsersPage />)
    expect(await screen.findByText('1 user on this page')).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /Make administrator/ })).not.toBeInTheDocument()
  })

  it('paginates users with the API offset', async () => {
    const urls: string[] = []
    const firstPage = Array.from({ length: 50 }, (_, index) => ({
      id: `user-${index}`, email: `member-${index}@example.com`, name: `Member ${index}`, is_admin: false,
      created_at: '2026-08-26T12:00:00Z', quota_bytes: 500, bytes_used: 100, locked: false,
    }))
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      urls.push(url)
      return json(url.includes('offset=50') ? [] : firstPage)
    }))

    renderPage(<AdminUsersPage />)
    const user = userEvent.setup()
    await screen.findByText('Member 0')
    await user.click(screen.getByTestId('users-next-page'))

    await waitFor(() => expect(urls).toContain('/api/admin/users?limit=50&offset=50'))
  })

  it('clears the authenticated state when an administrator deletes themself', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/admin/users?limit=50&offset=0') {
        return json([{ ...currentAdmin, quota_bytes: 500, bytes_used: 100, locked: false }])
      }
      if (url === '/api/admin/users/user-1' && init?.method === 'DELETE') return new Response(null, { status: 204 })
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderPage(
      <Routes>
        <Route path="/admin/users" element={<AdminUsersPage />} />
        <Route path="/login" element={<p>signed out</p>} />
      </Routes>,
      '/admin/users',
    )
    const user = userEvent.setup()
    await screen.findByText('admin@example.com')
    await user.click(screen.getByRole('button', { name: 'Delete' }))
    await user.click(screen.getByRole('button', { name: 'Confirm deletion' }))

    expect(await screen.findByText('signed out')).toBeInTheDocument()
  })

  it('replaces the complete Server MCP list with its current revision', async () => {
    const puts: unknown[] = []
    const response = {
      config_revision: 4,
      mcp_servers: [{ name: 'calculator', transport: 'stdio', command: 'python', args: ['-m', 'calculator_mcp'], cwd: null, env: {}, enabled_capabilities: [], max_concurrent_calls: 1 }],
      mcp_catalog_digest: 'c'.repeat(64),
      mcp_discovered: {},
      runtimes: { calculator: { configured: true, active_sessions: 1, idle_sessions: 0, closing_sessions: 0, active_calls: 0, last_error: null } },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PUT') {
        puts.push(JSON.parse(String(init.body)))
        return json({ ...response, config_revision: 5, mcp_servers: [] })
      }
      return json(response)
    }))

    renderPage(<AdminMcpPage />)
    const user = userEvent.setup()
    expect(await screen.findByText('calculator')).toBeInTheDocument()
    expect(screen.getByText('1 active · 0 idle · 0 closing · 0 active calls')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Delete calculator' }))
    await user.click(screen.getByRole('button', { name: 'Save shared MCP' }))

    await waitFor(() => expect(puts).toEqual([{ base_config_revision: 4, mcp_servers: [] }]))
  })

  it('renders aggregate conversation runtime counts without per-conversation details, preserves redacted secrets, and freezes the draft revision', async () => {
    const puts: Array<Record<string, unknown>> = []
    const response = {
      config_revision: 4,
      mcp_servers: [
        { name: 'company_search', transport: 'streamable_http', url: 'https://search.example/mcp', headers: { Authorization: '<redacted>' }, enabled_capabilities: ['mcp_company_search_search'], max_concurrent_calls: 8 },
        { name: 'unused_search', transport: 'streamable_http', url: 'https://unused.example/mcp', headers: {}, enabled_capabilities: [], max_concurrent_calls: 8 },
      ],
      mcp_catalog_digest: 'c'.repeat(64),
      mcp_discovered: { company_search: { tools: [{ raw_name: 'search', final_name: 'mcp_company_search_search', enabled: true }], resources: [], resource_templates: [], prompts: [] } },
      runtimes: {
        company_search: { configured: true, active_sessions: 2, idle_sessions: 3, closing_sessions: 1, active_calls: 4, last_error: null },
        old_search: { configured: false, active_sessions: 0, idle_sessions: 0, closing_sessions: 1, active_calls: 0, last_error: { code: 'mcp_cleanup', message: 'A connection is still closing.' } },
      },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PUT') {
        puts.push(JSON.parse(String(init.body)))
        return json({ ...response, config_revision: 5 })
      }
      return json(response)
    }))

    const client = renderPage(<AdminMcpPage />)
    const user = userEvent.setup()
    expect(await screen.findByText('mcp_company_search_search')).toBeInTheDocument()
    expect(screen.getByText('old_search')).toBeInTheDocument()
    expect(screen.getByText('2 active · 3 idle · 1 closing · 4 active calls')).toBeInTheDocument()
    expect(screen.getByText('0 active · 0 idle · 1 closing · 0 active calls')).toBeInTheDocument()
    expect(screen.getByText('A connection is still closing.')).toBeInTheDocument()
    expect(screen.queryByText(/user-\d|conversation id/i)).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Edit company_search' }))
    expect(screen.getByLabelText(/Request headers/)).toHaveValue('Authorization=<redacted>')
    await user.click(screen.getByRole('button', { name: 'Delete unused_search' }))
    client.setQueryData(['server-mcp'], { ...response, config_revision: 9 })
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await user.click(screen.getByRole('button', { name: 'Save shared MCP' }))

    await waitFor(() => expect(puts).toHaveLength(1))
    expect(puts[0].base_config_revision).toBe(4)
    expect(puts[0].mcp_servers).toEqual([expect.objectContaining({ name: 'company_search', headers: { Authorization: '<redacted>' } })])
  })

  it('offers a manual runtime refresh', async () => {
    let requests = 0
    const response = { config_revision: 1, mcp_servers: [], mcp_catalog_digest: 'c'.repeat(64), mcp_discovered: {}, runtimes: {} }
    vi.stubGlobal('fetch', vi.fn(async () => {
      requests += 1
      return json(response)
    }))
    renderPage(<AdminMcpPage />)
    const user = userEvent.setup()
    await waitFor(() => expect(requests).toBe(1))
    await user.click(screen.getByTestId('server-mcp-refresh'))
    await waitFor(() => expect(requests).toBe(2))
  })
})
