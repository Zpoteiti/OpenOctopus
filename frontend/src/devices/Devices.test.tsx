import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter, Route, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { DeviceDetailPage, DeviceListPage, DeviceMcpPage } from './Devices'

const device = {
  id: '24a26586-0347-45a7-9bc0-bb5bc98988dc',
  name: 'laptop-cn',
  workspace_path: '~/openoctopus/workspace',
  restrict_to_workspace: true,
  ssrf_denylist: ['127.0.0.0/8'],
  shell_timeout_max: 600,
  env_allowlist: ['PATH', 'LANG'],
  config_revision: 7,
  mcp_config_count: 1,
  mcp_enabled_capability_count: 4,
  mcp_provider_visible_capability_count: 4,
  mcp_suppressed_capability_count: 0,
  mcp_catalog_digest: 'a'.repeat(64),
  online: true,
  token_hint: 'openoct…8m2k',
  created_at: '2026-08-26T12:00:00Z',
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

function renderPage(node: React.ReactNode, path = '/devices'): QueryClient {
  const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={[path]}>{node}</MemoryRouter>
    </QueryClientProvider>,
  )
  return client
}

afterEach(() => vi.unstubAllGlobals())

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

describe('device pages', () => {
  it('shows a clear placeholder instead of a fake Client download link', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([])
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderPage(<DeviceListPage />)
    await user.click(await screen.findByRole('button', { name: 'Download Client' }))

    expect(screen.getByText('Client downloads are not published yet. Release links and setup scripts will appear here.')).toBeInTheDocument()
    expect(screen.queryByRole('link', { name: 'Download Client' })).not.toBeInTheDocument()
  })

  it('lists real device fields and reveals a newly issued token once', async () => {
    const requests: Array<{ url: string; init?: RequestInit }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      requests.push({ url, init })
      if (url === '/api/devices' && init?.method === 'POST') {
        return json({ token: 'openoctopus_dev_secret', device }, 201)
      }
      if (url === '/api/devices') return json([device])
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderPage(<DeviceListPage />)
    expect(await screen.findByRole('link', { name: /laptop-cn/ })).toBeInTheDocument()
    expect(screen.getByText('Online')).toBeInTheDocument()

    const user = userEvent.setup()
    await user.click(screen.getByRole('button', { name: 'Add device' }))
    await user.type(screen.getByLabelText('Device name'), 'Devbox')
    await user.click(screen.getByRole('button', { name: 'Create device' }))

    expect(await screen.findByText('openoctopus_dev_secret')).toBeInTheDocument()
    const create = requests.find((request) => request.init?.method === 'POST')
    expect(JSON.parse(String(create?.init?.body))).toMatchObject({
      name: 'Devbox',
      workspace_path: '~/openoctopus/workspace',
      restrict_to_workspace: true,
    })
  })

  it('saves device policy with the current config revision and shows cross-platform path help', async () => {
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([device])
      if (url === '/api/devices/laptop-cn/config' && init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ device: { name: device.name, online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
      }
      if (url === '/api/devices/laptop-cn/config') {
        return json({ device: { name: device.name, online: true, config_revision: 7 }, mcp_servers: [], mcp_catalog_digest: 'a'.repeat(64), mcp_discovered: {} })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderPage(
      <Routes><Route path="/devices/:name" element={<DeviceDetailPage />} /></Routes>,
      '/devices/laptop-cn',
    )
    const user = userEvent.setup()
    expect(await screen.findByDisplayValue('~/openoctopus/workspace')).toBeInTheDocument()
    const help = screen.getByRole('button', { name: 'View path examples for each operating system' })
    expect(help).toHaveAttribute('title', expect.stringContaining('Windows'))
    await user.clear(screen.getByLabelText('Maximum shell runtime (seconds)'))
    await user.type(screen.getByLabelText('Maximum shell runtime (seconds)'), '900')
    await user.click(screen.getByRole('button', { name: 'Save device configuration' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toMatchObject({ base_config_revision: 7, shell_timeout_max: 900 })
  })

  it('keeps the policy form revision frozen while a dirty form is refetched', async () => {
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([device])
      if (url === '/api/devices/laptop-cn/config' && init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ device: { name: device.name, online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
      }
      if (url === '/api/devices/laptop-cn/config') {
        return json({ device: { name: device.name, online: true, config_revision: 7 }, mcp_servers: [], mcp_catalog_digest: 'a'.repeat(64), mcp_discovered: {} })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    const client = renderPage(
      <Routes><Route path="/devices/:name" element={<DeviceDetailPage />} /></Routes>,
      '/devices/laptop-cn',
    )
    const user = userEvent.setup()
    const timeout = await screen.findByLabelText('Maximum shell runtime (seconds)')
    await user.clear(timeout)
    await user.type(timeout, '900')
    client.setQueryData(['devices'], [{ ...device, config_revision: 8, shell_timeout_max: 1200 }])
    client.setQueryData(['device-config', 'laptop-cn'], { device: { name: device.name, online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
    await user.click(screen.getByRole('button', { name: 'Save device configuration' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toMatchObject({ base_config_revision: 7, shell_timeout_max: 900 })
  })

  it('freezes the policy revision when a field is focused before a refetch', async () => {
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([device])
      if (url === '/api/devices/laptop-cn/config' && init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ device: { name: device.name, online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
      }
      if (url === '/api/devices/laptop-cn/config') {
        return json({ device: { name: device.name, online: true, config_revision: 7 }, mcp_servers: [], mcp_catalog_digest: 'a'.repeat(64), mcp_discovered: {} })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    const client = renderPage(
      <Routes><Route path="/devices/:name" element={<DeviceDetailPage />} /></Routes>,
      '/devices/laptop-cn',
    )
    const user = userEvent.setup()
    const timeout = await screen.findByLabelText('Maximum shell runtime (seconds)')
    await user.click(timeout)
    client.setQueryData(['devices'], [{ ...device, config_revision: 8, shell_timeout_max: 1200 }])
    client.setQueryData(['device-config', 'laptop-cn'], { device: { name: device.name, online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
    await user.clear(timeout)
    await user.type(timeout, '900')
    await user.click(screen.getByRole('button', { name: 'Save device configuration' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0]).toMatchObject({ base_config_revision: 7, shell_timeout_max: 900 })
  })

  it('navigates to the canonical name returned by a successful rename', async () => {
    let renamed = false
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([renamed ? { ...device, name: 'laptop-us', config_revision: 8 } : device])
      if (url === '/api/devices/laptop-cn/config' && init?.method === 'PATCH') {
        renamed = true
        return json({ device: { name: 'laptop-us', online: true, config_revision: 8 }, mcp_servers: [], mcp_catalog_digest: 'b'.repeat(64), mcp_discovered: {} })
      }
      if (url === '/api/devices/laptop-cn/config') {
        return json({ device: { name: device.name, online: true, config_revision: 7 }, mcp_servers: [], mcp_catalog_digest: 'a'.repeat(64), mcp_discovered: {} })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderPage(
      <Routes>
        <Route path="/devices/:name" element={<DeviceDetailPage />} />
        <Route path="/devices/laptop-us" element={<p>canonical destination</p>} />
      </Routes>,
      '/devices/laptop-cn',
    )
    const user = userEvent.setup()
    const name = await screen.findByLabelText('Device name')
    await user.clear(name)
    await user.type(name, 'Laptop US')
    await user.click(screen.getByRole('button', { name: 'Save device configuration' }))

    expect(await screen.findByText('canonical destination')).toBeInTheDocument()
  })

  it('requires confirmation before invalidating the current device token', async () => {
    let rotations = 0
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/devices') return json([device])
      if (url === '/api/devices/laptop-cn/config') {
        return json({ device: { name: device.name, online: true, config_revision: 7 }, mcp_servers: [], mcp_catalog_digest: 'a'.repeat(64), mcp_discovered: {} })
      }
      if (url === '/api/devices/laptop-cn/regenerate-token' && init?.method === 'POST') {
        rotations += 1
        return json({ token: 'replacement-device-token' })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderPage(
      <Routes><Route path="/devices/:name" element={<DeviceDetailPage />} /></Routes>,
      '/devices/laptop-cn',
    )
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: 'Regenerate Token' }))

    expect(rotations).toBe(0)
    expect(screen.getByText('Regenerating immediately invalidates the old token and disconnects the Client.')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Confirm token regeneration' }))

    expect(await screen.findByText('replacement-device-token')).toBeInTheDocument()
    expect(rotations).toBe(1)
  })

  it('edits Device MCP from the redacted snapshot and freezes its revision', async () => {
    const patches: Array<Record<string, unknown>> = []
    const response = {
      device: { name: device.name, online: true, config_revision: 7 },
      mcp_servers: [
        { name: 'internal_docs', transport: 'streamable_http', url: 'https://docs.example/mcp', headers: { Authorization: '<redacted>' }, enabled_capabilities: ['mcp_internal_docs_search'], effective_status: 'active', shadowed_by: null },
        { name: 'unused_search', transport: 'streamable_http', url: 'https://unused.example/mcp', headers: {}, enabled_capabilities: [], effective_status: 'active', shadowed_by: null },
      ],
      mcp_catalog_digest: 'a'.repeat(64),
      mcp_discovered: { internal_docs: { tools: [{ raw_name: 'search', final_name: 'mcp_internal_docs_search', enabled: true, provider_visible: true, suppression_reason: null }], resources: [], resource_templates: [], prompts: [] } },
    }
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ ...response, device: { ...response.device, config_revision: 8 } })
      }
      return json(response)
    }))

    const client = renderPage(
      <Routes><Route path="/devices/:name/mcp" element={<DeviceMcpPage />} /></Routes>,
      '/devices/laptop-cn/mcp',
    )
    const user = userEvent.setup()
    expect(await screen.findByText('mcp_internal_docs_search')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Edit internal_docs' }))
    expect(screen.getByLabelText(/Request headers/)).toHaveValue('Authorization=<redacted>')
    await user.click(screen.getByRole('button', { name: 'Delete unused_search' }))
    client.setQueryData(['device-config', 'laptop-cn'], { ...response, device: { ...response.device, config_revision: 9 } })
    await user.click(screen.getByRole('button', { name: 'Save' }))
    await user.click(screen.getByRole('button', { name: 'Save Device MCP' }))

    await waitFor(() => expect(patches).toHaveLength(1))
    expect(patches[0].base_config_revision).toBe(7)
    expect(patches[0].mcp_servers).toEqual([expect.objectContaining({ name: 'internal_docs', headers: { Authorization: '<redacted>' } })])
  })

  it('rejects an empty exact selection and requires new secrets after a sink change', async () => {
    const response = {
      device: { name: device.name, online: true, config_revision: 7 },
      mcp_servers: [{ name: 'internal_docs', transport: 'streamable_http', url: 'https://docs.example/mcp', headers: { Authorization: '<redacted>' }, enabled_capabilities: [], effective_status: 'active', shadowed_by: null }],
      mcp_catalog_digest: 'a'.repeat(64),
      mcp_discovered: { internal_docs: { tools: [{ raw_name: 'search', final_name: 'mcp_internal_docs_search', enabled: true, provider_visible: true, suppression_reason: null }], resources: [], resource_templates: [], prompts: [] } },
    }
    vi.stubGlobal('fetch', vi.fn(async () => json(response)))
    renderPage(
      <Routes><Route path="/devices/:name/mcp" element={<DeviceMcpPage />} /></Routes>,
      '/devices/laptop-cn/mcp',
    )
    const user = userEvent.setup()
    await screen.findByText('internal_docs')

    await user.type(screen.getByLabelText('Service name'), 'new_search')
    await user.type(screen.getByLabelText('MCP URL'), 'https://new.example/mcp')
    await user.click(screen.getByRole('radio', { name: 'Select exact capabilities' }))
    await user.type(screen.getByLabelText('Final capability names (comma-separated)'), ' , , ')
    await user.click(screen.getByRole('button', { name: 'Add to configuration draft' }))
    expect(screen.getByRole('alert')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Edit internal_docs' }))
    const url = screen.getByLabelText('MCP URL')
    await user.clear(url)
    await user.type(url, 'https://new.example/mcp')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    expect(screen.getByRole('alert')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Save Device MCP' })).toBeDisabled()
  })
})
