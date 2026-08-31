import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { AutomationsPage } from './Automations'

const userProfile = {
  id: '11111111-1111-4111-8111-111111111111',
  email: 'user@example.com',
  name: 'User',
  is_admin: false,
  timezone: 'Asia/Shanghai',
  created_at: '2026-08-26T12:00:00Z',
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function cronJob(id: string, name: string, sessionId: string | null = null) {
  return {
    id,
    name,
    schedule: { type: 'cron', cron_expr: '0 9 * * 1-5', tz: 'Asia/Shanghai' },
    session_id: sessionId,
    last_fired_at: null,
    next_fire_at: '2026-09-02T01:00:00Z',
    created_at: '2026-09-01T01:00:00Z',
  }
}

function renderPage(): void {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter>
        <AutomationsPage user={userProfile} />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

afterEach(() => vi.unstubAllGlobals())

describe('AutomationsPage', () => {
  it('loads every cron page and shows a missing Heartbeat file without creating it', async () => {
    const requests: Array<{ url: string; method: string }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      requests.push({ url, method })
      if (url === '/api/cron?limit=50&offset=0') {
        return json({ items: [cronJob('job-1', 'Morning report')], next_offset: 1 })
      }
      if (url === '/api/cron?limit=50&offset=1') {
        return json({ items: [cronJob('job-2', 'Evening report', 'job-2')], next_offset: null })
      }
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server') {
        return json({ code: 'workspace_not_found', message: 'Workspace file was not found' }, 404)
      }
      if (url === `/api/sessions/${userProfile.id}/messages?limit=1`) {
        return json({ code: 'session_not_found', message: 'Session was not found' }, 404)
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderPage()

    expect(await screen.findByRole('heading', { name: 'Automations' })).toBeInTheDocument()
    expect(await screen.findByText('Morning report')).toBeInTheDocument()
    expect(await screen.findByDisplayValue(/## Active Tasks/)).toBeInTheDocument()
    expect(requests.some((request) => request.method === 'PUT')).toBe(false)
    expect(screen.getByText('No heartbeat runs yet')).toBeInTheDocument()

    await userEvent.setup().click(screen.getByRole('button', { name: 'Load more' }))
    expect(await screen.findByText('Evening report')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'View history' })).toHaveAttribute(
      'href',
      '/chat/job-2?automation=cron',
    )
  })

  it('creates, edits, and deletes a cron job using mutually exclusive schedule fields', async () => {
    const writes: Array<{ url: string; method: string; body?: unknown }> = []
    const job = {
      ...cronJob('job-1', 'Morning report'),
      message: 'Prepare the report.',
    }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/cron?limit=50&offset=0') return json({ items: [cronJob('job-1', 'Morning report')], next_offset: null })
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server') return json({ code: 'workspace_not_found', message: 'missing' }, 404)
      if (url === `/api/sessions/${userProfile.id}/messages?limit=1`) return json({ code: 'session_not_found', message: 'missing' }, 404)
      if (url === '/api/cron/job-1' && method === 'GET') return json(job)
      if (url === '/api/cron/job-1' && method === 'PATCH') {
        writes.push({ url, method, body: JSON.parse(String(init?.body)) })
        return json({ ...job, name: 'Updated report' })
      }
      if (url === '/api/cron/job-1' && method === 'DELETE') {
        writes.push({ url, method })
        return new Response(null, { status: 204 })
      }
      if (url === '/api/cron' && method === 'POST') {
        writes.push({ url, method, body: JSON.parse(String(init?.body)) })
        return json({ ...job, id: 'job-2', name: 'One time task' }, 201)
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))
    const actor = userEvent.setup()

    renderPage()
    await screen.findByText('Morning report')
    await actor.click(screen.getByRole('button', { name: 'Create automation' }))
    await actor.type(screen.getByLabelText('Name'), 'One time task')
    await actor.type(screen.getByLabelText('Task'), 'Run once.')
    await actor.selectOptions(screen.getByLabelText('Schedule type'), 'at')
    expect(screen.queryByLabelText('Cron expression')).not.toBeInTheDocument()
    await actor.type(screen.getByLabelText('Run at'), '2026-09-03T10:00:00+08:00')
    await actor.click(screen.getByRole('button', { name: 'Create' }))

    await waitFor(() => expect(writes[0]).toEqual({
      url: '/api/cron',
      method: 'POST',
      body: {
        name: 'One time task',
        message: 'Run once.',
        at: '2026-09-03T10:00:00+08:00',
        tz: 'Asia/Shanghai',
      },
    }))

    await actor.click(screen.getByRole('button', { name: 'Edit Morning report' }))
    expect(await screen.findByDisplayValue('Prepare the report.')).toBeInTheDocument()
    await actor.clear(screen.getByLabelText('Name'))
    await actor.type(screen.getByLabelText('Name'), 'Updated report')
    await actor.click(screen.getByRole('button', { name: 'Save changes' }))
    await waitFor(() => expect(writes).toContainEqual({
      url: '/api/cron/job-1',
      method: 'PATCH',
      body: {
        name: 'Updated report',
        message: 'Prepare the report.',
        cron_expr: '0 9 * * 1-5',
        tz: 'Asia/Shanghai',
      },
    }))

    await actor.click(screen.getByRole('button', { name: 'Delete Morning report' }))
    expect(screen.getByText(/only stops future triggers/i)).toBeInTheDocument()
    await actor.click(screen.getByRole('button', { name: 'Confirm deletion' }))
    await waitFor(() => expect(writes).toContainEqual({ url: '/api/cron/job-1', method: 'DELETE' }))
  })

  it('preserves a one-shot instant and IANA timezone when editing the projected UTC schedule', async () => {
    const patches: unknown[] = []
    const summary = {
      ...cronJob('job-at', 'Shanghai reminder'),
      schedule: { type: 'at', at: '2026-09-03T02:00:00Z', tz: 'Asia/Shanghai' },
    }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/cron?limit=50&offset=0') return json({ items: [summary], next_offset: null })
      if (url === '/api/cron/job-at' && method === 'GET') return json({ ...summary, message: 'Send the reminder.' })
      if (url === '/api/cron/job-at' && method === 'PATCH') {
        patches.push(JSON.parse(String(init?.body)))
        return json({ ...summary, message: 'Send the reminder.' })
      }
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server') return json({ code: 'workspace_not_found', message: 'missing' }, 404)
      if (url === `/api/sessions/${userProfile.id}/messages?limit=1`) return json({ code: 'session_not_found', message: 'missing' }, 404)
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))
    const actor = userEvent.setup()

    renderPage()
    await actor.click(await screen.findByRole('button', { name: 'Edit Shanghai reminder' }))
    expect(await screen.findByLabelText('Run at')).toHaveValue('2026-09-03T10:00:00+08:00')
    await actor.click(screen.getByRole('button', { name: 'Save changes' }))

    await waitFor(() => expect(patches).toEqual([{
      name: 'Shanghai reminder',
      message: 'Send the reminder.',
      at: '2026-09-03T10:00:00+08:00',
      tz: 'Asia/Shanghai',
    }]))
  })

  it('saves the Heartbeat template only after the user clicks save', async () => {
    const writes: Array<{ method: string; headers: Headers; body: string }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/cron?limit=50&offset=0') return json({ items: [], next_offset: null })
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server' && method === 'GET') return json({ code: 'workspace_not_found', message: 'missing' }, 404)
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server' && method === 'PUT') {
        writes.push({ method, headers: new Headers(init?.headers), body: String(init?.body) })
        return new Response(JSON.stringify({ path: 'HEARTBEAT.md', etag: 'etag-1' }), {
          headers: { 'Content-Type': 'application/json', ETag: '"etag-1"' },
        })
      }
      if (url === `/api/sessions/${userProfile.id}/messages?limit=1`) return json({ code: 'session_not_found', message: 'missing' }, 404)
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderPage()
    expect(await screen.findByDisplayValue(/## Active Tasks/)).toBeInTheDocument()
    expect(writes).toHaveLength(0)
    await userEvent.setup().click(screen.getByRole('button', { name: 'Save HEARTBEAT.md' }))
    await waitFor(() => expect(writes).toHaveLength(1))
    expect(writes[0].headers.get('If-None-Match')).toBe('*')
    expect(writes[0].body).toContain('## Active Tasks')
  })

  it('keeps an edited Heartbeat file visible when saving fails', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/cron?limit=50&offset=0') return json({ items: [], next_offset: null })
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server' && method === 'GET') {
        return new Response('# Heartbeat\n\n## Active Tasks\n\nOld task', {
          headers: { 'Content-Type': 'text/plain', ETag: 'etag-old' },
        })
      }
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server' && method === 'PUT') {
        return json({ code: 'workspace_file_changed', message: 'The file changed elsewhere.' }, 412)
      }
      if (url === `/api/sessions/${userProfile.id}/messages?limit=1`) return json({ code: 'session_not_found', message: 'missing' }, 404)
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))
    const actor = userEvent.setup()

    renderPage()
    const editor = await screen.findByLabelText('HEARTBEAT.md')
    await actor.clear(editor)
    await actor.type(editor, '# Heartbeat\n\n## Active Tasks\n\nNew task')
    await actor.click(screen.getByRole('button', { name: 'Save HEARTBEAT.md' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('The file changed elsewhere.')
    expect(editor).toHaveValue('# Heartbeat\n\n## Active Tasks\n\nNew task')
  })
})
