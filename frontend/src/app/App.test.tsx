import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AppRoutes } from './App'

const regularUser = {
  id: 'user-1',
  email: 'user@example.com',
  name: 'Yucheng',
  is_admin: false,
  created_at: '2026-08-26T12:00:00Z',
}

function session(id: string, title: string) {
  return {
    id,
    user_id: regularUser.id,
    session_key: `web:${id}`,
    channel: 'web',
    chat_id: id,
    title,
    last_inbound_at: '2026-08-26T10:00:00Z',
    unread: false,
    cancel_requested: false,
    created_at: '2026-08-26T10:00:00Z',
  }
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function mockApi(user: typeof regularUser | null, sessions: unknown[] = []): void {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/me') {
        return user
          ? jsonResponse(user)
          : jsonResponse({ code: 'auth_unauthorized', message: 'Not authenticated' }, 401)
      }
      if (url.startsWith('/api/sessions')) return jsonResponse(sessions)
      if (url === '/api/devices') return jsonResponse([])
      throw new Error(`Unexpected request: ${url}`)
    }),
  )
}

function renderApp(path: string): void {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[path]}>
        <AppRoutes />
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

beforeEach(() => {
  vi.stubGlobal(
    'matchMedia',
    vi.fn().mockReturnValue({
      matches: false,
      addEventListener: vi.fn(),
      removeEventListener: vi.fn(),
    }),
  )
})

afterEach(() => vi.unstubAllGlobals())

describe('application routes', () => {
  it('redirects an unauthenticated browser route to login', async () => {
    mockApi(null)
    renderApp('/chat')
    expect(await screen.findByRole('heading', { name: 'Welcome back' })).toBeInTheDocument()
    expect(screen.queryByRole('navigation', { name: 'Main navigation' })).not.toBeInTheDocument()
  })

  it('renders authenticated navigation without admin controls for regular users', async () => {
    mockApi(regularUser)
    renderApp('/chat')
    expect(await screen.findByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Chat' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Workspace' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Devices' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Automations' })).toHaveAttribute('href', '/automations')
    expect(screen.queryByRole('link', { name: 'Admin settings' })).not.toBeInTheDocument()
  })

  it('routes an authenticated user to Automations', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/me') return jsonResponse(regularUser)
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url === '/api/cron?limit=50&offset=0') return jsonResponse({ items: [], next_offset: null })
      if (url === '/api/workspace/files/HEARTBEAT.md?openoctopus_device=server') {
        return jsonResponse({ code: 'workspace_not_found', message: 'missing' }, 404)
      }
      if (url === `/api/sessions/${regularUser.id}/messages?limit=1`) {
        return jsonResponse({ code: 'session_not_found', message: 'missing' }, 404)
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderApp('/automations')

    expect(await screen.findByRole('heading', { name: 'Automations' })).toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'Automations' })).toHaveAttribute('aria-current', 'page')
  })

  it('keeps every returned conversation reachable from the sidebar', async () => {
    const sessions = Array.from({ length: 9 }, (_, index) => ({
      id: `00000000-0000-4000-8000-00000000000${index}`,
      user_id: regularUser.id,
      session_key: `web:${index}`,
      channel: 'web',
      chat_id: `chat-${index}`,
      title: `Chat ${index + 1}`,
      last_inbound_at: `2026-08-26T10:00:0${index}Z`,
      unread: false,
      cancel_requested: false,
      created_at: `2026-08-26T10:00:0${index}Z`,
    }))
    mockApi(regularUser, sessions)

    renderApp('/chat')

    expect(await screen.findByRole('link', { name: /Chat 9/ })).toBeInTheDocument()
  })

  it('renames and deletes a conversation from its sidebar menu', async () => {
    let sessions = [session('session-1', 'Chat 1')]
    const requests: Array<{ url: string; method: string; body?: string }> = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      requests.push({ url, method, body: typeof init?.body === 'string' ? init.body : undefined })
      if (url === '/api/me') return jsonResponse(regularUser)
      if (url === '/api/devices') return jsonResponse([])
      if (url.startsWith('/api/sessions?')) return jsonResponse(sessions)
      if (url === '/api/sessions/session-1' && method === 'PATCH') {
        sessions = [{ ...sessions[0], title: 'Renamed chat' }]
        return jsonResponse(sessions[0])
      }
      if (url === '/api/sessions/session-1' && method === 'DELETE') {
        sessions = []
        return new Response(null, { status: 204 })
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderApp('/chat')
    const user = userEvent.setup()
    await user.click(await screen.findByRole('button', { name: 'Actions for Chat 1' }))
    await user.click(screen.getByRole('button', { name: 'Rename' }))
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Actions for Chat 1' })).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Actions for Chat 1' }))
    await user.click(screen.getByRole('button', { name: 'Rename' }))
    const title = screen.getByRole('textbox', { name: 'Conversation title' })
    await user.clear(title)
    await user.type(title, 'Renamed chat')
    await user.click(screen.getByRole('button', { name: 'Save' }))
    expect(await screen.findByRole('button', { name: 'Actions for Renamed chat' })).toHaveFocus()

    await user.click(await screen.findByRole('button', { name: 'Actions for Renamed chat' }))
    await user.click(screen.getByRole('button', { name: 'Delete' }))
    let confirmDelete = screen.getByRole('button', { name: 'Confirm deletion' })
    expect(confirmDelete).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Actions for Renamed chat' })).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Actions for Renamed chat' }))
    await user.click(screen.getByRole('button', { name: 'Delete' }))
    confirmDelete = screen.getByRole('button', { name: 'Confirm deletion' })
    await user.click(confirmDelete)

    await waitFor(() => expect(screen.queryByRole('link', { name: /Renamed chat/ })).not.toBeInTheDocument())
    expect(screen.getByRole('link', { name: 'New chat' })).toHaveFocus()
    expect(requests).toEqual(expect.arrayContaining([
      expect.objectContaining({ url: '/api/sessions/session-1', method: 'PATCH', body: JSON.stringify({ title: 'Renamed chat' }) }),
      expect.objectContaining({ url: '/api/sessions/session-1', method: 'DELETE' }),
    ]))
  })

  it('continues a bulk deletion after one selected conversation fails', async () => {
    let sessions = [session('session-1', 'Chat 1'), session('session-2', 'Chat 2')]
    const deleted: string[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/me') return jsonResponse(regularUser)
      if (url === '/api/devices') return jsonResponse([])
      if (url.startsWith('/api/sessions?')) return jsonResponse(sessions)
      if (url === '/api/sessions/session-1' && method === 'DELETE') {
        deleted.push('session-1')
        return jsonResponse({ code: 'session_has_cron_job', message: 'Delete the cron job first.' }, 409)
      }
      if (url === '/api/sessions/session-2' && method === 'DELETE') {
        deleted.push('session-2')
        sessions = sessions.filter((item) => item.id !== 'session-2')
        return new Response(null, { status: 204 })
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderApp('/chat')
    const user = userEvent.setup()
    await screen.findByRole('link', { name: /Chat 2/ })
    await user.click(screen.getByRole('button', { name: 'Manage conversations' }))
    expect(screen.getByRole('button', { name: 'Select all' })).toHaveFocus()
    await user.click(screen.getByRole('checkbox', { name: 'Select Chat 1' }))
    await user.click(screen.getByRole('checkbox', { name: 'Select Chat 2' }))
    await user.click(screen.getByRole('button', { name: 'Delete selected (2)' }))
    let confirmDelete = screen.getByRole('button', { name: 'Confirm deletion of 2 conversations' })
    expect(confirmDelete).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Delete selected (2)' })).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Delete selected (2)' }))
    confirmDelete = screen.getByRole('button', { name: 'Confirm deletion of 2 conversations' })
    await user.click(confirmDelete)

    await waitFor(() => expect(screen.queryByText('Chat 2')).not.toBeInTheDocument())
    expect(screen.getByText('Chat 1')).toBeInTheDocument()
    expect(screen.getByRole('alert')).toHaveTextContent('1 conversation could not be deleted')
    expect(screen.getByRole('button', { name: 'Delete selected (1)' })).toHaveFocus()
    expect(deleted).toEqual(['session-1', 'session-2'])
  })

  it('reconciles an ambiguous bulk delete from the refreshed session list', async () => {
    let sessions = [session('session-1', 'Chat 1')]
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/me') return jsonResponse(regularUser)
      if (url === '/api/devices') return jsonResponse([])
      if (url.startsWith('/api/sessions?')) return jsonResponse(sessions)
      if (url === '/api/sessions/session-1' && method === 'DELETE') {
        sessions = []
        throw new TypeError('Connection closed before the response arrived')
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderApp('/chat')
    const user = userEvent.setup()
    await screen.findByRole('link', { name: /Chat 1/ })
    await user.click(screen.getByRole('button', { name: 'Manage conversations' }))
    await user.click(screen.getByRole('checkbox', { name: 'Select Chat 1' }))
    await user.click(screen.getByRole('button', { name: 'Delete selected (1)' }))
    await user.click(screen.getByRole('button', { name: 'Confirm deletion of 1 conversation' }))

    await waitFor(() => expect(screen.queryByText('Chat 1')).not.toBeInTheDocument())
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'New chat' })).toHaveFocus()
  })

  it('keeps a confirmed bulk deletion successful when the final refresh fails', async () => {
    let deleted = false
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const method = init?.method ?? 'GET'
      if (url === '/api/me') return jsonResponse(regularUser)
      if (url === '/api/devices') return jsonResponse([])
      if (url.startsWith('/api/sessions?')) {
        if (deleted) throw new TypeError('Could not refresh conversations')
        return jsonResponse([session('session-1', 'Chat 1')])
      }
      if (url === '/api/sessions/session-1' && method === 'DELETE') {
        deleted = true
        return new Response(null, { status: 204 })
      }
      throw new Error(`Unexpected request: ${method} ${url}`)
    }))

    renderApp('/chat')
    const user = userEvent.setup()
    await screen.findByRole('link', { name: /Chat 1/ })
    await user.click(screen.getByRole('button', { name: 'Manage conversations' }))
    await user.click(screen.getByRole('checkbox', { name: 'Select Chat 1' }))
    await user.click(screen.getByRole('button', { name: 'Delete selected (1)' }))
    await user.click(screen.getByRole('button', { name: 'Confirm deletion of 1 conversation' }))

    await waitFor(() => expect(screen.queryByText('Chat 1')).not.toBeInTheDocument())
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
    expect(screen.getByRole('link', { name: 'New chat' })).toHaveFocus()
  })

  it('shows admin navigation only for an admin returned by /api/me', async () => {
    mockApi({ ...regularUser, is_admin: true })
    renderApp('/chat')
    expect(await screen.findByRole('link', { name: 'Admin settings' })).toBeInTheDocument()
  })

  it('keeps language and theme preferences on the account page', async () => {
    mockApi(regularUser)
    renderApp('/chat')
    const user = userEvent.setup()
    await screen.findByRole('navigation', { name: 'Main navigation' })
    expect(screen.queryByRole('button', { name: 'Theme: System' })).not.toBeInTheDocument()
    expect(screen.queryByRole('combobox', { name: 'Language' })).not.toBeInTheDocument()

    await user.click(screen.getByRole('link', { name: 'Account' }))
    expect(await screen.findByRole('heading', { name: 'Account' })).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'Language' })).toHaveValue('en')
    const toggle = await screen.findByRole('button', { name: 'Theme: System' })
    await user.click(toggle)
    expect(document.documentElement.dataset.theme).toBe('light')
    expect(toggle).toHaveAccessibleName('Theme: Light')
    await user.click(toggle)
    expect(document.documentElement.dataset.theme).toBe('dark')
    await user.click(toggle)
    await waitFor(() => expect(toggle).toHaveAccessibleName('Theme: System'))
  })

  it('applies a saved theme when opening a page without the preference control', async () => {
    window.localStorage.setItem('openoctopus-theme', 'dark')
    mockApi(regularUser)
    renderApp('/chat')
    await screen.findByRole('navigation', { name: 'Main navigation' })
    await waitFor(() => expect(document.documentElement.dataset.theme).toBe('dark'))
  })

  it('keeps the optional admin token on the registration page', async () => {
    mockApi(null)
    renderApp('/register')
    expect(await screen.findByRole('heading', { name: 'Create account' })).toBeInTheDocument()
    expect(screen.getByLabelText('Admin Token (optional)')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Theme: System' })).toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: 'Language' })).toHaveValue('en')
    expect(screen.queryByText(/safe|secure|安全/i)).not.toBeInTheDocument()
  })

  it('shows an admin-token mismatch before continuing as a regular member', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string | URL | Request) => {
        const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
        if (url === '/api/me') {
          return jsonResponse({ code: 'auth_unauthorized', message: 'Not authenticated' }, 401)
        }
        if (url === '/api/auth/register') return jsonResponse({ jwt: 'cookie-only', user: regularUser })
        if (url.startsWith('/api/sessions')) return jsonResponse([])
        if (url === '/api/devices') return jsonResponse([])
        throw new Error(`Unexpected request: ${url}`)
      }),
    )
    renderApp('/register')
    const user = userEvent.setup()

    await user.type(await screen.findByLabelText('Name'), 'Yucheng')
    await user.type(screen.getByLabelText('Email'), 'user@example.com')
    await user.type(screen.getByLabelText('Password'), 'password123')
    await user.type(screen.getByLabelText('Admin Token (optional)'), 'wrong-token')
    await user.click(screen.getByRole('button', { name: 'Create account' }))

    expect(await screen.findByText('The Admin Token did not match. A regular account was created.')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Create account' })).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Continue as member' }))
    expect(await screen.findByRole('navigation', { name: 'Main navigation' })).toBeInTheDocument()
  })
})
