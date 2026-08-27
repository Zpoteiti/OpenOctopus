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
    expect(screen.queryByRole('link', { name: 'Admin settings' })).not.toBeInTheDocument()
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
