import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { ThemeProvider } from '../theme/ThemeToggle'
import { AccountPage } from './Account'

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

afterEach(() => {
  vi.unstubAllGlobals()
  vi.restoreAllMocks()
})

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

describe('AccountPage', () => {
  it('requires an explicit choice and save before persisting the detected timezone', async () => {
    const originalResolvedOptions = Intl.DateTimeFormat.prototype.resolvedOptions
    vi.spyOn(Intl.DateTimeFormat.prototype, 'resolvedOptions').mockImplementation(function () {
      return { ...originalResolvedOptions.call(this), timeZone: 'Asia/Shanghai' }
    })
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      patches.push(JSON.parse(String(init?.body)))
      return json({ id: 'u1', email: 'user@example.com', name: 'User', is_admin: false, timezone: 'Asia/Shanghai', created_at: '2026-08-26T12:00:00Z' })
    }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <ThemeProvider>
          <MemoryRouter>
            <AccountPage user={{ id: 'u1', email: 'user@example.com', name: 'User', is_admin: false, timezone: 'UTC', created_at: '2026-08-26T12:00:00Z' }} />
          </MemoryRouter>
        </ThemeProvider>
      </QueryClientProvider>,
    )
    const actor = userEvent.setup()

    expect(screen.getByLabelText('Timezone')).toHaveValue('UTC')
    await actor.click(screen.getByRole('button', { name: 'Use detected timezone: Asia/Shanghai' }))
    expect(screen.getByLabelText('Timezone')).toHaveValue('Asia/Shanghai')
    expect(patches).toHaveLength(0)
    await actor.click(screen.getByRole('button', { name: 'Save timezone' }))
    await waitFor(() => expect(patches).toEqual([{ timezone: 'Asia/Shanghai' }]))
  })

  it('links directly to the personal SOUL and MEMORY files', () => {
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const { container } = render(
      <QueryClientProvider client={client}>
        <ThemeProvider>
          <MemoryRouter>
            <AccountPage user={{ id: 'u1', email: 'user@example.com', name: 'User', is_admin: false, timezone: 'UTC', created_at: '2026-08-26T12:00:00Z' }} />
          </MemoryRouter>
        </ThemeProvider>
      </QueryClientProvider>,
    )

    expect(screen.getByRole('link', { name: 'Edit SOUL.md' })).toHaveAttribute('href', '/workspace?path=SOUL.md')
    expect(screen.getByRole('link', { name: 'Edit MEMORY.md' })).toHaveAttribute('href', '/workspace?path=MEMORY.md')
    expect(container.querySelector('.settings-stack')?.querySelectorAll(':scope > .card')).toHaveLength(5)
  })

  it('updates only submitted profile fields', async () => {
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/me' && init?.method === 'PATCH') {
        patches.push(JSON.parse(String(init.body)))
        return json({ id: 'u1', email: 'user@example.com', name: 'New Name', is_admin: false, created_at: '2026-08-26T12:00:00Z' })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <ThemeProvider>
          <MemoryRouter>
            <AccountPage user={{ id: 'u1', email: 'user@example.com', name: 'Old Name', is_admin: false, timezone: 'UTC', created_at: '2026-08-26T12:00:00Z' }} />
          </MemoryRouter>
        </ThemeProvider>
      </QueryClientProvider>,
    )

    const user = userEvent.setup()
    await user.clear(screen.getByLabelText('Name'))
    await user.type(screen.getByLabelText('Name'), 'New Name')
    await user.click(screen.getByRole('button', { name: 'Save profile' }))
    await waitFor(() => expect(patches).toEqual([{ name: 'New Name', email: 'user@example.com' }]))
  })

  it('preserves password whitespace and clears the secret after a successful update', async () => {
    const patches: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      patches.push(JSON.parse(String(init?.body)))
      return json({ id: 'u1', email: 'user@example.com', name: 'User', is_admin: false, created_at: '2026-08-26T12:00:00Z' })
    }))
    const client = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    render(
      <QueryClientProvider client={client}>
        <ThemeProvider>
          <MemoryRouter>
            <AccountPage user={{ id: 'u1', email: 'user@example.com', name: 'User', is_admin: false, timezone: 'UTC', created_at: '2026-08-26T12:00:00Z' }} />
          </MemoryRouter>
        </ThemeProvider>
      </QueryClientProvider>,
    )

    const user = userEvent.setup()
    const password = screen.getByLabelText('New password')
    await user.type(password, ' pass word ')
    await user.click(screen.getByRole('button', { name: 'Save profile' }))

    await waitFor(() => expect(patches).toEqual([{
      name: 'User',
      email: 'user@example.com',
      password: ' pass word ',
    }]))
    expect(password).toHaveValue('')
  })
})
