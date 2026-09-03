import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { ChannelsPage } from './Channels'
import { channelRefetchInterval, type ChannelConfig } from './api'

function config(
  channel: 'discord' | 'dingtalk',
  overrides: Partial<ChannelConfig> = {},
): ChannelConfig {
  return {
    channel,
    configured: false,
    state: 'stopped',
    bot: null,
    owner: null,
    allow_list: [],
    credential_hint: null,
    pairing: null,
    last_error: null,
    ...overrides,
  }
}

function json(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function renderPage(): QueryClient {
  const client = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  render(
    <QueryClientProvider client={client}>
      <MemoryRouter><ChannelsPage /></MemoryRouter>
    </QueryClientProvider>,
  )
  return client
}

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

afterEach(() => vi.unstubAllGlobals())

describe('ChannelsPage', () => {
  it('loads the fixed Discord and DingTalk cards with their current states', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => json([
      config('discord'),
      config('dingtalk', {
        configured: true,
        state: 'ready',
        bot: { id: 'ding-bot-1', name: 'Office helper', avatar_url: null },
        owner: { id: 'ding-owner-1', dm_chat_id: 'ding-dm-chat-1' },
        credential_hint: 'Configured',
      }),
    ])))

    renderPage()

    expect(screen.getByText('Loading channel settings…')).toBeInTheDocument()
    expect(await screen.findByRole('heading', { name: 'Discord' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'DingTalk' })).toBeInTheDocument()
    const discordCard = screen.getByRole('heading', { name: 'Discord' }).closest('.card')
    expect(discordCard).not.toBeNull()
    expect(within(discordCard as HTMLElement).getByText('Not configured')).toBeInTheDocument()
    expect(within(discordCard as HTMLElement).getByText(
      'Create a Discord bot, enable Message Content Intent, and grant View Channels, Read Message History, Send Messages, Send Messages in Threads, and Attach Files.',
    )).toBeInTheDocument()
    expect(screen.getByText('Ready')).toBeInTheDocument()
    expect(screen.getByText('Office helper')).toBeInTheDocument()
    expect(screen.getByText('ding-owner-1')).toBeInTheDocument()
    expect(screen.getByText('ding-dm-chat-1')).toBeInTheDocument()
    expect(screen.getByText('Paired owner ID')).toBeInTheDocument()
    expect(screen.getByText('Owner DM chat ID')).toBeInTheDocument()
    expect(screen.queryByDisplayValue('ding-owner-1')).not.toBeInTheDocument()
    expect(screen.queryByDisplayValue('ding-dm-chat-1')).not.toBeInTheDocument()
  })

  it.each([
    ['connecting', 'Connecting'],
    ['awaiting_pairing', 'Awaiting pairing'],
    ['ready', 'Ready'],
    ['degraded', 'Degraded'],
  ] as const)('shows the %s state', async (state, label) => {
    vi.stubGlobal('fetch', vi.fn(async () => json([
      config('discord', { configured: true, state }),
      config('dingtalk'),
    ])))

    renderPage()

    expect(await screen.findByText(label)).toBeInTheDocument()
  })

  it('shows stable runtime guidance for lowercase degraded codes without raw details', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => json([
      config('discord', {
        configured: true,
        state: 'degraded',
        last_error: {
          code: 'channel_runtime_start_failed',
          message: 'raw gateway exception and request body',
          at: '2026-09-02T12:00:00Z',
        },
      }),
      config('dingtalk'),
    ])))

    renderPage()

    expect(await screen.findByRole('alert')).toHaveTextContent(
      'The bot could not connect. Review its platform setup and permissions.',
    )
    expect(screen.queryByText(/raw gateway exception/)).not.toBeInTheDocument()
  })

  it('keeps both setup cards available when the list response is empty', async () => {
    vi.stubGlobal('fetch', vi.fn(async () => json([])))

    renderPage()

    expect(await screen.findByText('No saved channel settings were returned. You can configure either channel below.')).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'Discord' })).toBeInTheDocument()
    expect(screen.getByRole('heading', { name: 'DingTalk' })).toBeInTheDocument()
  })

  it('shows stable error copy and can retry a failed list request', async () => {
    let loads = 0
    vi.stubGlobal('fetch', vi.fn(async () => {
      loads += 1
      return loads === 1
        ? json({ code: 'CHANNEL_NOT_SUPPORTED', message: 'raw backend detail' }, 400)
        : json([config('discord'), config('dingtalk')])
    }))
    const user = userEvent.setup()

    renderPage()

    expect(await screen.findByRole('alert')).toHaveTextContent('This channel is not supported.')
    expect(screen.queryByText('raw backend detail')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Retry' }))
    expect(await screen.findByRole('heading', { name: 'Discord' })).toBeInTheDocument()
  })

  it('creates Discord with a secret and the whole normalized allow list without refilling the secret', async () => {
    const requests: unknown[] = []
    const initial = [config('discord'), config('dingtalk')]
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/channels' && !init?.method) return json(initial)
      if (url === '/api/channels/discord' && init?.method === 'PATCH') {
        requests.push(JSON.parse(String(init.body)))
        return json(config('discord', {
          configured: true,
          state: 'awaiting_pairing',
          bot: { id: 'discord-bot-1', name: 'Octo bot', avatar_url: null },
          credential_hint: 'Configured',
          allow_list: ['1001', '1002'],
          pairing: { code: 'PAIR-123', expires_at: new Date(Date.now() + 600_000).toISOString() },
        }))
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()

    renderPage()
    const token = await screen.findByLabelText('Bot Token')
    await user.type(token, 'discord-secret')
    await user.type(screen.getByLabelText('Allowed platform user IDs for Discord'), ' 1001\n\n1002 ')
    await user.click(screen.getByRole('button', { name: 'Save Discord' }))

    await waitFor(() => expect(requests).toEqual([{
      bot_token: 'discord-secret',
      allow_list: ['1001', '1002'],
    }]))
    expect(token).toHaveValue('')
    expect(screen.getByText('Configuration saved.')).toBeInTheDocument()
    expect(screen.getByText('PAIR-123')).toBeInTheDocument()
  })

  it('rejects duplicate allow-list IDs before sending the request', async () => {
    const fetchMock = vi.fn(async () => json([config('discord'), config('dingtalk')]))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    renderPage()
    await user.type(await screen.findByLabelText('Allowed platform user IDs for Discord'), '1001\n 1001')
    await user.type(screen.getByLabelText('Bot Token'), 'discord-secret')
    await user.click(screen.getByRole('button', { name: 'Save Discord' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Each platform user ID can appear only once.')
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('rejects more than 256 allow-list IDs before sending the request', async () => {
    const fetchMock = vi.fn(async () => json([config('discord'), config('dingtalk')]))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    renderPage()
    fireEvent.change(await screen.findByLabelText('Allowed platform user IDs for Discord'), {
      target: { value: Array.from({ length: 257 }, (_, index) => String(index)).join('\n') },
    })
    await user.click(screen.getByRole('button', { name: 'Save Discord' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Enter at most 256 platform user IDs.')
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['letters', 'discord', '123abc', 'Discord user IDs must contain 1 to 20 digits.'],
    ['more than 20 digits', 'discord', '123456789012345678901', 'Discord user IDs must contain 1 to 20 digits.'],
    ['more than 256 characters', 'dingtalk', '用'.repeat(257), 'DingTalk user IDs must contain 1 to 256 characters and no control characters.'],
    ['a control character', 'dingtalk', 'staff\u0007id', 'DingTalk user IDs must contain 1 to 256 characters and no control characters.'],
  ] as const)('rejects %s in the %s allow list before sending', async (_case, channel, value, message) => {
    const fetchMock = vi.fn(async () => json([config('discord'), config('dingtalk')]))
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    const platform = channel === 'discord' ? 'Discord' : 'DingTalk'

    renderPage()
    fireEvent.change(await screen.findByLabelText(`Allowed platform user IDs for ${platform}`), {
      target: { value },
    })
    await user.click(screen.getByRole('button', { name: `Save ${platform}` }))

    expect(await screen.findByRole('alert')).toHaveTextContent(message)
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('allows 256 shape-valid IDs through to the authoritative Server validation', async () => {
    const writes: unknown[] = []
    const saved = config('discord', {
      configured: true,
      state: 'ready',
      bot: { id: 'discord-bot-1', name: 'Octo bot', avatar_url: null },
      credential_hint: 'Configured',
    })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/channels' && !init?.method) return json([saved, config('dingtalk')])
      if (url === '/api/channels/discord' && init?.method === 'PATCH') {
        writes.push(JSON.parse(String(init.body)))
        return json({ code: 'CONFIG_VALIDATION_FAILED', message: 'server rejected full semantics' }, 400)
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()
    const ids = Array.from({ length: 256 }, (_, index) => String(10_000 + index))

    renderPage()
    fireEvent.change(await screen.findByLabelText('Allowed platform user IDs for Discord'), {
      target: { value: ids.join('\n') },
    })
    await user.click(screen.getByRole('button', { name: 'Save Discord' }))

    await waitFor(() => expect(writes).toEqual([{ allow_list: ids }]))
    expect(screen.getByRole('alert')).toHaveTextContent('The channel request could not be completed.')
    expect(screen.queryByText('server rejected full semantics')).not.toBeInTheDocument()
  })

  it('updates a saved channel with the complete allow list and omits blank credentials', async () => {
    const writes: unknown[] = []
    const saved = config('dingtalk', {
      configured: true,
      state: 'ready',
      bot: { id: 'ding-bot-1', name: 'Ding helper', avatar_url: null },
      owner: { id: 'owner-1', dm_chat_id: 'dm-chat-1' },
      allow_list: ['old-id'],
      credential_hint: 'Configured',
    })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/channels' && !init?.method) return json([config('discord'), saved])
      if (url === '/api/channels/dingtalk' && init?.method === 'PATCH') {
        writes.push(JSON.parse(String(init.body)))
        return json({ ...saved, allow_list: ['new-id'] })
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()

    renderPage()
    const allowList = await screen.findByLabelText('Allowed platform user IDs for DingTalk')
    expect(screen.getByLabelText('Client ID')).toHaveValue('')
    expect(screen.getByLabelText('Client Secret')).toHaveValue('')
    await user.clear(allowList)
    await user.type(allowList, ' new-id ')
    await user.click(screen.getByRole('button', { name: 'Save DingTalk' }))

    await waitFor(() => expect(writes).toEqual([{ allow_list: ['new-id'] }]))
  })

  it('creates DingTalk only when both credentials are present', async () => {
    const writes: unknown[] = []
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/channels' && !init?.method) return json([config('discord'), config('dingtalk')])
      if (url === '/api/channels/dingtalk' && init?.method === 'PATCH') {
        writes.push(JSON.parse(String(init.body)))
        return json(config('dingtalk', {
          configured: true,
          state: 'connecting',
          bot: { id: 'ding-bot-1', name: 'Ding helper', avatar_url: null },
          credential_hint: 'Configured',
          allow_list: ['staff-1'],
        }))
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()

    renderPage()
    await user.type(await screen.findByLabelText('Client ID'), 'client-id')
    await user.click(screen.getByRole('button', { name: 'Save DingTalk' }))
    expect(await screen.findByRole('alert')).toHaveTextContent('Enter both the DingTalk Client ID and Client Secret.')
    expect(writes).toHaveLength(0)
    await user.type(screen.getByLabelText('Client Secret'), 'client-secret')
    await user.type(screen.getByLabelText('Allowed platform user IDs for DingTalk'), ' staff-1 ')
    await user.click(screen.getByRole('button', { name: 'Save DingTalk' }))

    await waitFor(() => expect(writes).toEqual([{
      client_id: 'client-id',
      client_secret: 'client-secret',
      allow_list: ['staff-1'],
    }]))
  })

  it('shows a new pairing code once, can copy it, and requires confirmation before deletion', async () => {
    const requests: string[] = []
    const saved = config('discord', {
      configured: true,
      state: 'awaiting_pairing',
      bot: { id: 'discord-bot-1', name: 'Octo bot', avatar_url: null },
      credential_hint: 'Configured',
      pairing: { code: 'MUST-NOT-BE-SHOWN-FROM-GET', expires_at: '2026-09-02T12:10:00Z' },
    })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      requests.push(`${init?.method ?? 'GET'} ${url}`)
      if (url === '/api/channels' && !init?.method) return json([saved, config('dingtalk')])
      if (url === '/api/channels/discord/pairing' && init?.method === 'POST') {
        return json({
          ...saved,
          pairing: { code: 'FRESH-CODE', expires_at: new Date(Date.now() + 600_000).toISOString() },
        })
      }
      if (url === '/api/channels/discord' && init?.method === 'DELETE') {
        return new Response(null, { status: 204 })
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()
    const copy = vi.spyOn(navigator.clipboard, 'writeText')

    renderPage()
    expect(await screen.findByRole('button', { name: 'Generate new pairing code' })).toBeInTheDocument()
    expect(screen.queryByText('MUST-NOT-BE-SHOWN-FROM-GET')).not.toBeInTheDocument()
    expect(screen.queryByText('FRESH-CODE')).not.toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Generate new pairing code' }))
    expect(await screen.findByText('FRESH-CODE')).toBeInTheDocument()
    await user.click(screen.getByRole('button', { name: 'Copy pairing code' }))
    expect(copy).toHaveBeenCalledWith('FRESH-CODE')

    await user.click(screen.getByRole('button', { name: 'Delete Discord configuration' }))
    expect(screen.getByRole('button', { name: 'Confirm deletion of Discord' })).toHaveFocus()
    expect(requests.filter((request) => request === 'DELETE /api/channels/discord')).toHaveLength(0)
    await user.click(screen.getByRole('button', { name: 'Cancel' }))
    expect(screen.getByRole('button', { name: 'Delete Discord configuration' })).toHaveFocus()
    await user.click(screen.getByRole('button', { name: 'Delete Discord configuration' }))
    await user.click(screen.getByRole('button', { name: 'Confirm deletion of Discord' }))
    await waitFor(() => expect(requests).toContain('DELETE /api/channels/discord'))
    const discordCard = screen.getByRole('heading', { name: 'Discord' }).closest('.card')
    expect(discordCard).not.toBeNull()
    expect(within(discordCard as HTMLElement).getByText('Not configured')).toBeInTheDocument()
    expect(within(discordCard as HTMLElement).getByRole('button', { name: 'Save Discord' })).toHaveFocus()
  })

  it('refetches immediately when the page regains focus', async () => {
    let loads = 0
    vi.stubGlobal('fetch', vi.fn(async () => {
      loads += 1
      return json([
        config('discord', { configured: true, state: 'ready' }),
        config('dingtalk', { configured: true, state: 'ready' }),
      ])
    }))

    renderPage()
    await screen.findByRole('heading', { name: 'Discord' })
    expect(loads).toBe(1)
    await act(async () => {
      window.dispatchEvent(new Event('focus'))
    })
    await waitFor(() => expect(loads).toBeGreaterThan(1))
  })

  it('shows friendly stable error copy and preserves a saved degraded response', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/channels' && !init?.method) return json([config('discord'), config('dingtalk')])
      if (url === '/api/channels/discord' && init?.method === 'PATCH') {
        return json(config('discord', {
          configured: true,
          state: 'degraded',
          bot: { id: 'discord-bot-1', name: 'Octo bot', avatar_url: null },
          credential_hint: 'Configured',
          last_error: {
            code: 'CHANNEL_CREDENTIALS_UNVERIFIED',
            message: 'upstream request id should not be shown',
            at: '2026-09-02T12:00:00Z',
          },
        }))
      }
      throw new Error(`Unexpected request: ${init?.method ?? 'GET'} ${url}`)
    }))
    const user = userEvent.setup()

    renderPage()
    await user.type(await screen.findByLabelText('Bot Token'), 'discord-secret')
    await user.click(screen.getByRole('button', { name: 'Save Discord' }))

    expect(await screen.findByText('Configuration saved.')).toBeInTheDocument()
    expect(screen.getByText('Degraded')).toBeInTheDocument()
    expect(screen.getByRole('alert')).toHaveTextContent('The platform could not verify these credentials. Check the credentials and try again.')
    expect(screen.queryByText(/upstream request id/)).not.toBeInTheDocument()
  })
})

describe('channelRefetchInterval', () => {
  it('polls transitional states every 3 seconds, stable states every 30 seconds, and pauses while hidden', () => {
    expect(channelRefetchInterval([config('discord'), config('dingtalk')], true)).toBe(30_000)
    expect(channelRefetchInterval([
      config('discord', { configured: true, state: 'connecting' }),
      config('dingtalk'),
    ], true)).toBe(3_000)
    expect(channelRefetchInterval([
      config('discord', { configured: true, state: 'degraded' }),
      config('dingtalk'),
    ], true)).toBe(3_000)
    expect(channelRefetchInterval([
      config('discord', { configured: true, state: 'ready' }),
      config('dingtalk', { configured: true, state: 'ready' }),
    ], true)).toBe(30_000)
    expect(channelRefetchInterval([config('discord')], false)).toBe(false)
  })
})
