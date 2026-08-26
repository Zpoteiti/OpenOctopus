import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { createMemoryRouter, MemoryRouter, Route, RouterProvider, Routes } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { ChatPage } from './ChatPage'

const baseSession = {
  id: '11111111-1111-4111-8111-111111111111',
  user_id: 'user-1',
  session_key: 'web:11111111-1111-4111-8111-111111111111',
  channel: 'web',
  chat_id: '11111111-1111-4111-8111-111111111111',
  title: 'New chat',
  last_inbound_at: '2026-08-26T10:00:00Z',
  unread: false,
  cancel_requested: false,
  created_at: '2026-08-26T10:00:00Z',
}

function jsonResponse(body: unknown, status = 200): Response {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' },
  })
}

function history(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    messages: [],
    pending_messages: [],
    status: 'idle',
    active_turn_id: null,
    last_message_id: null,
    pending_count: 0,
    has_more_before: false,
    ...overrides,
  }
}

function renderChat(path: string, pollIntervalMs = 5, idFactory?: () => string): void {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false }, mutations: { retry: false } },
  })
  render(
    <QueryClientProvider client={queryClient}>
      <MemoryRouter initialEntries={[path]}>
        <Routes>
          <Route path="/chat" element={<ChatPage pollIntervalMs={pollIntervalMs} idFactory={idFactory} />} />
          <Route path="/chat/:sessionId" element={<ChatPage pollIntervalMs={pollIntervalMs} idFactory={idFactory} />} />
        </Routes>
      </MemoryRouter>
    </QueryClientProvider>,
  )
}

beforeEach(async () => {
  await i18n.changeLanguage('zh-CN')
})

afterEach(async () => {
  vi.unstubAllGlobals()
  await i18n.changeLanguage('en')
})

describe('ChatPage', () => {
  it('uses English chat copy by default', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat('/chat')

    expect(await screen.findByRole('heading', { name: 'What would you like your agent to do?' })).toBeInTheDocument()
    expect(screen.getByRole('textbox', { name: 'Message' })).toBeInTheDocument()
    expect(screen.getByText('0 devices online')).toBeInTheDocument()
  })

  it('keeps selected browser attachments after clearing the file input', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat('/chat')

    const input = document.querySelector<HTMLInputElement>('input.chat-file-input')
    expect(input).not.toBeNull()
    await user.upload(input!, new File(['attachment'], 'context.txt', { type: 'text/plain' }))

    expect(await screen.findByText('context.txt')).toBeInTheDocument()
    expect(input).toHaveValue('')
  })

  it('renders non-web sessions as browser read-only', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([{ ...baseSession, channel: 'discord', session_key: 'discord:1' }])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`)

    expect(await screen.findByText('此会话来自 discord，只能在浏览器中查看。')).toBeInTheDocument()
    expect(screen.queryByRole('textbox', { name: '消息' })).not.toBeInTheDocument()
  })

  it('loads later session pages before deciding a deep-linked conversation is unavailable', async () => {
    const firstPage = Array.from({ length: 200 }, (_, index) => ({
      ...baseSession,
      id: `session-${index}`,
      chat_id: `session-${index}`,
      session_key: `web:session-${index}`,
    }))
    const oldSession = {
      ...baseSession,
      id: '22222222-2222-4222-8222-222222222222',
      chat_id: '22222222-2222-4222-8222-222222222222',
      session_key: 'web:22222222-2222-4222-8222-222222222222',
      title: 'Older web chat',
    }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse(firstPage)
      if (url === '/api/sessions?limit=200&offset=200') return jsonResponse([oldSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes(`/api/sessions/${oldSession.id}/messages?limit=200`)) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${oldSession.id}`)

    expect(await screen.findByRole('textbox', { name: '消息' })).toBeInTheDocument()
    expect(screen.queryByText('此会话不存在，或当前账号无权访问。')).not.toBeInTheDocument()
  })

  it('opens an existing conversation at its latest saved message', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [
            {
              id: 'message-1', session_id: baseSession.id, role: 'user', message_kind: 'human',
              content: [{ type: 'text', text: 'Earlier question' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
            {
              id: 'message-2', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'Latest answer' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:02Z',
            },
          ],
          last_message_id: 'message-2',
        }))
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`)
    const scroll = document.querySelector<HTMLElement>('.chat-scroll')
    expect(scroll).not.toBeNull()
    Object.defineProperty(scroll, 'scrollHeight', { configurable: true, value: 640 })

    expect(await screen.findByText('Latest answer')).toBeInTheDocument()
    await waitFor(() => expect(scroll?.scrollTop).toBe(640))
  })

  it('only performs the initial scroll once while a recovered turn is still running', async () => {
    let finishSecondPage: ((response: Response) => void) | undefined
    const secondPage = new Promise<Response>((resolve) => { finishSecondPage = resolve })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{
            id: 'message-1', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'Running answer' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          status: 'running', active_turn_id: 'turn-1', last_message_id: 'message-1',
        }))
      }
      if (url.includes('/messages?after=message-1')) return secondPage
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`, 1)
    const scroll = document.querySelector<HTMLElement>('.chat-scroll')
    expect(scroll).not.toBeNull()
    Object.defineProperty(scroll, 'scrollHeight', { configurable: true, value: 640 })

    expect(await screen.findByText('Running answer')).toBeInTheDocument()
    await waitFor(() => expect(finishSecondPage).toBeDefined())
    expect(scroll?.scrollTop).toBe(640)
    if (scroll) scroll.scrollTop = 120
    await act(async () => {
      finishSecondPage?.(jsonResponse(history({
        messages: [{
          id: 'message-2', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
          content: [{ type: 'text', text: 'Finished answer' }], delivery_refs: [], is_compacted: false,
          created_at: '2026-08-26T10:00:02Z',
        }],
        last_message_id: 'message-2',
      })))
    })

    expect(await screen.findByText('Finished answer')).toBeInTheDocument()
    expect(scroll?.scrollTop).toBe(120)
  })

  it('uses message_kind rather than Provider wire role for transcript authorship', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{
            id: 'tool-result-1', session_id: baseSession.id, role: 'user', message_kind: 'tool_result',
            content: [{ type: 'text', text: 'Device command output' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          last_message_id: 'tool-result-1',
        }))
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`)

    const row = (await screen.findByText('Device command output')).closest('article')
    expect(row).not.toBeNull()
    expect(row?.querySelector('header strong')).toHaveTextContent('工具结果')
    expect(row).toHaveClass('chat-message-assistant')
  })

  it('polls persisted history with an after cursor while a turn is running', async () => {
    let historyCalls = 0
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        historyCalls += 1
        return jsonResponse(history({
          messages: [{
            id: 'message-1', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'first' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          status: 'running', active_turn_id: 'turn-1', last_message_id: 'message-1',
        }))
      }
      if (url.includes('/messages?after=message-1')) {
        historyCalls += 1
        return jsonResponse(history({
          messages: [{
            id: 'message-2', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'finished' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:02Z',
          }],
          last_message_id: 'message-2',
        }))
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat(`/chat/${baseSession.id}`)

    expect(await screen.findByText('first')).toBeInTheDocument()
    expect(await screen.findByText('finished')).toBeInTheDocument()
    expect(historyCalls).toBe(2)
    expect(fetchMock).toHaveBeenCalledWith(
      `/api/sessions/${baseSession.id}/messages?after=message-1&limit=200`,
      expect.anything(),
    )
  })

  it('keeps recovering while durable pending messages are waiting between turns', async () => {
    let historyCalls = 0
    let finishSecondHistory: ((response: Response) => void) | undefined
    const secondHistory = new Promise<Response>((resolve) => { finishSecondHistory = resolve })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        historyCalls += 1
        if (historyCalls === 1) {
          return jsonResponse(history({
            pending_messages: [{
              id: 'pending-1', session_id: baseSession.id,
              content: [{ type: 'text', text: 'Queued question' }], effort: null,
              received_at: '2026-08-26T10:00:01Z',
            }],
            pending_count: 1,
          }))
        }
        return secondHistory
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`, 1)

    expect(await screen.findByText('Queued question')).toBeInTheDocument()
    await waitFor(() => expect(historyCalls).toBe(2))
    await act(async () => {
      finishSecondHistory?.(jsonResponse(history({
        messages: [{
          id: 'message-1', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
          content: [{ type: 'text', text: 'Queued answer' }], delivery_refs: [], is_compacted: false,
          created_at: '2026-08-26T10:00:02Z',
        }],
        last_message_id: 'message-1',
      })))
    })
    expect(await screen.findByText('Queued answer')).toBeInTheDocument()
    expect(historyCalls).toBe(2)
  })

  it('requests a stop for the current running turn', async () => {
    let cancelRequests = 0
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({ status: 'running', active_turn_id: 'turn-1' }))
      }
      if (url.endsWith('/cancel') && init?.method === 'POST') {
        cancelRequests += 1
        return jsonResponse({ cancel_requested: true })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`, 10_000)
    await userEvent.click(await screen.findByRole('button', { name: '停止' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('已请求在下一个可停止点停止')
    expect(cancelRequests).toBe(1)
  })

  it('clears composer and rename drafts when navigation changes the session', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', title: 'Session A' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', title: 'Session B' }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    }))
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await user.type(await screen.findByRole('textbox', { name: '消息' }), 'Session A draft')
    const fileInput = document.querySelector<HTMLInputElement>('input.chat-file-input')
    expect(fileInput).not.toBeNull()
    await user.upload(fileInput!, new File(['attachment'], 'session-a.txt', { type: 'text/plain' }))
    await user.click(screen.getByRole('button', { name: '重命名' }))
    const titleInput = screen.getByRole('textbox', { name: '会话标题' })
    await user.clear(titleInput)
    await user.type(titleInput, 'A title draft')

    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })

    expect(await screen.findByRole('textbox', { name: '消息' })).toHaveValue('')
    expect(screen.queryByText('session-a.txt')).not.toBeInTheDocument()
    expect(screen.queryByRole('textbox', { name: '会话标题' })).not.toBeInTheDocument()
  })

  it('keeps every composer control disabled while another session is sending', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url.includes('/api/workspace/files/') && init?.method === 'PUT') return jsonResponse({ created: true })
      if (url === `/api/sessions/${sessionA.id}/messages` && init?.method === 'POST') {
        return new Response(new ReadableStream<Uint8Array>({
          start(controller) { streamController = controller },
        }), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await user.type(await screen.findByRole('textbox', { name: '消息' }), 'Run A')
    const fileInput = document.querySelector<HTMLInputElement>('input.chat-file-input')
    expect(fileInput).not.toBeNull()
    await user.upload(fileInput!, new File(['attachment'], 'context.txt', { type: 'text/plain' }))
    await user.click(screen.getByRole('button', { name: '发送消息' }))
    await waitFor(() => expect(streamController).toBeDefined())

    expect(screen.getByRole('textbox', { name: '消息' })).toBeDisabled()
    expect(fileInput).toBeDisabled()
    expect(screen.getByRole('button', { name: '移除' })).toBeDisabled()
    expect(document.querySelector<HTMLButtonElement>('.composer-actions .text-button')).toBeDisabled()

    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })

    expect(await screen.findByRole('textbox', { name: '消息' })).toBeDisabled()
    expect(document.querySelector<HTMLInputElement>('input.chat-file-input')).toBeDisabled()
    expect(document.querySelector<HTMLButtonElement>('.composer-actions .text-button')).toBeDisabled()
    expect(screen.getByRole('button', { name: '发送消息' })).toBeDisabled()
    expect(fetchMock.mock.calls.filter(([url, init]) => String(url).endsWith('/messages') && init?.method === 'POST')).toHaveLength(1)

    await act(async () => streamController?.close())
  })

  it('re-enables the composer before a session-list refresh finishes', async () => {
    let sessionLoads = 0
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined
    const pendingRefresh = new Promise<Response>(() => undefined)
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') {
        sessionLoads += 1
        return sessionLoads <= 2 ? jsonResponse([baseSession]) : pendingRefresh
      }
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${baseSession.id}/messages` && init?.method === 'POST') {
        return new Response(new ReadableStream<Uint8Array>({
          start(controller) { streamController = controller },
        }), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    renderChat(`/chat/${baseSession.id}`)
    const user = userEvent.setup()

    const composer = await screen.findByRole('textbox', { name: '消息' })
    await user.type(composer, 'Run task')
    await user.click(screen.getByRole('button', { name: '发送消息' }))
    await waitFor(() => expect(streamController).toBeDefined())
    expect(composer).toBeDisabled()

    await act(async () => streamController?.close())

    await waitFor(() => expect(sessionLoads).toBe(3))
    await waitFor(() => expect(composer).toBeEnabled())
    expect(document.querySelector<HTMLInputElement>('input.chat-file-input')).toBeEnabled()
  })

  it('does not let a late rename success close the next session editor', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', title: 'Session A' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', title: 'Session B' }
    let finishRename: ((response: Response) => void) | undefined
    const renameResponse = new Promise<Response>((resolve) => { finishRename = resolve })
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${sessionA.id}` && init?.method === 'PATCH') return renameResponse
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '重命名' }))
    await user.clear(screen.getByRole('textbox', { name: '会话标题' }))
    await user.type(screen.getByRole('textbox', { name: '会话标题' }), 'Renamed A')
    await user.click(screen.getByRole('button', { name: '保存' }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      `/api/sessions/${sessionA.id}`,
      expect.objectContaining({ method: 'PATCH' }),
    ))

    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })
    await user.click(screen.getByRole('button', { name: '重命名' }))
    const sessionBTitle = screen.getByRole('textbox', { name: '会话标题' })
    await user.clear(sessionBTitle)
    await user.type(sessionBTitle, 'Session B draft')
    await act(async () => { finishRename?.(jsonResponse({ ...sessionA, title: 'Renamed A' })) })

    expect(screen.getByRole('textbox', { name: '会话标题' })).toHaveValue('Session B draft')
  })

  it('does not surface a late rename failure after leaving its target session', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', title: 'Session A' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', title: 'Session B' }
    let finishRename: ((response: Response) => void) | undefined
    const renameResponse = new Promise<Response>((resolve) => { finishRename = resolve })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${sessionA.id}` && init?.method === 'PATCH') return renameResponse
      throw new Error(`Unexpected request: ${url}`)
    }))
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '重命名' }))
    await user.click(screen.getByRole('button', { name: '保存' }))
    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })
    await act(async () => {
      finishRename?.(jsonResponse({ code: 'session_conflict', message: 'Rename rejected.' }, 409))
      await Promise.resolve()
    })
    await act(async () => { await router.navigate(`/chat/${sessionA.id}`) })

    expect(screen.queryByText(/Rename rejected/)).not.toBeInTheDocument()
  })

  it('resets destructive confirmation when navigation changes the target session', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    }))
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '删除' }))
    expect(screen.getByRole('button', { name: '确认删除' })).toBeInTheDocument()

    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })

    expect(await screen.findByRole('button', { name: '删除' })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: '确认删除' })).not.toBeInTheDocument()
  })

  it('does not navigate away from a new session when an earlier deletion completes late', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }
    let finishDelete: ((response: Response) => void) | undefined
    const deleteResponse = new Promise<Response>((resolve) => { finishDelete = resolve })
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${sessionA.id}` && init?.method === 'DELETE') return deleteResponse
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat', element: <ChatPage pollIntervalMs={5} /> },
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '删除' }))
    await user.click(screen.getByRole('button', { name: '确认删除' }))
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      `/api/sessions/${sessionA.id}`,
      expect.objectContaining({ method: 'DELETE' }),
    ))
    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })
    await act(async () => { finishDelete?.(new Response(null, { status: 204 })) })

    expect(await screen.findByRole('button', { name: '删除' })).toBeEnabled()
    expect(router.state.location.pathname).toBe(`/chat/${sessionB.id}`)
  })

  it('shows the stable Server code for Chat control errors', async () => {
    let deleteRequests = 0
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${baseSession.id}` && init?.method === 'DELETE') {
        deleteRequests += 1
        const message = deleteRequests === 1
          ? 'Delete the cron job first.'
          : 'Delete the cron job first. (session_has_cron_job)'
        return jsonResponse({ code: 'session_has_cron_job', message }, 409)
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()
    renderChat(`/chat/${baseSession.id}`)

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '删除' }))
    await user.click(screen.getByRole('button', { name: '确认删除' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Delete the cron job first. (session_has_cron_job)')
    await user.click(screen.getByRole('button', { name: '删除' }))
    await user.click(screen.getByRole('button', { name: '确认删除' }))
    await waitFor(() => {
      expect(deleteRequests).toBe(2)
      expect(screen.getByRole('alert').textContent).toBe('Delete the cron job first. (session_has_cron_job)')
    })
  })

  it('advances recovery with the last returned row instead of the session-global latest id', async () => {
    const messages = Array.from({ length: 200 }, (_, index) => ({
      id: `message-${index + 1}`,
      session_id: baseSession.id,
      role: 'assistant',
      message_kind: 'assistant',
      content: [{ type: 'text', text: `chunk ${index + 1}` }],
      delivery_refs: [],
      is_compacted: false,
      created_at: `2026-08-26T10:${String(Math.floor(index / 60)).padStart(2, '0')}:${String(index % 60).padStart(2, '0')}Z`,
    }))
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{ ...messages[0], id: 'anchor', content: [{ type: 'text', text: 'anchor' }] }],
          status: 'running',
          last_message_id: 'anchor',
        }))
      }
      if (url.includes('/messages?after=anchor')) {
        return jsonResponse(history({ messages, status: 'idle', last_message_id: 'message-400' }))
      }
      if (url.includes('/messages?after=message-200')) {
        return jsonResponse(history({
          messages: [{ ...messages[0], id: 'message-201', content: [{ type: 'text', text: 'caught up' }] }],
          status: 'idle',
          last_message_id: 'message-400',
        }))
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat(`/chat/${baseSession.id}`, 1)

    expect(await screen.findByText('caught up')).toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('after=message-200'))).toBe(true)
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('after=message-400'))).toBe(false)
  })

  it('marks the latest visible message as read and offers explicit recovery', async () => {
    let historyCalls = 0
    let revealLatest: (() => void) | undefined
    vi.stubGlobal('IntersectionObserver', class {
      constructor(callback: IntersectionObserverCallback) {
        revealLatest = () => callback([{ isIntersecting: true } as IntersectionObserverEntry], this as unknown as IntersectionObserver)
      }
      observe(): void {}
      disconnect(): void {}
    })
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([{ ...baseSession, unread: true }])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        historyCalls += 1
        return jsonResponse(history({
          messages: [{
            id: 'message-1', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'visible answer' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          last_message_id: 'message-400',
        }))
      }
      if (url.includes('/messages?after=message-1')) {
        historyCalls += 1
        return jsonResponse(history({
          messages: [{
            id: 'message-400', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'latest answer' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:02Z',
          }],
          last_message_id: 'message-400',
        }))
      }
      if (url === `/api/sessions/${baseSession.id}` && init?.method === 'PATCH') {
        return jsonResponse({ ...baseSession, unread: false })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat(`/chat/${baseSession.id}`, 10_000)

    expect(await screen.findByText('visible answer')).toBeInTheDocument()
    expect(fetchMock.mock.calls.some(([, init]) => init?.method === 'PATCH')).toBe(false)
    await act(async () => revealLatest?.())
    await waitFor(() => expect(fetchMock).toHaveBeenCalledWith(
      `/api/sessions/${baseSession.id}`,
      expect.objectContaining({
        method: 'PATCH',
        body: JSON.stringify({ read_through_message_id: 'message-1' }),
      }),
    ))

    await userEvent.click(screen.getByRole('button', { name: '刷新' }))
    await waitFor(() => expect(historyCalls).toBe(2))
  })

  it('shows a clear ambiguous-send warning without retrying', async () => {
    const user = userEvent.setup()
    const sessionId = '22222222-2222-4222-8222-222222222222'
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (init?.method === 'POST' && url.includes('/messages')) throw new TypeError('network lost')
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat('/chat', 5, () => sessionId)
    const textbox = await screen.findByRole('textbox', { name: '消息' })
    await user.type(textbox, 'hello')
    await user.click(screen.getByRole('button', { name: '发送消息' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Server 确认前连接中断')
    const postCalls = fetchMock.mock.calls.filter(([url, init]) => String(url).includes('/messages') && init?.method === 'POST')
    expect(postCalls).toHaveLength(1)
    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => (
      String(url).includes(`/api/sessions/${sessionId}/messages?limit=200`) && init?.method === undefined
    ))).toBe(true))
  })

  it('fences late stream events when the user navigates to another conversation', async () => {
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined
    const encoder = new TextEncoder()
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes(`/api/sessions/${sessionA.id}/messages?limit=200`)) return jsonResponse(history())
      if (url.includes(`/api/sessions/${sessionB.id}/messages?limit=200`)) {
        return jsonResponse(history({
          messages: [{
            id: 'message-b', session_id: sessionB.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'B canonical answer' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          last_message_id: 'message-b',
        }))
      }
      if (url === `/api/sessions/${sessionA.id}/messages` && init?.method === 'POST') {
        return new Response(new ReadableStream<Uint8Array>({
          start(controller) { streamController = controller },
        }), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false }, mutations: { retry: false } } })
    const router = createMemoryRouter([
      { path: '/chat/:sessionId', element: <ChatPage pollIntervalMs={5} /> },
    ], { initialEntries: [`/chat/${sessionA.id}`] })
    render(<QueryClientProvider client={queryClient}><RouterProvider router={router} /></QueryClientProvider>)
    const user = userEvent.setup()

    await user.type(await screen.findByRole('textbox', { name: '消息' }), 'run A')
    await user.click(screen.getByRole('button', { name: '发送消息' }))
    await waitFor(() => expect(streamController).toBeDefined())
    await act(async () => {
      streamController?.enqueue(encoder.encode('{"type":"message_accepted","message_id":"message-a","disposition":"started","created_session":false}\n'))
    })
    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })
    expect(await screen.findByText('B canonical answer')).toBeInTheDocument()

    await act(async () => {
      streamController?.enqueue(encoder.encode('{"type":"token_delta","turn_id":"turn-a","channel":"text","text":"A leaked preview"}\n'))
      streamController?.enqueue(encoder.encode(`{"type":"message_persisted","turn_id":"turn-a","message":{"id":"assistant-a","session_id":"${sessionA.id}","role":"assistant","message_kind":"assistant","content":[{"type":"text","text":"A persisted answer"}],"delivery_refs":[],"is_compacted":false,"created_at":"2026-08-26T10:00:02Z"}}\n`))
    })

    expect(screen.queryByText('A leaked preview')).not.toBeInTheDocument()
    expect(screen.queryByText('A persisted answer')).not.toBeInTheDocument()
    expect(screen.getByText('B canonical answer')).toBeInTheDocument()
    await act(async () => streamController?.close())
  })

  it('recovers from the last GET cursor so a promoted human message is not skipped', async () => {
    const user = userEvent.setup()
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{
            id: 'anchor', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'text', text: 'Earlier answer' }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:00Z',
          }],
          last_message_id: 'anchor',
        }))
      }
      if (url.includes('/messages?after=anchor')) {
        return jsonResponse(history({
          messages: [
            {
              id: 'human-message', session_id: baseSession.id, role: 'user', message_kind: 'human',
              content: [{ type: 'text', text: 'Keep my question' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
            {
              id: 'assistant-message', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'Canonical answer' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:02Z',
            },
          ],
          last_message_id: 'assistant-message',
        }))
      }
      if (url.includes('/messages?after=assistant-message')) return jsonResponse(history())
      if (url === `/api/sessions/${baseSession.id}/messages` && init?.method === 'POST') {
        const records = [
          { type: 'message_accepted', message_id: 'human-message', disposition: 'started', created_session: false },
          {
            type: 'message_persisted', turn_id: 'turn-1',
            message: {
              id: 'assistant-message', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'Canonical answer' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:02Z',
            },
          },
          { type: 'turn_finished', turn_id: 'turn-1', status: 'completed' },
        ]
        return new Response(records.map((record) => JSON.stringify(record)).join('\n') + '\n', {
          headers: { 'Content-Type': 'application/x-ndjson' },
        })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat(`/chat/${baseSession.id}`)
    await user.type(await screen.findByRole('textbox', { name: '消息' }), 'Keep my question')
    await user.click(screen.getByRole('button', { name: '发送消息' }))

    expect(await screen.findByText('Canonical answer')).toBeInTheDocument()
    await waitFor(() => expect(fetchMock.mock.calls.some(([url]) => String(url).includes('after=anchor'))).toBe(true))
    expect(fetchMock.mock.calls.some(([url]) => String(url).includes('after=assistant-message'))).toBe(false)
    expect(await screen.findByText('Keep my question')).toBeInTheDocument()
  })

  it('shows online devices in a linked header menu', async () => {
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([
        { id: 'device-1', name: 'laptop-cn', online: true },
        { id: 'device-2', name: 'devbox', online: false },
      ])
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat('/chat')

    expect(await screen.findByText('1 台设备在线')).toBeInTheDocument()
    expect(screen.getByRole('link', { name: /laptop-cn/ })).toHaveAttribute('href', '/devices/laptop-cn')
    expect(screen.getByRole('link', { name: /devbox/ })).toHaveAttribute('href', '/devices/devbox')
  })

  it('uses a client UUID for the first POST and reconciles repeated persisted snapshots', async () => {
    const user = userEvent.setup()
    const sessionId = '22222222-2222-4222-8222-222222222222'
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (init?.method === 'POST' && url === `/api/sessions/${sessionId}/messages`) {
        const records = [
          { type: 'message_accepted', message_id: 'message-user', disposition: 'started', created_session: true },
          {
            type: 'message_persisted', turn_id: 'turn-1',
            message: {
              id: 'message-assistant', session_id: sessionId, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'draft answer' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
          },
          {
            type: 'message_persisted', turn_id: 'turn-1',
            message: {
              id: 'message-assistant', session_id: sessionId, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'final answer' }], delivery_refs: [{ type: 'workspace_file', filename: 'report.txt' }], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
          },
          { type: 'turn_finished', turn_id: 'turn-1', status: 'completed' },
        ]
        const body = records.map((record) => JSON.stringify(record)).join('\n') + '\n'
        return new Response(body, { status: 200, headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat('/chat', 5, () => sessionId)
    const scroll = document.querySelector<HTMLElement>('.chat-scroll')
    expect(scroll).not.toBeNull()
    Object.defineProperty(scroll, 'scrollHeight', { configurable: true, value: 720 })
    await user.type(await screen.findByRole('textbox', { name: '消息' }), 'hello')
    await user.click(screen.getByRole('button', { name: '发送消息' }))

    expect(await screen.findByText('final answer')).toBeInTheDocument()
    expect(screen.queryByText('draft answer')).not.toBeInTheDocument()
    await waitFor(() => expect(scroll?.scrollTop).toBe(720))
    expect(fetchMock.mock.calls.filter(([url, init]) => url === `/api/sessions/${sessionId}/messages` && init?.method === 'POST')).toHaveLength(1)
  })
})
