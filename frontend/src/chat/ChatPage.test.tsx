import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { act, fireEvent, render, screen, waitFor, within } from '@testing-library/react'
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
  const result: Record<string, unknown> = {
    messages: [],
    pending_messages: [],
    status: 'idle',
    active_turn_id: null,
    last_message_id: null,
    pending_count: 0,
    has_more_before: false,
    ...overrides,
  }
  for (const key of ['messages', 'pending_messages']) {
    const rows = result[key]
    if (Array.isArray(rows)) {
      result[key] = rows.map((row) => (
        row && typeof row === 'object'
          ? { attachment_refs: [], ...row }
          : row
      ))
    }
  }
  return result
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((next) => { resolve = next })
  return { promise, resolve }
}

function delayedFile(name: string, header: Promise<ArrayBuffer>): File {
  const file = new File(['data'], name, { type: 'text/plain' })
  Object.defineProperty(file, 'slice', {
    value: () => ({ arrayBuffer: () => header }),
  })
  return file
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

  it('sends with Enter while Shift+Enter inserts a newline', async () => {
    await i18n.changeLanguage('en')
    let postedBody: unknown
    let finishRequest: ((response: Response) => void) | undefined
    const pendingResponse = new Promise<Response>((resolve) => {
      finishRequest = resolve
    })
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages') && init?.method === 'POST') {
        postedBody = JSON.parse(String(init.body))
        return pendingResponse
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat('/chat', 5, () => '11111111-1111-4111-8111-111111111111')

    const composer = await screen.findByRole('textbox', { name: 'Message' })
    await user.selectOptions(screen.getByRole('combobox', { name: 'Reasoning effort' }), 'high')
    await user.type(composer, 'first line')
    fireEvent.keyDown(composer, { key: 'Enter', code: 'Enter', isComposing: true })
    expect(postedBody).toBeUndefined()
    await user.keyboard('{Shift>}{Enter}{/Shift}second line')
    expect(composer).toHaveValue('first line\nsecond line')

    await user.keyboard('{Enter}')

    await waitFor(() => expect(postedBody).toEqual({
      effort: 'high',
      content: [{ type: 'text', text: 'first line\nsecond line' }],
      attachments: [],
    }))
    expect(composer).toHaveValue('')

    finishRequest?.(new Response([
      '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}',
      '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}',
      '',
    ].join('\n'), { headers: { 'Content-Type': 'application/x-ndjson' } }))
    await waitFor(() => expect(composer).not.toBeDisabled())
  })

  it('keeps live reasoning inside the assistant turn after the pending user message', async () => {
    await i18n.changeLanguage('en')
    let streamController: ReadableStreamDefaultController<Uint8Array> | undefined
    const encoder = new TextEncoder()
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${baseSession.id}/messages` && init?.method === 'POST') {
        return new Response(new ReadableStream<Uint8Array>({
          start(controller) { streamController = controller },
        }), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat(`/chat/${baseSession.id}`)
    await user.type(await screen.findByRole('textbox', { name: 'Message' }), 'Latest question')
    await user.click(screen.getByRole('button', { name: 'Send message' }))
    await waitFor(() => expect(streamController).toBeDefined())
    await act(async () => {
      streamController?.enqueue(encoder.encode([
        '{"type":"message_accepted","message_id":"message-user","disposition":"started","created_session":false}',
        '{"type":"token_delta","turn_id":"turn-1","channel":"thinking","text":"Checking sources"}',
        '{"type":"token_delta","turn_id":"turn-1","channel":"text","text":"Draft answer"}',
        '',
      ].join('\n')))
    })

    const userMessage = screen.getByText('Latest question').closest('article')
    const reasoning = screen.getByText('Reasoning')
    const assistantMessage = reasoning.closest('article')
    expect(userMessage).not.toBeNull()
    expect(assistantMessage).toHaveClass('chat-message-assistant', 'chat-message-live')
    expect(assistantMessage).toContainElement(screen.getByText('Draft answer'))
    expect(assistantMessage).toContainElement(screen.getByText('OpenOctopus'))
    expect(userMessage?.compareDocumentPosition(assistantMessage as Node) ?? 0)
      .toBe(Node.DOCUMENT_POSITION_FOLLOWING)

    await act(async () => streamController?.close())
  })

  it('restores the draft when the Server rejects a message before accepting it', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages') && init?.method === 'POST') {
        return jsonResponse({ code: 'provider_unavailable', message: 'Provider unavailable' }, 503)
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat('/chat', 5, () => '11111111-1111-4111-8111-111111111111')

    const composer = await screen.findByRole('textbox', { name: 'Message' })
    await user.type(composer, 'keep this draft')
    await user.keyboard('{Enter}')

    await waitFor(() => expect(screen.getByRole('alert')).toHaveTextContent('Provider unavailable'))
    expect(composer).toHaveValue('keep this draft')
  })

  it('defers browser file upload until send, uploads before POST, and reuses the ref when message retry is needed', async () => {
    await i18n.changeLanguage('en')
    let finishUpload: ((response: Response) => void) | undefined
    const uploadResponse = new Promise<Response>((resolve) => { finishUpload = resolve })
    let rejectFirstMessage: ((response: Response) => void) | undefined
    const firstMessageResponse = new Promise<Response>((resolve) => { rejectFirstMessage = resolve })
    let postCount = 0
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/api/workspace/files/.attachments/uploads/') && init?.method === 'PUT') return uploadResponse
      if (url.endsWith('/messages') && init?.method === 'POST') {
        postCount += 1
        if (postCount === 1) return firstMessageResponse
        return new Response([
          '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}',
          '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}',
          '',
        ].join('\n'), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    renderChat('/chat', 5, () => '11111111-1111-4111-8111-111111111111')

    const input = document.querySelector<HTMLInputElement>('input.chat-file-input')
    expect(input).not.toBeNull()
    await user.type(await screen.findByRole('textbox', { name: 'Message' }), 'Read this')
    await user.upload(input!, new File(['attachment'], 'context.txt', { type: 'text/plain' }))

    expect(await screen.findByText('context.txt')).toBeInTheDocument()
    expect(input).toHaveValue('')
    expect(await screen.findByText('Waiting to send')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Send message' })).not.toBeDisabled()
    expect(fetchMock.mock.calls.some(([url]) => String(url).endsWith('/messages'))).toBe(false)
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(0)

    await user.click(screen.getByRole('button', { name: 'Send message' }))
    await waitFor(() => expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(1))
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'POST')).toHaveLength(0)

    finishUpload?.(jsonResponse({ path: '.attachments/uploads/upload-id/context.txt', size: 10, etag: 'etag', created: true }))
    await waitFor(() => expect(screen.queryByRole('list', { name: 'Attachments to send' })).not.toBeInTheDocument())

    rejectFirstMessage?.(jsonResponse({ code: 'provider_unavailable', message: 'Try again' }, 503))
    expect(await screen.findByRole('alert')).toHaveTextContent('Try again')
    expect(screen.getByText('Ready')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: 'Send message' }))
    await waitFor(() => expect(postCount).toBe(2))
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(1)
    const postBodies = fetchMock.mock.calls
      .filter(([, init]) => init?.method === 'POST')
      .map(([, init]) => JSON.parse(String(init?.body)))
    expect(postBodies).toEqual([
      expect.objectContaining({ attachments: [{ openoctopus_device: 'server', path: expect.stringContaining('/context.txt') }] }),
      expect.objectContaining({ attachments: [{ openoctopus_device: 'server', path: expect.stringContaining('/context.txt') }] }),
    ])
  })

  it('turns a pasted clipboard image into a browser attachment and sends its uploaded ref', async () => {
    await i18n.changeLanguage('en')
    let postedBody: Record<string, unknown> | undefined
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/api/workspace/files/.attachments/uploads/') && init?.method === 'PUT') {
        return jsonResponse({ path: '.attachments/uploads/paste-id/pasted-image.png', size: 8, etag: 'etag', created: true })
      }
      if (url.endsWith('/messages') && init?.method === 'POST') {
        postedBody = JSON.parse(String(init.body)) as Record<string, unknown>
        return new Response([
          '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}',
          '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}',
          '',
        ].join('\n'), { headers: { 'Content-Type': 'application/x-ndjson' } })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    renderChat('/chat', 5, () => 'paste-id')

    const composer = await screen.findByRole('textbox', { name: 'Message' })
    const image = new File([
      new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
    ], '', { type: 'image/png' })
    fireEvent.paste(composer, {
      clipboardData: {
        items: [{ kind: 'file', type: 'image/png', getAsFile: () => image }],
      },
    })

    expect(await screen.findByText('pasted-image.png')).toBeInTheDocument()
    expect(await screen.findByText('Waiting to send')).toBeInTheDocument()
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(0)

    await user.click(screen.getByRole('button', { name: 'Send message' }))
    await waitFor(() => expect(postedBody).toEqual({
      effort: 'off',
      content: [],
      attachments: [{
        openoctopus_device: 'server',
        path: '.attachments/uploads/paste-id/pasted-image.png',
      }],
    }))
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(1)
  })

  it('reserves concurrent browser selections atomically before validating the files', async () => {
    await i18n.changeLanguage('en')
    const header = deferred<ArrayBuffer>()
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/api/workspace/files/.attachments/uploads/') && init?.method === 'PUT') {
        return jsonResponse({ path: url, size: 4, etag: 'etag', created: true })
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    let nextId = 0
    renderChat('/chat', 5, () => `upload-${nextId += 1}`)
    const input = await screen.findByLabelText('Select attachments')
    const first = Array.from({ length: 6 }, (_, index) => delayedFile(`first-${index}.txt`, header.promise))
    const second = Array.from({ length: 6 }, (_, index) => delayedFile(`second-${index}.txt`, header.promise))

    await user.upload(input, first)
    await user.upload(input, second)

    expect(screen.getAllByText('Uploading')).toHaveLength(6)
    expect(screen.getByRole('alert')).toHaveTextContent('at most 10 attachments')
    expect(screen.queryByText('second-0.txt')).not.toBeInTheDocument()

    await act(async () => header.resolve(new ArrayBuffer(12)))
    await waitFor(() => expect(screen.getAllByText('Waiting to send')).toHaveLength(6))
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(0)
  })

  it('ignores browser-file validation that completes after leaving its draft session', async () => {
    await i18n.changeLanguage('en')
    const sessionA = { ...baseSession, id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa', chat_id: 'aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa' }
    const sessionB = { ...baseSession, id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb', chat_id: 'bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb' }
    const header = deferred<ArrayBuffer>()
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([sessionA, sessionB])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes(`/api/sessions/${sessionA.id}/messages?limit=200`)) return jsonResponse(history())
      if (url.includes(`/api/sessions/${sessionB.id}/messages?limit=200`)) return jsonResponse(history())
      if (url.includes('/api/workspace/files/') && init?.method === 'PUT') {
        return jsonResponse({ path: url, size: 4, etag: 'etag', created: true })
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

    await user.upload(await screen.findByLabelText('Select attachments'), delayedFile('session-a.txt', header.promise))
    await act(async () => { await router.navigate(`/chat/${sessionB.id}`) })
    await act(async () => {
      header.resolve(new ArrayBuffer(12))
      await header.promise
      await Promise.resolve()
    })

    expect(screen.queryByText('session-a.txt')).not.toBeInTheDocument()
    expect(fetchMock.mock.calls.filter(([, init]) => init?.method === 'PUT')).toHaveLength(0)
  })

  it('offers local, Server Workspace, and online Client attachment sources', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([])
      if (url === '/api/devices') return jsonResponse([
        { id: 'device-1', name: 'laptop-cn', online: true },
        { id: 'device-2', name: 'offline-laptop', online: false },
      ])
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat('/chat')

    await user.click(await screen.findByRole('button', { name: 'Attachment' }))
    expect(screen.getByRole('menuitem', { name: 'This computer' })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: 'Server Workspaces' })).toBeInTheDocument()
    expect(screen.getByRole('menuitem', { name: 'laptop-cn' })).toBeInTheDocument()
    expect(screen.queryByRole('menuitem', { name: 'offline-laptop' })).not.toBeInTheDocument()
  })

  it('renders structured attachment refs for saved and pending user messages', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{
            id: 'human-1', session_id: baseSession.id, role: 'user', message_kind: 'human',
            content: [{ type: 'text', text: 'Use these files' }], delivery_refs: [], is_compacted: false,
            attachment_refs: [
              { openoctopus_device: 'server', path: '/Marketing@a4f7e2d1/brief.pdf' },
              { openoctopus_device: 'laptop-cn', device_id: 'device-1', path: 'reports/current.csv' },
            ],
            created_at: '2026-08-26T10:00:01Z',
          }],
          pending_messages: [{
            id: 'pending-1', session_id: baseSession.id, content: [], effort: 'off',
            attachment_refs: [{ openoctopus_device: 'server', path: 'notes.txt' }],
            received_at: '2026-08-26T10:00:02Z',
          }],
          pending_count: 0,
          last_message_id: 'human-1',
        }))
      }
      if (url.includes('/messages?after=human-1')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`)

    expect(await screen.findByText('brief.pdf')).toBeInTheDocument()
    expect(screen.getAllByText('Server Workspace')).toHaveLength(2)
    expect(screen.getByText('current.csv')).toBeInTheDocument()
    expect(screen.getByText('laptop-cn')).toBeInTheDocument()
    expect(screen.getByText('notes.txt')).toBeInTheDocument()
    expect(screen.getByText('Use these files')).toBeInTheDocument()
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

  it('collapses intermediate reasoning and tool messages while keeping the final reply visible', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [
            {
              id: 'human-1', session_id: baseSession.id, role: 'user', message_kind: 'human',
              content: [{ type: 'text', text: 'Inspect the API' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
            {
              id: 'assistant-tool', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
              content: [
                { type: 'thinking', thinking: 'I should fetch the schema.' },
                { type: 'tool_use', name: 'web_fetch', input: { url: 'https://example.com' } },
              ], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:02Z',
            },
            {
              id: 'tool-result', session_id: baseSession.id, role: 'user', message_kind: 'tool_result',
              content: [{ type: 'tool_result', content: 'OpenAPI schema' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:03Z',
            },
            {
              id: 'assistant-final', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'The API exposes four endpoints.' }], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:04Z',
            },
          ],
          last_message_id: 'assistant-final',
        }))
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    renderChat(`/chat/${baseSession.id}`)

    const summary = await screen.findByText('Work details · 2 steps')
    const details = summary.closest('details')
    expect(details).not.toBeNull()
    expect(details).not.toHaveAttribute('open')
    expect(screen.getByText('The API exposes four endpoints.').closest('details')).toBeNull()

    await user.click(summary)
    expect(details).toHaveAttribute('open')
    expect(details).toContainElement(screen.getByText('Tool call: web_fetch'))
    expect(within(details as HTMLElement).getAllByText('Tool result')).toHaveLength(2)
  })

  it('shows the latest tool in the collapsed summary while a turn is running', async () => {
    await i18n.changeLanguage('en')
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse([baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.endsWith('/messages?limit=200')) {
        return jsonResponse(history({
          messages: [{
            id: 'assistant-tool', session_id: baseSession.id, role: 'assistant', message_kind: 'assistant',
            content: [{ type: 'tool_use', name: 'web_fetch', input: {} }], delivery_refs: [], is_compacted: false,
            created_at: '2026-08-26T10:00:01Z',
          }],
          status: 'running', active_turn_id: 'turn-1', last_message_id: 'assistant-tool',
        }))
      }
      if (url.includes('/messages?after=assistant-tool')) return new Promise<Response>(() => {})
      throw new Error(`Unexpected request: ${url}`)
    }))

    renderChat(`/chat/${baseSession.id}`, 60_000)

    const summary = await screen.findByText('Working · web_fetch')
    expect(summary.closest('details')).not.toHaveAttribute('open')
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
    expect(screen.queryByRole('list', { name: '待发送附件' })).not.toBeInTheDocument()
    expect(screen.getByRole('combobox', { name: '思考强度' })).toBeDisabled()
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

  it('treats an already absent conversation as a completed deletion', async () => {
    let deleteRequested = false
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') return jsonResponse(deleteRequested ? [] : [baseSession])
      if (url === '/api/devices') return jsonResponse([])
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      if (url === `/api/sessions/${baseSession.id}` && init?.method === 'DELETE') {
        deleteRequested = true
        return jsonResponse({ code: 'session_not_found', message: 'Conversation not found.' }, 404)
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()
    renderChat(`/chat/${baseSession.id}`)

    await screen.findByRole('textbox', { name: '消息' })
    await user.click(screen.getByRole('button', { name: '删除' }))
    await user.click(screen.getByRole('button', { name: '确认删除' }))

    expect(await screen.findByRole('heading', { name: '今天想让 Agent 做什么？' })).toBeInTheDocument()
    expect(screen.queryByRole('alert')).not.toBeInTheDocument()
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
      if (url.includes('/api/workspace/files/') && init?.method === 'PUT') return jsonResponse({ created: true })
      if (init?.method === 'POST' && url.includes('/messages')) throw new TypeError('network lost')
      if (url.includes('/messages?limit=200')) return jsonResponse(history())
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    renderChat('/chat', 5, () => sessionId)
    const textbox = await screen.findByRole('textbox', { name: '消息' })
    const fileInput = document.querySelector<HTMLInputElement>('input.chat-file-input')
    expect(fileInput).not.toBeNull()
    await user.upload(fileInput!, new File(['attachment'], 'report.txt', { type: 'text/plain' }))
    expect(await screen.findByText('待发送')).toBeInTheDocument()
    await user.type(textbox, 'hello')
    await user.click(screen.getByRole('button', { name: '发送消息' }))

    expect(await screen.findByRole('alert')).toHaveTextContent('Server 确认前连接中断')
    const postCalls = fetchMock.mock.calls.filter(([url, init]) => String(url).includes('/messages') && init?.method === 'POST')
    expect(postCalls).toHaveLength(1)
    await waitFor(() => expect(fetchMock.mock.calls.some(([url, init]) => (
      String(url).includes(`/api/sessions/${sessionId}/messages?limit=200`) && init?.method === undefined
    ))).toBe(true))
    expect(screen.getByRole('textbox', { name: '消息' })).toHaveValue('hello')
    expect(screen.getByText('report.txt')).toBeInTheDocument()
    expect(screen.getByText('就绪')).toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: '发送消息' }))
    await waitFor(() => expect(fetchMock.mock.calls.filter(([url, init]) => (
      String(url).includes('/messages') && init?.method === 'POST'
    ))).toHaveLength(2))
    expect(fetchMock.mock.calls.filter(([url, init]) => (
      String(url).includes('/api/workspace/files/') && init?.method === 'PUT'
    ))).toHaveLength(1)
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
      streamController?.enqueue(encoder.encode(`{"type":"message_persisted","turn_id":"turn-a","message":{"id":"assistant-a","session_id":"${sessionA.id}","role":"assistant","message_kind":"assistant","content":[{"type":"text","text":"A persisted answer"}],"attachment_refs":[],"delivery_refs":[],"is_compacted":false,"created_at":"2026-08-26T10:00:02Z"}}\n`))
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
              content: [{ type: 'text', text: 'Canonical answer' }], attachment_refs: [], delivery_refs: [], is_compacted: false,
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
              content: [{ type: 'text', text: 'Canonical answer' }], attachment_refs: [], delivery_refs: [], is_compacted: false,
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
              content: [{ type: 'text', text: 'draft answer' }], attachment_refs: [], delivery_refs: [], is_compacted: false,
              created_at: '2026-08-26T10:00:01Z',
            },
          },
          {
            type: 'message_persisted', turn_id: 'turn-1',
            message: {
              id: 'message-assistant', session_id: sessionId, role: 'assistant', message_kind: 'assistant',
              content: [{ type: 'text', text: 'final answer' }], attachment_refs: [], delivery_refs: [{ type: 'workspace_file', filename: 'report.txt' }], is_compacted: false,
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
