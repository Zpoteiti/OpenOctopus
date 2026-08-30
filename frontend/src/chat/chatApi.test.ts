import { afterEach, describe, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import {
  cancelSession,
  deleteSession,
  loadSessions,
  renameSession,
  sendChatMessage,
  uploadBrowserAttachment,
  type StreamEvent,
} from './chatApi'

function streamResponse(...records: string[]): Response {
  const encoder = new TextEncoder()
  return new Response(
    new ReadableStream({
      start(controller) {
        records.forEach((record) => controller.enqueue(encoder.encode(record)))
        controller.close()
      },
    }),
    { status: 200, headers: { 'Content-Type': 'application/x-ndjson' } },
  )
}

function jsonResponse(body: unknown): Response {
  return new Response(JSON.stringify(body), { headers: { 'Content-Type': 'application/json' } })
}

function session(id: string, lastInboundAt = '2026-08-27T10:00:00Z') {
  return { id, last_inbound_at: lastInboundAt }
}

afterEach(async () => {
  vi.unstubAllGlobals()
  await i18n.changeLanguage('en')
})

describe('chat API', () => {
  it('loads every session page with the Server offset contract', async () => {
    const firstPage = Array.from({ length: 200 }, (_, index) => session(`session-${index}`))
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/sessions?limit=200') {
        return jsonResponse(firstPage)
      }
      if (url === '/api/sessions?limit=200&offset=200') {
        return jsonResponse([session('old-session')])
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    const sessions = await loadSessions()

    expect(sessions).toHaveLength(201)
    expect(sessions.at(-1)).toEqual(session('old-session'))
    expect(fetchMock.mock.calls.map(([url]) => url)).toEqual([
      '/api/sessions?limit=200',
      '/api/sessions?limit=200&offset=200',
      '/api/sessions?limit=200',
      '/api/sessions?limit=200&offset=200',
    ])
  })

  it('merges three volatile scans so offset reordering cannot hide a session', async () => {
    const allSessions = Array.from(
      { length: 201 },
      (_, index) => session(`session-${String(index).padStart(3, '0')}`),
    )
    const scans = [
      [allSessions.slice(0, 200), [allSessions[199]]],
      [[allSessions[200], ...allSessions.slice(0, 199)], [allSessions[198]]],
      [[allSessions[199], allSessions[200], ...allSessions.slice(0, 198)], [allSessions[197]]],
    ]
    let requestIndex = 0
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      const pageIndex = requestIndex % 2
      expect(url).toBe(pageIndex === 0
        ? '/api/sessions?limit=200'
        : '/api/sessions?limit=200&offset=200')
      const page = scans[Math.floor(requestIndex / 2)]?.[pageIndex]
      requestIndex += 1
      if (!page) throw new Error(`Unexpected request: ${url}`)
      return jsonResponse(page)
    })
    vi.stubGlobal('fetch', fetchMock)

    const sessions = await loadSessions()

    expect(sessions.map((item) => item.id)).toEqual([
      ...scans[2][0].map((item) => item.id),
      'session-198',
    ])
    expect(new Set(sessions.map((item) => item.id)).size).toBe(201)
    expect(fetchMock).toHaveBeenCalledTimes(6)
  })

  it('deduplicates each scan and waits for last_inbound_at to stabilize', async () => {
    const scans = [
      [session('session-1', '2026-08-27T10:00:00Z'), session('session-1', '2026-08-27T10:00:00Z')],
      [session('session-1', '2026-08-27T10:01:00Z'), session('session-1', '2026-08-27T10:01:00Z')],
      [session('session-1', '2026-08-27T10:01:00Z'), session('session-1', '2026-08-27T10:01:00Z')],
    ]
    let scanIndex = 0
    const fetchMock = vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      expect(url).toBe('/api/sessions?limit=200')
      const scan = scans[scanIndex]
      scanIndex += 1
      if (!scan) throw new Error(`Unexpected request: ${url}`)
      return jsonResponse(scan)
    })
    vi.stubGlobal('fetch', fetchMock)

    const sessions = await loadSessions()

    expect(sessions).toEqual([session('session-1', '2026-08-27T10:01:00Z')])
    expect(fetchMock).toHaveBeenCalledTimes(3)
  })

  it('uploads a browser attachment once and posts its ready ref without re-uploading on retry', async () => {
    const events: StreamEvent[] = []
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (init?.method === 'PUT') {
        return new Response(JSON.stringify({ path: '.attachments/uploads/file-id/report.txt', size: 4, etag: 'etag', created: true }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      if (init?.method === 'POST') {
        return streamResponse(
          '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}\n',
          '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}\n',
        )
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    const attachment = await uploadBrowserAttachment(
      new File(['data'], 'report.txt', { type: 'text/plain' }),
      'file-id',
    )

    await sendChatMessage({
      sessionId: 'session-1',
      text: 'summarize this',
      attachments: [attachment],
      effort: 'high',
      onEvent: (event) => events.push(event),
    })

    expect(fetchMock).toHaveBeenCalledTimes(2)
    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/workspace/files/.attachments/uploads/file-id/report.txt?openoctopus_device=server')
    expect(fetchMock.mock.calls[0]?.[1]).toMatchObject({ method: 'PUT', credentials: 'same-origin' })
    expect(new Headers(fetchMock.mock.calls[0]?.[1]?.headers).get('If-None-Match')).toBe('*')
    expect(fetchMock.mock.calls[1]?.[0]).toBe('/api/sessions/session-1/messages')
    expect(JSON.parse(String(fetchMock.mock.calls[1]?.[1]?.body))).toEqual({
      effort: 'high',
      content: [{ type: 'text', text: 'summarize this' }],
      attachments: [{ openoctopus_device: 'server', path: '.attachments/uploads/file-id/report.txt' }],
    })
    expect(events.map((event) => event.type)).toEqual(['message_accepted', 'turn_finished'])
  })

  it('preserves meaningful leading and trailing whitespace in submitted text', async () => {
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'POST') {
        return streamResponse(
          '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}\n',
          '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}\n',
        )
      }
      throw new Error('Unexpected upload')
    })
    vi.stubGlobal('fetch', fetchMock)

    await sendChatMessage({
      sessionId: 'session-1',
      text: '  indented text\n',
      attachments: [],
      onEvent: () => undefined,
    })

    expect(JSON.parse(String(fetchMock.mock.calls[0]?.[1]?.body))).toMatchObject({
      content: [{ type: 'text', text: '  indented text\n' }],
    })
  })

  it('rejects more than ten mixed attachment refs before posting', async () => {
    await i18n.changeLanguage('zh-CN')
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)

    await expect(sendChatMessage({
      sessionId: 'session-1',
      text: '',
      attachments: [
        ...Array.from({ length: 10 }, (_, index) => ({
          openoctopus_device: 'server' as const,
          path: `${index}.txt`,
        })),
        { openoctopus_device: 'laptop-cn', device_id: 'device-1', path: 'remote.txt' },
      ],
      onEvent: () => undefined,
    })).rejects.toThrow('每条消息最多上传 10 个附件')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('rejects image bytes above the aggregate limit before uploading a browser selection', async () => {
    const fetchMock = vi.fn()
    vi.stubGlobal('fetch', fetchMock)
    const header = new Uint8Array([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a])
    const padding = new Uint8Array(8 * 1024 * 1024 + 1 - header.byteLength)

    await expect(uploadBrowserAttachment(
      new File([header, padding], 'large.png', { type: 'image/png' }),
      'file-id',
    )).rejects.toThrow('8 MiB')
    expect(fetchMock).not.toHaveBeenCalled()
  })

  it('does not let dot-segment attachment names alter the upload path', async () => {
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PUT') {
        return new Response(JSON.stringify({ path: '.attachments/uploads/file-id/attachment' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      return streamResponse(
        '{"type":"message_accepted","message_id":"message-1","disposition":"started","created_session":true}\n',
        '{"type":"turn_finished","turn_id":"turn-1","status":"completed"}\n',
      )
    })
    vi.stubGlobal('fetch', fetchMock)

    await uploadBrowserAttachment(new File(['data'], '..'), 'file-id')

    expect(fetchMock.mock.calls[0]?.[0]).toBe('/api/workspace/files/.attachments/uploads/file-id/attachment?openoctopus_device=server')
  })

  it('does not retry when the stream breaks before message_accepted', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(
        new ReadableStream({
          start(controller) {
            controller.error(new Error('connection lost'))
          },
        }),
        { status: 200, headers: { 'Content-Type': 'application/x-ndjson' } },
      ),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(sendChatMessage({
      sessionId: 'session-1',
      text: 'hello',
      attachments: [],
      onEvent: () => undefined,
    })).rejects.toEqual(expect.objectContaining({ accepted: false }))
    expect(fetchMock).toHaveBeenCalledTimes(1)
  })

  it('uses the session control endpoints', async () => {
    const fetchMock = vi.fn(async (_input: string | URL | Request, init?: RequestInit) => {
      if (init?.method === 'PATCH') {
        return new Response(JSON.stringify({ id: 'session-1', title: 'Renamed' }), {
          status: 200,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      if (init?.method === 'POST') {
        return new Response(JSON.stringify({ cancel_requested: true }), {
          status: 202,
          headers: { 'Content-Type': 'application/json' },
        })
      }
      return new Response(null, { status: 204 })
    })
    vi.stubGlobal('fetch', fetchMock)

    await renameSession('session-1', 'Renamed')
    await cancelSession('session-1')
    await deleteSession('session-1')

    expect(fetchMock.mock.calls.map(([url, init]) => [url, init?.method])).toEqual([
      ['/api/sessions/session-1', 'PATCH'],
      ['/api/sessions/session-1/cancel', 'POST'],
      ['/api/sessions/session-1', 'DELETE'],
    ])
  })
})
