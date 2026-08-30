import { afterEach, describe, expect, it, vi } from 'vitest'

import { listDirectory, listMembers, listWorkspaces } from './api'

afterEach(() => {
  vi.unstubAllGlobals()
})

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), {
    headers: { 'Content-Type': 'application/json' },
  })
}

describe('Workspace pagination', () => {
  it('fences Client directory requests with the immutable device id', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      expect(url).toContain(
        'openoctopus_device=laptop-cn&openoctopus_device_id=11111111-1111-4111-8111-111111111111',
      )
      return json({
        items: [],
        limit: 200,
        offset: url.endsWith('offset=0') ? 0 : 200,
        next_offset: url.endsWith('offset=0') ? 200 : null,
        truncated: false,
      })
    })
    vi.stubGlobal('fetch', fetchMock)

    await listDirectory('docs', 'laptop-cn', '11111111-1111-4111-8111-111111111111')

    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('continues Client directory pagination when the scan ceiling is reported', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      const first = url.endsWith('offset=0')
      return json({
        items: [{ name: first ? 'first.txt' : 'last.txt', path: first ? 'first.txt' : 'last.txt', kind: 'file', size: 1 }],
        limit: 200,
        offset: first ? 0 : 200,
        next_offset: first ? 200 : null,
        truncated: true,
      })
    })
    vi.stubGlobal('fetch', fetchMock)

    const page = await listDirectory('.', 'laptop-cn', '11111111-1111-4111-8111-111111111111')

    expect(page.items.map((item) => item.name)).toEqual(['first.txt', 'last.txt'])
    expect(page.truncated).toBe(true)
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('collects every Workspace page through next_offset', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url === '/api/workspaces?limit=200&offset=0') {
        return json({
          items: [{ id: 'personal', name: 'Personal', type: 'personal' }],
          limit: 200,
          offset: 0,
          next_offset: 200,
          truncated: false,
        })
      }
      if (url === '/api/workspaces?limit=200&offset=200') {
        return json({
          items: [{ id: 'shared', name: 'Shared', type: 'shared' }],
          limit: 200,
          offset: 200,
          next_offset: null,
          truncated: false,
        })
      }
      throw new Error(`Unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    const page = await listWorkspaces()

    expect(page.items.map((workspace) => workspace.id)).toEqual(['personal', 'shared'])
    expect(page.next_offset).toBeNull()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it('collects every member page through next_offset', async () => {
    const fetchMock = vi.fn(async (input: RequestInfo | URL) => {
      const url = String(input)
      if (url === '/api/workspaces/team%40abcdef12/members?limit=200&offset=0') {
        return json({
          items: [{ user_id: 'user-1', email: 'one@example.com', name: 'One' }],
          limit: 200,
          offset: 0,
          next_offset: 200,
          truncated: false,
        })
      }
      if (url === '/api/workspaces/team%40abcdef12/members?limit=200&offset=200') {
        return json({
          items: [{ user_id: 'user-2', email: 'two@example.com', name: 'Two' }],
          limit: 200,
          offset: 200,
          next_offset: null,
          truncated: false,
        })
      }
      throw new Error(`Unexpected fetch: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)

    const page = await listMembers('team@abcdef12')

    expect(page.items.map((member) => member.user_id)).toEqual(['user-1', 'user-2'])
    expect(page.next_offset).toBeNull()
    expect(fetchMock).toHaveBeenCalledTimes(2)
  })

  it.each([
    ['Workspace', () => listWorkspaces(), '/api/workspaces?limit=200&offset=0'],
    ['member', () => listMembers('team@abcdef12'), '/api/workspaces/team%40abcdef12/members?limit=200&offset=0'],
  ])('rejects a non-advancing %s next_offset', async (_label, request, expectedUrl) => {
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL) => {
      expect(String(input)).toBe(expectedUrl)
      return json({ items: [], limit: 200, offset: 0, next_offset: 0, truncated: false })
    }))

    await expect(request()).rejects.toThrow(/pagination did not advance/i)
  })
})
