import { act, render, screen, waitFor, within } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { MemoryRouter } from 'react-router-dom'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'

import { AuthenticatedUserContext } from '../auth/context'
import i18n from '../i18n'
import { WorkspacePage } from './WorkspacePage'

const zhTestResources = {
  'workspace.title': '工作区',
  'workspace.specialSoul': 'Agent 身份',
  'workspace.previewNotUtf8': '文件不是有效 UTF-8，无法作为文本编辑；可直接下载。',
}

const currentUser = {
  id: 'user-1',
  email: 'yucheng@example.com',
  name: 'Yucheng',
  is_admin: true,
  created_at: '2026-08-26T00:00:00Z',
}

const personalWorkspace = {
  id: 'user-1',
  name: 'Yucheng',
  type: 'personal',
  quota_bytes: 524_288_000,
  bytes_used: 12_000,
  locked: false,
}

const sharedWorkspace = {
  id: 'workspace-1',
  name: '市场协作',
  suffix: 'a4f7e2d1',
  ref: '市场协作@a4f7e2d1',
  type: 'shared',
  quota_bytes: 524_288_000,
  bytes_used: 92_100_000,
  locked: false,
  created_by: 'user-1',
}

const secondSharedWorkspace = {
  ...sharedWorkspace,
  id: 'workspace-2',
  name: '产品文档',
  suffix: 'b4f7e2d1',
  ref: '产品文档@b4f7e2d1',
}

const device = {
  id: 'device-1',
  name: 'laptop-cn',
  workspace_path: '~/openoctopus/workspace',
  restrict_to_workspace: true,
  ssrf_denylist: [],
  shell_timeout_max: 600,
  env_allowlist: [],
  config_revision: 2,
  mcp_config_count: 0,
  mcp_enabled_capability_count: 0,
  mcp_provider_visible_capability_count: 0,
  mcp_suppressed_capability_count: 0,
  mcp_catalog_digest: '',
  online: true,
  token_hint: 'openoct...1234',
  created_at: '2026-08-26T00:00:00Z',
}

const offlineDevice = {
  ...device,
  id: 'device-2',
  name: 'offline-laptop',
  online: false,
}

const rootEntries = {
  items: [
    { name: 'projects', path: 'projects', kind: 'directory', size: 0 },
    { name: 'skills', path: 'skills', kind: 'directory', size: 0 },
    { name: '.attachments', path: '.attachments', kind: 'directory', size: 0 },
    { name: 'SOUL.md', path: 'SOUL.md', kind: 'file', size: 42 },
    { name: 'MEMORY.md', path: 'MEMORY.md', kind: 'file', size: 84 },
    { name: 'notes.txt', path: 'notes.txt', kind: 'file', size: 12 },
  ],
  limit: 200,
  offset: 0,
  next_offset: null,
  truncated: false,
}

beforeEach(async () => {
  await i18n.changeLanguage('en')
})

afterEach(async () => {
  vi.unstubAllGlobals()
  await i18n.changeLanguage('en')
})

function json(value: unknown, status = 200, headers: HeadersInit = {}): Response {
  return new Response(JSON.stringify(value), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers },
  })
}

function baseFetch(overrides?: (url: string, init?: RequestInit) => Response | Promise<Response> | undefined) {
  return vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
    const url = String(input)
    const overridden = overrides?.(url, init)
    if (overridden) return overridden
    if (url === '/api/workspaces?limit=200&offset=0') {
      return json({ items: [personalWorkspace, sharedWorkspace], limit: 200, offset: 0, next_offset: null, truncated: false })
    }
    if (url === '/api/devices') return json([device, offlineDevice])
    if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
      return json(rootEntries)
    }
    if (url.startsWith('/api/workspace/list//')) {
      return json({ ...rootEntries, items: [] })
    }
    if (url.includes('/members?')) {
      return json({
        items: [
          { user_id: 'user-1', email: 'yucheng@example.com', name: 'Yucheng' },
          { user_id: 'user-2', email: 'alice@example.com', name: 'Alice' },
        ],
        limit: 200,
        offset: 0,
        next_offset: null,
        truncated: false,
      })
    }
    throw new Error(`Unexpected fetch: ${init?.method ?? 'GET'} ${url}`)
  })
}

function deferred<T>() {
  let resolve!: (value: T) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

function renderPage(path = '/workspace') {
  return render(
    <MemoryRouter initialEntries={[path]}>
      <AuthenticatedUserContext.Provider value={currentUser}>
        <WorkspacePage />
      </AuthenticatedUserContext.Provider>
    </MemoryRouter>,
  )
}

describe('WorkspacePage', () => {
  it('lists real workspace/device locations and highlights agent files without invented metadata', async () => {
    vi.stubGlobal('fetch', baseFetch())
    renderPage()

    expect(await screen.findByText('Workspace')).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'New shared Workspace' })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: /Yucheng/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /市场协作/ })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: /laptop-cn.*Online/ })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: /offline-laptop/ })).not.toBeInTheDocument()
    expect(await screen.findByText('Agent identity')).toBeInTheDocument()
    expect(screen.getByText('Long-term memory')).toBeInTheDocument()
    expect(screen.getByText('Skills')).toBeInTheDocument()
    expect(screen.getByText('Attachments')).toBeInTheDocument()
    expect(screen.queryByText(/MIME|modified/i)).not.toBeInTheDocument()
  })

  it('follows directory next_offset pages instead of hiding later entries', async () => {
    vi.stubGlobal('fetch', baseFetch((url) => {
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({ ...rootEntries, items: [rootEntries.items[0]], next_offset: 2 })
      }
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=2') {
        return json({ ...rootEntries, items: [rootEntries.items[4]], offset: 2, next_offset: null })
      }
      return undefined
    }))
    renderPage()

    expect(await screen.findByRole('button', { name: /projects/ })).toBeInTheDocument()
    expect(await screen.findByRole('button', { name: /MEMORY\.md/ })).toBeInTheDocument()
  })

  it('previews text and saves with the ETag returned by the read', async () => {
    const fetchMock = baseFetch((url, init) => {
      if (url === '/api/workspace/files/SOUL.md?openoctopus_device=server' && !init?.method) {
        return new Response('# Soul\nBe practical.', { headers: { ETag: '"etag-1"' } })
      }
      if (url === '/api/workspace/files/SOUL.md?openoctopus_device=server' && init?.method === 'PUT') {
        expect(new Headers(init.headers).get('If-Match')).toBe('"etag-1"')
        expect(init.body).toBe('# Soul\nBe concise.')
        return json({ path: 'SOUL.md', size: 23, etag: 'etag-2', created: false }, 200, { ETag: '"etag-2"' })
      }
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /SOUL\.md/ }))
    const editor = await screen.findByRole('textbox', { name: 'File content' })
    expect(editor).toHaveValue('# Soul\nBe practical.')
    await user.clear(editor)
    await user.type(editor, '# Soul\nBe concise.')
    await user.click(screen.getByRole('button', { name: 'Save file' }))

    expect(await screen.findByText('File saved.')).toBeInTheDocument()
  })

  it('keeps the latest selected file content and ETag when an earlier read resolves last', async () => {
    const firstRead = deferred<Response>()
    const fetchMock = baseFetch((url, init) => {
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({
          ...rootEntries,
          items: [
            { name: 'first.txt', path: 'first.txt', kind: 'file', size: 5 },
            { name: 'second.txt', path: 'second.txt', kind: 'file', size: 6 },
          ],
        })
      }
      if (url === '/api/workspace/files/first.txt?openoctopus_device=server' && !init?.method) {
        return firstRead.promise
      }
      if (url === '/api/workspace/files/second.txt?openoctopus_device=server' && !init?.method) {
        return new Response('second', { headers: { ETag: '"second-etag"' } })
      }
      if (url === '/api/workspace/files/second.txt?openoctopus_device=server' && init?.method === 'PUT') {
        expect(new Headers(init.headers).get('If-Match')).toBe('"second-etag"')
        expect(init.body).toBe('second updated')
        return json({ path: 'second.txt', size: 14, etag: 'saved-etag', created: false }, 200, { ETag: '"saved-etag"' })
      }
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /first\.txt/ }))
    await user.click(screen.getByRole('button', { name: /second\.txt/ }))
    const editor = await screen.findByRole('textbox', { name: 'File content' })
    expect(editor).toHaveValue('second')

    await act(async () => {
      firstRead.resolve(new Response('first', { headers: { ETag: '"first-etag"' } }))
    })
    expect(editor).toHaveValue('second')
    await user.clear(editor)
    await user.type(editor, 'second updated')
    await user.click(screen.getByRole('button', { name: 'Save file' }))

    expect(await screen.findByText('File saved.')).toBeInTheDocument()
  })

  it('ignores a stale file-read failure after navigating into a directory', async () => {
    const fileRead = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspace/files/notes.txt?openoctopus_device=server' && !init?.method) {
        return fileRead.promise
      }
      if (url === '/api/workspace/list/projects?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({ ...rootEntries, items: [] })
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /notes\.txt/ }))
    await user.click(screen.getByRole('button', { name: /projects/ }))
    expect(await screen.findByText('This directory is empty')).toBeInTheDocument()

    await act(async () => {
      fileRead.reject(new Error('stale file read failed'))
    })
    expect(screen.queryByText('stale file read failed')).not.toBeInTheDocument()
  })

  it('ignores a stale file-read failure after switching Workspace locations', async () => {
    const fileRead = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspace/files/notes.txt?openoctopus_device=server' && !init?.method) {
        return fileRead.promise
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /notes\.txt/ }))
    await user.click(screen.getByRole('button', { name: /市场协作/ }))
    expect(await screen.findByText('All members have equal permissions')).toBeInTheDocument()

    await act(async () => {
      fileRead.reject(new Error('stale location read failed'))
    })
    expect(screen.queryByText('stale location read failed')).not.toBeInTheDocument()
  })

  it('shows an actionable stale-file message on an If-Match conflict', async () => {
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url.includes('/api/workspace/files/notes.txt') && !init?.method) {
        return new Response('old', { headers: { ETag: '"old-etag"' } })
      }
      if (url.includes('/api/workspace/files/notes.txt') && init?.method === 'PUT') {
        return json({ code: 'workspace_file_changed', message: 'stale' }, 409)
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /notes\.txt/ }))
    await user.type(await screen.findByRole('textbox', { name: 'File content' }), ' changed')
    await user.click(screen.getByRole('button', { name: 'Save file' }))

    expect(await screen.findByText('The file changed elsewhere. Reload it before trying again. (workspace_file_changed)')).toBeInTheDocument()
  })

  it('ignores a save completion after switching locations and releases its stale busy state', async () => {
    const saveRequest = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspace/files/notes.txt?openoctopus_device=server' && !init?.method) {
        return new Response('old', { headers: { ETag: '"notes-etag"' } })
      }
      if (url === '/api/workspace/files/notes.txt?openoctopus_device=server' && init?.method === 'PUT') {
        return saveRequest.promise
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /notes\.txt/ }))
    await user.type(await screen.findByRole('textbox', { name: 'File content' }), ' changed')
    await user.click(screen.getByRole('button', { name: 'Save file' }))
    await user.click(screen.getByRole('button', { name: /市场协作/ }))
    const removeAlice = await screen.findByRole('button', { name: 'Remove Alice' })
    const wasEnabledAfterSwitch = !removeAlice.hasAttribute('disabled')

    await act(async () => {
      saveRequest.resolve(json({ path: 'notes.txt', size: 11, etag: 'saved-etag', created: false }, 200, { ETag: '"saved-etag"' }))
    })

    expect(wasEnabledAfterSwitch).toBe(true)
    expect(screen.queryByText('File saved.')).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Remove Alice' })).not.toBeDisabled()
  })

  it('does not edit oversized or invalid UTF-8 files', async () => {
    const fetchMock = baseFetch((url) => {
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({
          ...rootEntries,
          items: [
            { name: 'large.txt', path: 'large.txt', kind: 'file', size: 1_048_577 },
            { name: 'invalid.txt', path: 'invalid.txt', kind: 'file', size: 1 },
            { name: 'stale-size.txt', path: 'stale-size.txt', kind: 'file', size: 1 },
          ],
        })
      }
      if (url === '/api/workspace/files/invalid.txt?openoctopus_device=server') {
        return new Response(new Uint8Array([255]), { headers: { ETag: '"invalid"' } })
      }
      if (url === '/api/workspace/files/stale-size.txt?openoctopus_device=server') {
        return new Response(new Uint8Array(1_048_577), { headers: { ETag: '"oversized"' } })
      }
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /large\.txt/ }))
    expect(await screen.findByText('This file exceeds the 1 MiB text preview limit. You can still download it.')).toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalledWith(
      '/api/workspace/files/large.txt?openoctopus_device=server',
      expect.anything(),
    )

    await user.click(screen.getByRole('button', { name: /invalid\.txt/ }))
    expect(
      await screen.findByText(
        'This file is not valid UTF-8 and cannot be edited as text. You can still download it. (file_not_utf8)',
      ),
    ).toBeInTheDocument()
    expect(screen.queryByRole('textbox', { name: 'File content' })).not.toBeInTheDocument()

    await user.click(screen.getByRole('button', { name: /stale-size.txt/ }))
    expect(
      await screen.findByText(
        'This file exceeds the 1 MiB text preview limit. You can still download it. (file_preview_too_large)',
      ),
    ).toBeInTheDocument()
  })

  it('uploads a new file with create-only semantics into the selected directory', async () => {
    const fetchMock = baseFetch((url, init) => {
      if (url === '/api/workspace/files/report.txt?openoctopus_device=server' && init?.method === 'PUT') {
        expect(new Headers(init.headers).get('If-None-Match')).toBe('*')
        expect(init.body).toBeInstanceOf(File)
        return json({ path: 'report.txt', size: 6, etag: 'new-etag', created: true })
      }
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    const input = await screen.findByLabelText('Choose file')
    await user.upload(input, new File(['report'], 'report.txt', { type: 'text/plain' }))
    await user.click(screen.getByRole('button', { name: 'Upload file' }))

    expect(await screen.findByText('File uploaded.')).toBeInTheDocument()
  })

  it('does not refresh a new directory with a stale upload completion', async () => {
    const uploadRequest = deferred<Response>()
    let rootListCalls = 0
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        rootListCalls += 1
        return json(rootEntries)
      }
      if (url === '/api/workspace/list/projects?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({
          ...rootEntries,
          items: [{ name: 'inside.txt', path: 'projects/inside.txt', kind: 'file', size: 6 }],
        })
      }
      if (url === '/api/workspace/files/report.txt?openoctopus_device=server' && init?.method === 'PUT') {
        return uploadRequest.promise
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    const input = await screen.findByLabelText('Choose file')
    await user.upload(input, new File(['report'], 'report.txt', { type: 'text/plain' }))
    await user.click(screen.getByRole('button', { name: 'Upload file' }))
    await user.click(screen.getByRole('button', { name: /projects/ }))
    expect(await screen.findByRole('button', { name: /inside\.txt/ })).toBeInTheDocument()

    await act(async () => {
      uploadRequest.resolve(json({ path: 'report.txt', size: 6, etag: 'new-etag', created: true }))
    })

    expect(screen.getByRole('button', { name: /inside\.txt/ })).toBeInTheDocument()
    expect(screen.queryByText('File uploaded.')).not.toBeInTheDocument()
    expect(rootListCalls).toBe(1)
  })

  it('does not add a member to a different Workspace when the request completes late', async () => {
    const addRequest = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspaces?limit=200&offset=0') {
        return json({ items: [personalWorkspace, sharedWorkspace, secondSharedWorkspace], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url === '/api/workspaces/%E5%B8%82%E5%9C%BA%E5%8D%8F%E4%BD%9C%40a4f7e2d1/members' && init?.method === 'POST') {
        return addRequest.promise
      }
      if (url === '/api/workspaces/%E4%BA%A7%E5%93%81%E6%96%87%E6%A1%A3%40b4f7e2d1/members?limit=200&offset=0') {
        return json({
          items: [
            { user_id: 'user-1', email: 'yucheng@example.com', name: 'Yucheng' },
            { user_id: 'user-4', email: 'carol@example.com', name: 'Carol' },
          ],
          limit: 200,
          offset: 0,
          next_offset: null,
          truncated: false,
        })
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /市场协作/ }))
    await user.type(screen.getByLabelText('Member email'), 'bob@example.com')
    await user.click(screen.getByRole('button', { name: 'Add member' }))
    await user.click(screen.getByRole('button', { name: /产品文档/ }))
    expect(await screen.findByText('carol@example.com')).toBeInTheDocument()
    const removeCarol = screen.getByRole('button', { name: 'Remove Carol' })
    const wasEnabledAfterSwitch = !removeCarol.hasAttribute('disabled')

    await act(async () => {
      addRequest.resolve(json({ user_id: 'user-3', email: 'bob@example.com', name: 'Bob' }, 201))
    })

    expect(wasEnabledAfterSwitch).toBe(true)
    expect(screen.queryByText('bob@example.com')).not.toBeInTheDocument()
    expect(screen.getByText('carol@example.com')).toBeInTheDocument()
  })

  it('does not remove a member from a different Workspace when the request completes late', async () => {
    const removeRequest = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url === '/api/workspaces?limit=200&offset=0') {
        return json({ items: [personalWorkspace, sharedWorkspace, secondSharedWorkspace], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url.endsWith('/members/user-2') && init?.method === 'DELETE') {
        return removeRequest.promise
      }
      if (url === '/api/workspaces/%E4%BA%A7%E5%93%81%E6%96%87%E6%A1%A3%40b4f7e2d1/members?limit=200&offset=0') {
        return json({
          items: [
            { user_id: 'user-1', email: 'yucheng@example.com', name: 'Yucheng' },
            { user_id: 'user-2', email: 'alice@example.com', name: 'Alice' },
          ],
          limit: 200,
          offset: 0,
          next_offset: null,
          truncated: false,
        })
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /市场协作/ }))
    await user.click(await screen.findByRole('button', { name: 'Remove Alice' }))
    await user.click(screen.getByRole('button', { name: 'Confirm removal' }))
    await user.click(screen.getByRole('button', { name: /产品文档/ }))
    expect(await screen.findByText('alice@example.com')).toBeInTheDocument()
    const wasEnabledAfterSwitch = !screen.getByRole('button', { name: 'Remove Alice' }).hasAttribute('disabled')

    await act(async () => {
      removeRequest.resolve(new Response(null, { status: 204 }))
    })

    expect(wasEnabledAfterSwitch).toBe(true)
    expect(screen.getByText('alice@example.com')).toBeInTheDocument()
  })

  it('manages equal-permission shared members and creates a shared workspace', async () => {
    const fetchMock = baseFetch((url, init) => {
      if (url === '/api/workspaces/%E5%B8%82%E5%9C%BA%E5%8D%8F%E4%BD%9C%40a4f7e2d1/members' && init?.method === 'POST') {
        return json({ user_id: 'user-3', email: 'bob@example.com', name: 'Bob' }, 201)
      }
      if (url.endsWith('/members/user-2') && init?.method === 'DELETE') return new Response(null, { status: 204 })
      if (url === '/api/workspaces' && init?.method === 'POST') {
        return json({ ...sharedWorkspace, id: 'workspace-2', name: '产品文档', ref: '产品文档@b4f7e2d1' }, 201)
      }
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /市场协作/ }))
    expect(await screen.findByText('All members have equal permissions')).toBeInTheDocument()
    expect(screen.getByText('alice@example.com')).toBeInTheDocument()
    await user.type(screen.getByLabelText('Member email'), 'bob@example.com')
    await user.click(screen.getByRole('button', { name: 'Add member' }))
    expect(await screen.findByText('bob@example.com')).toBeInTheDocument()

    const aliceRow = screen.getByText('alice@example.com').closest('li')
    expect(aliceRow).not.toBeNull()
    await user.click(within(aliceRow as HTMLElement).getByRole('button', { name: 'Remove Alice' }))
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringMatching(/\/members\/user-2$/),
      expect.objectContaining({ method: 'DELETE' }),
    )
    await user.click(within(aliceRow as HTMLElement).getByRole('button', { name: 'Confirm removal' }))
    await waitFor(() => expect(screen.queryByText('alice@example.com')).not.toBeInTheDocument())

    await user.click(screen.getByRole('button', { name: 'New shared Workspace' }))
    await user.type(screen.getByLabelText('Workspace name'), '产品文档')
    await user.clear(screen.getByLabelText('Quota (MiB)'))
    await user.type(screen.getByLabelText('Quota (MiB)'), '256')
    await user.click(screen.getByRole('button', { name: 'Create Workspace' }))
    expect(await screen.findByRole('button', { name: /产品文档/ })).toBeInTheDocument()
  })

  it('lets the current member leave a shared workspace through the member route', async () => {
    const fetchMock = baseFetch((url, init) => {
      if (url.includes('/members?')) {
        return json({
          items: [{ user_id: 'user-1', email: 'yucheng@example.com', name: 'Yucheng' }],
          limit: 200,
          offset: 0,
          next_offset: null,
          truncated: false,
        })
      }
      if (url.endsWith('/members/user-1') && init?.method === 'DELETE') return new Response(null, { status: 204 })
      return undefined
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /市场协作/ }))
    await user.click(await screen.findByRole('button', { name: 'Leave Workspace' }))
    expect(await screen.findByText('You are the last member. Leaving permanently deletes this Workspace and its files.')).toBeInTheDocument()
    expect(fetchMock).not.toHaveBeenCalledWith(
      expect.stringMatching(/\/members\/user-1$/),
      expect.objectContaining({ method: 'DELETE' }),
    )
    await user.click(screen.getByRole('button', { name: 'Confirm leave and delete Workspace' }))

    await waitFor(() => expect(screen.queryByRole('button', { name: /市场协作/ })).not.toBeInTheDocument())
    expect(screen.getByRole('button', { name: /Yucheng/ })).toHaveClass('active')
  })

  it('removes a shared Workspace locally when leaving completes after navigating away', async () => {
    const leaveRequest = deferred<Response>()
    vi.stubGlobal('fetch', baseFetch((url, init) => {
      if (url.endsWith('/members/user-1') && init?.method === 'DELETE') return leaveRequest.promise
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    await user.click(await screen.findByRole('button', { name: /市场协作/ }))
    await user.click(await screen.findByRole('button', { name: 'Leave Workspace' }))
    await user.click(screen.getByRole('button', { name: 'Confirm leave' }))
    await user.click(screen.getByRole('button', { name: /Yucheng/ }))

    await act(async () => leaveRequest.resolve(new Response(null, { status: 204 })))

    expect(screen.queryByRole('button', { name: /市场协作/ })).not.toBeInTheDocument()
    expect(screen.getByRole('button', { name: /Yucheng/ })).toHaveClass('active')
  })

  it('switches Workspace copy to zh-CN without changing API-backed names', async () => {
    i18n.addResources('zh-CN', 'translation', zhTestResources)
    vi.stubGlobal('fetch', baseFetch((url) => {
      if (url === '/api/workspace/files/invalid.txt?openoctopus_device=server') {
        return new Response(new Uint8Array([255]), { headers: { ETag: '"invalid"' } })
      }
      if (url === '/api/workspace/list/.?openoctopus_device=server&recursive=false&limit=200&offset=0') {
        return json({
          ...rootEntries,
          items: [
            { name: 'SOUL.md', path: 'SOUL.md', kind: 'file', size: 42 },
            { name: 'invalid.txt', path: 'invalid.txt', kind: 'file', size: 1 },
          ],
        })
      }
      return undefined
    }))
    const user = userEvent.setup()
    renderPage()

    expect(await screen.findByText('Workspace')).toBeInTheDocument()
    await i18n.changeLanguage('zh-CN')
    expect(await screen.findByText('工作区')).toBeInTheDocument()
    expect(await screen.findByText('Agent 身份')).toBeInTheDocument()
    await user.click(await screen.findByRole('button', { name: /invalid\.txt/ }))
    expect(
      await screen.findByText('文件不是有效 UTF-8，无法作为文本编辑；可直接下载。 (file_not_utf8)'),
    ).toBeInTheDocument()
  })
})
