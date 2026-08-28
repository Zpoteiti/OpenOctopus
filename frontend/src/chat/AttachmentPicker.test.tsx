import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it, vi } from 'vitest'

import type { Device } from '../api/types'
import { AttachmentPicker } from './AttachmentPicker'

const personalWorkspace = {
  id: 'user-1', name: 'Personal', type: 'personal', quota_bytes: 500, bytes_used: 10, locked: false,
}
const sharedWorkspace = {
  id: 'workspace-1', name: 'Marketing', suffix: 'a4f7e2d1', ref: 'Marketing@a4f7e2d1',
  type: 'shared', quota_bytes: 500, bytes_used: 20, locked: false, created_by: 'user-1',
}
const device = {
  id: '11111111-1111-4111-8111-111111111111', name: 'laptop-cn', online: true,
} as Device

function json(value: unknown): Response {
  return new Response(JSON.stringify(value), { headers: { 'Content-Type': 'application/json' } })
}

function directory(items: unknown[]): Response {
  return json({ items, limit: 200, offset: 0, next_offset: null, truncated: false })
}

function deferred<T>(): { promise: Promise<T>; resolve: (value: T) => void } {
  let resolve!: (value: T) => void
  const promise = new Promise<T>((next) => { resolve = next })
  return { promise, resolve }
}

afterEach(() => vi.unstubAllGlobals())

describe('AttachmentPicker', () => {
  it('focuses the close action when opened and closes on Escape', async () => {
    const onClose = vi.fn()
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url.includes('/api/workspace/list/.?openoctopus_device=laptop-cn')) return directory([])
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    render(<AttachmentPicker source={{ kind: 'device', device }} onSelect={() => undefined} onClose={onClose} />)

    expect(screen.getByRole('button', { name: 'Close' })).toHaveFocus()
    await user.keyboard('{Escape}')
    expect(onClose).toHaveBeenCalledOnce()
  })

  it('references personal and shared Server Workspace files without reading or uploading bytes', async () => {
    const onSelect = vi.fn()
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      expect(init?.method).toBeUndefined()
      if (url === '/api/workspaces?limit=200&offset=0') {
        return json({ items: [personalWorkspace, sharedWorkspace], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url.includes('/api/workspace/list/.?openoctopus_device=server')) {
        return directory([{ name: 'personal.txt', path: 'personal.txt', kind: 'file', size: 12 }])
      }
      if (url.includes('/api/workspace/list//Marketing%40a4f7e2d1?openoctopus_device=server')) {
        return directory([{ name: 'brief.pdf', path: '/Marketing@a4f7e2d1/brief.pdf', kind: 'file', size: 22 }])
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    render(<AttachmentPicker source={{ kind: 'server' }} onSelect={onSelect} onClose={() => undefined} />)

    await user.click(await screen.findByRole('button', { name: /personal\.txt/i }))
    expect(onSelect).toHaveBeenLastCalledWith({ openoctopus_device: 'server', path: 'personal.txt' })

    await user.click(screen.getByRole('button', { name: /Marketing/ }))
    await user.click(await screen.findByRole('button', { name: /brief\.pdf/i }))
    expect(onSelect).toHaveBeenLastCalledWith({
      openoctopus_device: 'server',
      path: '/Marketing@a4f7e2d1/brief.pdf',
    })
    expect(fetchMock.mock.calls.some(([, init]) => ['PUT', 'POST'].includes(init?.method ?? ''))).toBe(false)
  })

  it('references a connected Client file with its immutable id and never fetches file bytes', async () => {
    const onSelect = vi.fn()
    const fetchMock = vi.fn(async (input: string | URL | Request, init?: RequestInit) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      expect(init?.method).toBeUndefined()
      if (url.includes('/api/workspace/list/.?openoctopus_device=laptop-cn')) {
        return directory([
          { name: 'notes', path: 'notes', kind: 'directory', size: 0 },
          { name: 'report.pdf', path: 'report.pdf', kind: 'file', size: 42 },
        ])
      }
      throw new Error(`Unexpected request: ${url}`)
    })
    vi.stubGlobal('fetch', fetchMock)
    const user = userEvent.setup()

    render(<AttachmentPicker source={{ kind: 'device', device }} onSelect={onSelect} onClose={() => undefined} />)

    expect(screen.getByRole('dialog').querySelector('.chat-attachment-picker-body'))
      .toHaveClass('chat-attachment-picker-body-device')
    await user.click(await screen.findByRole('button', { name: /report\.pdf/i }))
    expect(onSelect).toHaveBeenCalledWith({
      openoctopus_device: 'laptop-cn',
      device_id: '11111111-1111-4111-8111-111111111111',
      path: 'report.pdf',
    })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(fetchMock.mock.calls[0]?.[0]).toContain('/api/workspace/list/')
    expect(fetchMock.mock.calls[0]?.[0]).toContain(
      'openoctopus_device_id=11111111-1111-4111-8111-111111111111',
    )
  })

  it('browses directories but only returns file selections', async () => {
    const onSelect = vi.fn()
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url.includes('/api/workspace/list/.?')) {
        return directory([{ name: 'docs', path: 'docs', kind: 'directory', size: 0 }])
      }
      if (url.includes('/api/workspace/list/docs?')) {
        return directory([{ name: 'guide.md', path: 'docs/guide.md', kind: 'file', size: 9 }])
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    render(<AttachmentPicker source={{ kind: 'device', device }} onSelect={onSelect} onClose={() => undefined} />)

    await user.click(await screen.findByRole('button', { name: /docs/i }))
    expect(onSelect).not.toHaveBeenCalled()
    await user.click(await screen.findByRole('button', { name: /guide\.md/i }))
    await waitFor(() => expect(onSelect).toHaveBeenCalledOnce())
    expect(vi.mocked(fetch).mock.calls.every(([url]) => String(url).includes(
        'openoctopus_device_id=11111111-1111-4111-8111-111111111111',
      ))).toBe(true)
  })

  it('keeps the selected Workspace directory when an older request finishes late', async () => {
    const personalDirectory = deferred<Response>()
    vi.stubGlobal('fetch', vi.fn(async (input: string | URL | Request) => {
      const url = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url
      if (url === '/api/workspaces?limit=200&offset=0') {
        return json({ items: [personalWorkspace, sharedWorkspace], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url.includes('/api/workspace/list/.?openoctopus_device=server')) {
        return personalDirectory.promise
      }
      if (url.includes('/api/workspace/list//Marketing%40a4f7e2d1?openoctopus_device=server')) {
        return directory([{ name: 'brief.pdf', path: '/Marketing@a4f7e2d1/brief.pdf', kind: 'file', size: 22 }])
      }
      throw new Error(`Unexpected request: ${url}`)
    }))
    const user = userEvent.setup()

    render(<AttachmentPicker source={{ kind: 'server' }} onSelect={() => undefined} onClose={() => undefined} />)

    await user.click(await screen.findByRole('button', { name: /Marketing/ }))
    expect(await screen.findByRole('button', { name: /brief\.pdf/i })).toBeInTheDocument()

    personalDirectory.resolve(directory([
      { name: 'personal.txt', path: 'personal.txt', kind: 'file', size: 12 },
    ]))

    await waitFor(() => expect(screen.queryByRole('button', { name: /personal\.txt/i })).not.toBeInTheDocument())
    expect(screen.getByRole('button', { name: /brief\.pdf/i })).toBeInTheDocument()
  })
})
