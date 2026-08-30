import { ApiError, apiJson } from '../api/client'
import type { components } from '../api/openapi'

export type Workspace = components['schemas']['Workspace']
export type WorkspacePageResponse = components['schemas']['WorkspacePage']
export type WorkspaceMember = components['schemas']['WorkspaceMember']
export type WorkspaceMemberPage = components['schemas']['WorkspaceMemberPage']
export type Device = components['schemas']['Device']
export type ListDirEntry = components['schemas']['ListDirEntry']
export type ListDirEntryPage = components['schemas']['ListDirEntryPage']
export type FileMutation = components['schemas']['FileMutation']

const PAGE_LIMIT = 200
const DIRECTORY_PAGE_LIMIT = 200

export const MAX_TEXT_PREVIEW_BYTES = 1024 * 1024

export function listWorkspaces(): Promise<WorkspacePageResponse> {
  return collectPages((offset) => apiJson(`/api/workspaces?limit=${PAGE_LIMIT}&offset=${offset}`))
}

export function listDevices(): Promise<Device[]> {
  return apiJson('/api/devices')
}

export async function listDirectory(
  path: string,
  device: string,
  deviceId?: string,
): Promise<ListDirEntryPage> {
  let offset = 0
  const seenOffsets = new Set([offset])
  let page = await directoryPage(path, device, deviceId, offset)
  const items = [...page.items]

  while (page.next_offset !== null) {
    const nextOffset = page.next_offset
    if (nextOffset <= offset || seenOffsets.has(nextOffset)) {
      throw new ApiError(502, 'request_failed', 'Directory pagination did not advance')
    }
    offset = nextOffset
    seenOffsets.add(offset)
    page = await directoryPage(path, device, deviceId, offset)
    items.push(...page.items)
  }

  return {
    items,
    limit: DIRECTORY_PAGE_LIMIT,
    offset: 0,
    next_offset: page.truncated ? page.next_offset : null,
    truncated: page.truncated,
  }
}

function directoryPage(
  path: string,
  device: string,
  deviceId: string | undefined,
  offset: number,
): Promise<ListDirEntryPage> {
  const deviceFence = deviceId
    ? `&openoctopus_device_id=${encodeURIComponent(deviceId)}`
    : ''
  return apiJson(
    `${workspacePathUrl('/api/workspace/list', path)}?openoctopus_device=${encodeURIComponent(device)}${deviceFence}&recursive=false&limit=${DIRECTORY_PAGE_LIMIT}&offset=${offset}`,
  )
}

export async function readTextFile(
  path: string,
  device: string,
): Promise<{ content: string; etag: string }> {
  const response = await rawRequest(fileUrl(path, device))
  const etag = response.headers.get('ETag')
  if (!etag) throw new ApiError(502, 'request_failed', 'File response did not include an ETag')
  const declaredLength = Number(response.headers.get('Content-Length'))
  if (Number.isFinite(declaredLength) && declaredLength > MAX_TEXT_PREVIEW_BYTES) {
    await response.body?.cancel()
    throw new ApiError(413, 'file_preview_too_large', 'File exceeds the text preview limit')
  }

  const bytes = await readBoundedBody(response)
  try {
    return { content: new TextDecoder('utf-8', { fatal: true }).decode(bytes), etag }
  } catch (caught) {
    if (caught instanceof TypeError) {
      throw new ApiError(415, 'file_not_utf8', 'File is not valid UTF-8')
    }
    throw caught
  }
}

async function readBoundedBody(response: Response): Promise<Uint8Array> {
  if (!response.body) {
    const bytes = new Uint8Array(await response.arrayBuffer())
    if (bytes.byteLength > MAX_TEXT_PREVIEW_BYTES) {
      throw new ApiError(413, 'file_preview_too_large', 'File exceeds the text preview limit')
    }
    return bytes
  }

  const reader = response.body.getReader()
  const chunks: Uint8Array[] = []
  let length = 0
  try {
    while (true) {
      const { done, value } = await reader.read()
      if (done) break
      length += value.byteLength
      if (length > MAX_TEXT_PREVIEW_BYTES) {
        await reader.cancel()
        throw new ApiError(413, 'file_preview_too_large', 'File exceeds the text preview limit')
      }
      chunks.push(value)
    }
  } finally {
    reader.releaseLock()
  }

  const bytes = new Uint8Array(length)
  let offset = 0
  for (const chunk of chunks) {
    bytes.set(chunk, offset)
    offset += chunk.byteLength
  }
  return bytes
}

export async function saveTextFile(
  path: string,
  device: string,
  content: string,
  etag: string | null,
): Promise<{ mutation: FileMutation; etag: string }> {
  const response = await rawRequest(fileUrl(path, device), {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      [etag === null ? 'If-None-Match' : 'If-Match']: etag ?? '*',
    },
    body: content,
  })
  const mutation = await response.json() as FileMutation
  return { mutation, etag: response.headers.get('ETag') ?? quoteEtag(mutation.etag) }
}

export async function uploadFile(
  path: string,
  device: string,
  file: File,
): Promise<FileMutation> {
  const response = await rawRequest(fileUrl(path, device), {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      'If-None-Match': '*',
    },
    body: file,
  })
  return await response.json() as FileMutation
}

export async function deleteFile(
  path: string,
  device: string,
  etag: string | null,
): Promise<void> {
  const headers = etag ? { 'If-Match': etag } : undefined
  await rawRequest(fileUrl(path, device), { method: 'DELETE', headers })
}

export function createSharedWorkspace(name: string, quotaBytes: number): Promise<Workspace> {
  return apiJson('/api/workspaces', {
    method: 'POST',
    body: JSON.stringify({ name, quota_bytes: quotaBytes }),
  })
}

export function listMembers(workspaceRef: string): Promise<WorkspaceMemberPage> {
  return collectPages((offset) => apiJson(
    `/api/workspaces/${encodeURIComponent(workspaceRef)}/members?limit=${PAGE_LIMIT}&offset=${offset}`,
  ))
}

export function addMember(workspaceRef: string, email: string): Promise<WorkspaceMember> {
  return apiJson(`/api/workspaces/${encodeURIComponent(workspaceRef)}/members`, {
    method: 'POST',
    body: JSON.stringify({ email }),
  })
}

export function removeMember(workspaceRef: string, userId: string): Promise<void> {
  return apiJson(
    `/api/workspaces/${encodeURIComponent(workspaceRef)}/members/${encodeURIComponent(userId)}`,
    { method: 'DELETE' },
  )
}

export function fileUrl(path: string, device: string): string {
  return `${workspacePathUrl('/api/workspace/files', path)}?openoctopus_device=${encodeURIComponent(device)}`
}

function workspacePathUrl(prefix: string, path: string): string {
  return `${prefix}/${path.split('/').map((part) => encodeURIComponent(part)).join('/')}`
}

interface Page<T> {
  items: T[]
  limit: number
  offset: number
  next_offset: number | null
  truncated: boolean
}

async function collectPages<T, P extends Page<T>>(
  fetchPage: (offset: number) => Promise<P>,
): Promise<P> {
  let offset = 0
  const seenOffsets = new Set([offset])
  const first = await fetchPage(offset)
  let page = first
  const items = [...page.items]
  let truncated = page.truncated

  while (page.next_offset !== null) {
    const nextOffset = page.next_offset
    if (nextOffset <= offset || seenOffsets.has(nextOffset)) {
      throw new ApiError(502, 'request_failed', 'Pagination did not advance')
    }
    offset = nextOffset
    seenOffsets.add(offset)
    page = await fetchPage(offset)
    items.push(...page.items)
    truncated ||= page.truncated
  }

  return { ...first, items, next_offset: null, truncated }
}

async function rawRequest(input: string, init: RequestInit = {}): Promise<Response> {
  const response = await fetch(input, { ...init, credentials: 'same-origin' })
  if (response.ok) return response

  let code = 'request_failed'
  let message = `Request failed (${response.status})`
  if (response.headers.get('content-type')?.includes('application/json')) {
    const body = await response.json() as { code?: unknown; message?: unknown }
    if (typeof body.code === 'string') code = body.code
    if (typeof body.message === 'string') message = body.message
  }
  throw new ApiError(response.status, code, message)
}

function quoteEtag(etag: string): string {
  return etag.startsWith('"') ? etag : `"${etag}"`
}
