import { ApiError, apiJson } from '../api/client'
import { parseNdjsonStream } from '../api/ndjson'
import type { Effort, Session } from '../api/types'
import i18n from '../i18n'
import type { ChatMessage, MessageAttachmentRef, MessageHistory } from './model'

export type { MessageAttachmentRef } from './model'

export type StreamEvent =
  | { type: 'message_accepted'; message_id: string; disposition: 'started' | 'queued'; created_session: boolean }
  | { type: 'turn_started'; turn_id: string; message_ids: string[] }
  | { type: 'token_delta'; turn_id: string; channel: 'text' | 'thinking'; text: string }
  | { type: 'tool_progress'; turn_id: string; kind: string; tool_call_id: string; tool_name: string }
  | { type: 'message_persisted'; turn_id: string; message: ChatMessage }
  | { type: 'turn_finished'; turn_id: string; status: 'completed' | 'failed' | 'cancelled' | 'abandoned'; final_message_id?: string | null }
  | { type: 'session_deleted'; session_id?: string }
  | { type: 'stream_replaced'; message_id: string; by_message_id: string }
  | { type: 'keepalive'; message_id?: string }

export interface SendChatMessageOptions {
  sessionId: string
  text: string
  attachments: MessageAttachmentRef[]
  effort?: Effort
  onEvent: (event: StreamEvent) => void
}

const MAX_ATTACHMENT_IMAGE_BYTES = 8 * 1024 * 1024
const SESSION_PAGE_LIMIT = 200
const MAX_SESSION_SCANS = 3

export class MessageStreamError extends Error {
  readonly accepted: boolean

  constructor(accepted: boolean, message: string, cause?: unknown) {
    super(message, { cause })
    this.name = 'MessageStreamError'
    this.accepted = accepted
  }
}

export async function loadSessions(): Promise<Session[]> {
  const scans: Session[][] = []
  let previousFingerprint: string | null = null

  for (let attempt = 0; attempt < MAX_SESSION_SCANS; attempt += 1) {
    const sessions = await scanSessions()
    const fingerprint = sessionFingerprint(sessions)
    if (fingerprint === previousFingerprint) return sessions
    scans.push(sessions)
    previousFingerprint = fingerprint
  }

  return mergeSessionScans(scans)
}

async function scanSessions(): Promise<Session[]> {
  const sessions = new Map<string, Session>()
  let offset = 0

  while (true) {
    const page = await apiJson<Session[]>(
      `/api/sessions?limit=${SESSION_PAGE_LIMIT}${offset ? `&offset=${offset}` : ''}`,
    )
    for (const session of page) sessions.set(session.id, session)
    if (page.length < SESSION_PAGE_LIMIT) return [...sessions.values()]
    offset += page.length
  }
}

function sessionFingerprint(sessions: Session[]): string {
  return JSON.stringify(sessions.map((session) => [session.id, session.last_inbound_at]))
}

function mergeSessionScans(scans: Session[][]): Session[] {
  const merged: Session[] = []
  const seen = new Set<string>()
  for (let index = scans.length - 1; index >= 0; index -= 1) {
    for (const session of scans[index]) {
      if (seen.has(session.id)) continue
      seen.add(session.id)
      merged.push(session)
    }
  }
  return merged
}

export function loadMessageHistory(sessionId: string, after?: string | null): Promise<MessageHistory> {
  const cursor = after ? `after=${encodeURIComponent(after)}&` : ''
  return apiJson<MessageHistory>(`/api/sessions/${encodeURIComponent(sessionId)}/messages?${cursor}limit=200`)
}

export function renameSession(sessionId: string, title: string): Promise<Session> {
  return apiJson<Session>(`/api/sessions/${encodeURIComponent(sessionId)}`, {
    method: 'PATCH',
    body: JSON.stringify({ title }),
  })
}

export function cancelSession(sessionId: string): Promise<{ cancel_requested: boolean }> {
  return apiJson<{ cancel_requested: boolean }>(`/api/sessions/${encodeURIComponent(sessionId)}/cancel`, {
    method: 'POST',
  })
}

export function deleteSession(sessionId: string): Promise<undefined> {
  return apiJson(`/api/sessions/${encodeURIComponent(sessionId)}`, { method: 'DELETE' })
}

export async function sendChatMessage(options: SendChatMessageOptions): Promise<void> {
  if (options.attachments.length > 10) {
    throw new Error(i18n.t('chat.tooManyAttachments', {
      defaultValue: 'A message can include at most 10 attachments.',
    }))
  }
  const hasText = options.text.trim().length > 0
  const body = {
    ...(options.effort === undefined ? {} : { effort: options.effort }),
    content: hasText ? [{ type: 'text', text: options.text }] : [],
    attachments: options.attachments,
  }

  let response: Response
  try {
    response = await fetch(`/api/sessions/${encodeURIComponent(options.sessionId)}/messages`, {
      method: 'POST',
      credentials: 'same-origin',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
    })
  } catch (cause) {
    throw ambiguousStreamError(false, cause)
  }

  if (!response.ok) throw await responseError(response)
  if (!response.body) throw ambiguousStreamError(false)

  let accepted = false
  try {
    for await (const event of parseNdjsonStream<StreamEvent>(response.body)) {
      if (event.type === 'message_accepted') accepted = true
      options.onEvent(event)
    }
  } catch (cause) {
    if (cause instanceof ApiError || cause instanceof MessageStreamError) throw cause
    throw ambiguousStreamError(accepted, cause)
  }

  if (!accepted) throw ambiguousStreamError(false)
}

export async function uploadBrowserAttachment(file: File, uploadId: string): Promise<MessageAttachmentRef> {
  await validateBrowserAttachmentFiles([file])
  const path = `.attachments/uploads/${uploadId}/${safeFilename(file.name)}`
  const encodedPath = path.split('/').map(encodeURIComponent).join('/')
  await apiJson(`/api/workspace/files/${encodedPath}?openoctopus_device=server`, {
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream',
      'If-None-Match': '*',
    },
    body: file,
  })
  return { openoctopus_device: 'server', path }
}

export async function validateBrowserAttachmentFiles(files: File[]): Promise<void> {
  let imageBytes = 0
  for (const file of files) {
    const header = new Uint8Array(await file.slice(0, 12).arrayBuffer())
    if (!isSupportedImage(header)) continue
    imageBytes += file.size
    if (imageBytes > MAX_ATTACHMENT_IMAGE_BYTES) {
      throw new Error(i18n.t('chat.imagesTooLarge', {
        defaultValue: 'Attachment images can contain at most 8 MiB in total.',
      }))
    }
  }
}

function isSupportedImage(header: Uint8Array): boolean {
  const startsWith = (...signature: number[]): boolean => signature.every((byte, index) => header[index] === byte)
  return startsWith(0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a)
    || startsWith(0xff, 0xd8, 0xff)
    || startsWith(0x47, 0x49, 0x46, 0x38, 0x37, 0x61)
    || startsWith(0x47, 0x49, 0x46, 0x38, 0x39, 0x61)
    || (startsWith(0x52, 0x49, 0x46, 0x46)
      && header[8] === 0x57
      && header[9] === 0x45
      && header[10] === 0x42
      && header[11] === 0x50)
}

function safeFilename(name: string): string {
  const cleaned = name.replaceAll('/', '_').replaceAll('\\', '_').trim()
  return cleaned && cleaned !== '.' && cleaned !== '..' ? cleaned : 'attachment'
}

async function responseError(response: Response): Promise<ApiError> {
  if (response.headers.get('content-type')?.includes('application/json')) {
    const value = await response.json() as unknown
    if (isErrorEnvelope(value)) return new ApiError(response.status, value.code, value.message)
  }
  return new ApiError(response.status, 'request_failed', `Request failed (${response.status})`)
}

function isErrorEnvelope(value: unknown): value is { code: string; message: string } {
  if (!value || typeof value !== 'object') return false
  const envelope = value as Record<string, unknown>
  return typeof envelope.code === 'string' && typeof envelope.message === 'string'
}

function ambiguousStreamError(accepted: boolean, cause?: unknown): MessageStreamError {
  const message = accepted
    ? i18n.t('chat.acceptedDisconnected', {
        defaultValue: 'The Server accepted the message, but the live connection closed. Recovering from saved history.',
      })
    : i18n.t('chat.confirmationDisconnected', {
        defaultValue: 'The connection closed before the Server confirmed receipt. The message may have been accepted; it was not retried automatically. Refresh the conversation to check.',
      })
  return new MessageStreamError(accepted, message, cause)
}
