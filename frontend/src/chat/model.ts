export interface ContentBlock {
  type: string
  text?: string
  thinking?: string
  name?: string
  input?: unknown
  content?: unknown
  is_error?: boolean
  [key: string]: unknown
}

export interface ChatMessage {
  id: string
  session_id: string
  role: 'user' | 'assistant'
  message_kind: string
  content: ContentBlock[]
  delivery_refs: Record<string, unknown>[]
  is_compacted: boolean
  created_at: string
}

export interface PendingMessage {
  id: string
  session_id: string
  content: ContentBlock[]
  effort: string | null
  received_at: string
}

export interface MessageHistory {
  messages: ChatMessage[]
  pending_messages: PendingMessage[]
  status: 'idle' | 'running' | 'failed' | 'abandoned'
  active_turn_id: string | null
  last_message_id: string | null
  pending_count: number
  has_more_before: boolean
}

export const emptyHistory = (): MessageHistory => ({
  messages: [],
  pending_messages: [],
  status: 'idle',
  active_turn_id: null,
  last_message_id: null,
  pending_count: 0,
  has_more_before: false,
})

export function upsertMessage(messages: ChatMessage[], incoming: ChatMessage): ChatMessage[] {
  const byId = new Map(messages.map((message) => [message.id, message]))
  byId.set(incoming.id, incoming)
  return [...byId.values()].sort(compareMessages)
}

export function mergeHistory(current: MessageHistory, incoming: MessageHistory): MessageHistory {
  const messages = incoming.messages.reduce(upsertMessage, current.messages)
  const persistedIds = new Set(messages.map((message) => message.id))

  return {
    ...incoming,
    messages,
    pending_messages: incoming.pending_messages.filter((message) => !persistedIds.has(message.id)),
    has_more_before: current.has_more_before || incoming.has_more_before,
  }
}

function compareMessages(left: ChatMessage, right: ChatMessage): number {
  return left.created_at.localeCompare(right.created_at) || left.id.localeCompare(right.id)
}
