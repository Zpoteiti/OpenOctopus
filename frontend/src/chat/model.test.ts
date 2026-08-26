import { describe, expect, it } from 'vitest'

import { mergeHistory, upsertMessage, type ChatMessage, type MessageHistory } from './model'

function message(id: string, text: string, createdAt: string): ChatMessage {
  return {
    id,
    session_id: 'session-1',
    role: 'assistant',
    message_kind: 'assistant',
    content: [{ type: 'text', text }],
    delivery_refs: [],
    is_compacted: false,
    created_at: createdAt,
  }
}

describe('chat history reconciliation', () => {
  it('upserts repeated message_persisted snapshots by id', () => {
    const original = message('message-1', 'draft', '2026-08-26T10:00:00Z')
    const updated = { ...original, content: [{ type: 'text', text: 'final' }], delivery_refs: [{ type: 'workspace_file' }] }

    expect(upsertMessage([original], updated)).toEqual([updated])
  })

  it('merges cursor recovery pages and removes promoted pending rows', () => {
    const original: MessageHistory = {
      messages: [message('message-1', 'first', '2026-08-26T10:00:00Z')],
      pending_messages: [{
        id: 'message-2',
        session_id: 'session-1',
        content: [{ type: 'text', text: 'pending' }],
        effort: null,
        received_at: '2026-08-26T10:01:00Z',
      }],
      status: 'running',
      active_turn_id: 'turn-1',
      last_message_id: 'message-1',
      pending_count: 1,
      has_more_before: false,
    }
    const recovery: MessageHistory = {
      messages: [message('message-2', 'pending', '2026-08-26T10:01:01Z')],
      pending_messages: [],
      status: 'idle',
      active_turn_id: null,
      last_message_id: 'message-2',
      pending_count: 0,
      has_more_before: true,
    }

    expect(mergeHistory(original, recovery)).toMatchObject({
      messages: [original.messages[0], recovery.messages[0]],
      pending_messages: [],
      status: 'idle',
      last_message_id: 'message-2',
      has_more_before: true,
    })
  })
})
