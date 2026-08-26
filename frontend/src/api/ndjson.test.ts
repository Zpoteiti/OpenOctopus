import { describe, expect, it } from 'vitest'

import { parseNdjsonStream } from './ndjson'

function streamChunks(...chunks: string[]): ReadableStream<Uint8Array> {
  const encoder = new TextEncoder()
  return new ReadableStream({
    start(controller) {
      chunks.forEach((chunk) => controller.enqueue(encoder.encode(chunk)))
      controller.close()
    },
  })
}

describe('parseNdjsonStream', () => {
  it('reassembles records split across transport chunks', async () => {
    const stream = streamChunks(
      '{"type":"message_ac',
      'cepted","turn_id":"turn-1"}\n{"type":"token_delta",',
      '"text":"hello"}\n',
    )

    const records: unknown[] = []
    for await (const record of parseNdjsonStream(stream)) records.push(record)

    expect(records).toEqual([
      { type: 'message_accepted', turn_id: 'turn-1' },
      { type: 'token_delta', text: 'hello' },
    ])
  })

  it('accepts a final record without a trailing newline', async () => {
    const records: unknown[] = []
    for await (const record of parseNdjsonStream(streamChunks('{"type":"done"}'))) {
      records.push(record)
    }
    expect(records).toEqual([{ type: 'done' }])
  })

  it('rejects malformed records instead of silently dropping them', async () => {
    const consume = async () => {
      for await (const record of parseNdjsonStream(streamChunks('{not-json}\n'))) void record
    }
    await expect(consume()).rejects.toThrow('Invalid NDJSON record')
  })
})
