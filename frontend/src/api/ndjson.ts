export async function* parseNdjsonStream<T = unknown>(
  stream: ReadableStream<Uint8Array>,
): AsyncGenerator<T> {
  const reader = stream.getReader()
  const decoder = new TextDecoder()
  let buffer = ''

  const parse = (line: string): T => {
    try {
      return JSON.parse(line) as T
    } catch (error) {
      throw new Error('Invalid NDJSON record', { cause: error })
    }
  }

  try {
    while (true) {
      const { done, value } = await reader.read()
      buffer += decoder.decode(value, { stream: !done })
      const lines = buffer.split('\n')
      buffer = lines.pop() ?? ''
      for (const line of lines) {
        const record = line.endsWith('\r') ? line.slice(0, -1) : line
        if (record.trim()) yield parse(record)
      }
      if (done) break
    }
    if (buffer.trim()) yield parse(buffer)
  } finally {
    reader.releaseLock()
  }
}
