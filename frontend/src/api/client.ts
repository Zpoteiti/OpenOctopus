export interface ErrorEnvelope {
  code: string
  message: string
}

export class ApiError extends Error {
  readonly status: number
  readonly code: string

  constructor(
    status: number,
    code: string,
    message: string,
  ) {
    super(message)
    this.name = 'ApiError'
    this.status = status
    this.code = code
  }
}

function isErrorEnvelope(value: unknown): value is ErrorEnvelope {
  if (!value || typeof value !== 'object') return false
  const envelope = value as Record<string, unknown>
  return typeof envelope.code === 'string' && typeof envelope.message === 'string'
}

export async function apiJson<T = undefined>(
  input: string,
  init: RequestInit = {},
): Promise<T> {
  const headers = new Headers(init.headers)
  if (init.body && !(init.body instanceof FormData) && !headers.has('Content-Type')) {
    headers.set('Content-Type', 'application/json')
  }

  const response = await fetch(input, {
    ...init,
    credentials: 'same-origin',
    headers,
  })

  if (response.status === 204) return undefined as T

  const isJson = response.headers.get('content-type')?.includes('application/json') ?? false
  const body: unknown = isJson ? await response.json() : undefined
  if (!response.ok) {
    if (isErrorEnvelope(body)) throw new ApiError(response.status, body.code, body.message)
    throw new ApiError(response.status, 'request_failed', `Request failed (${response.status})`)
  }
  return body as T
}
