import { afterEach, describe, expect, it, vi } from 'vitest'

import { ApiError, apiJson } from './client'

afterEach(() => vi.unstubAllGlobals())

describe('apiJson', () => {
  it('uses same-origin cookies and returns JSON', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      new Response(JSON.stringify({ id: 'user-1' }), {
        status: 200,
        headers: { 'Content-Type': 'application/json' },
      }),
    )
    vi.stubGlobal('fetch', fetchMock)

    await expect(apiJson('/api/me')).resolves.toEqual({ id: 'user-1' })
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/me',
      expect.objectContaining({ credentials: 'same-origin' }),
    )
  })

  it('preserves the stable server error code', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn().mockResolvedValue(
        new Response(
          JSON.stringify({ code: 'auth_invalid_credentials', message: 'Invalid credentials' }),
          { status: 401, headers: { 'Content-Type': 'application/json' } },
        ),
      ),
    )

    await expect(apiJson('/api/auth/login')).rejects.toEqual(
      new ApiError(401, 'auth_invalid_credentials', 'Invalid credentials'),
    )
  })

  it('returns undefined for a successful empty response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue(new Response(null, { status: 204 })))
    await expect(apiJson('/api/auth/logout', { method: 'POST' })).resolves.toBeUndefined()
  })
})
