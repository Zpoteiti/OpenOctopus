import { describe, expect, it, vi } from 'vitest'

import { applyTheme, cycleTheme, getStoredTheme, storeTheme } from './theme'

describe('theme preference', () => {
  it('defaults to automatic system theme', () => {
    expect(getStoredTheme()).toBe('auto')
  })

  it('cycles auto, light, dark', () => {
    expect(cycleTheme('auto')).toBe('light')
    expect(cycleTheme('light')).toBe('dark')
    expect(cycleTheme('dark')).toBe('auto')
  })

  it('persists an explicit choice', () => {
    storeTheme('dark')
    expect(getStoredTheme()).toBe('dark')
  })

  it('falls back to automatic mode when storage is unavailable', () => {
    const getItem = vi.spyOn(Storage.prototype, 'getItem').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError')
    })
    expect(getStoredTheme()).toBe('auto')
    getItem.mockRestore()

    const setItem = vi.spyOn(Storage.prototype, 'setItem').mockImplementation(() => {
      throw new DOMException('blocked', 'SecurityError')
    })
    expect(() => storeTheme('dark')).not.toThrow()
    setItem.mockRestore()
  })

  it('applies the current system preference in auto mode', () => {
    vi.stubGlobal(
      'matchMedia',
      vi.fn().mockReturnValue({ matches: true, addEventListener: vi.fn(), removeEventListener: vi.fn() }),
    )
    const cleanup = applyTheme('auto')
    expect(document.documentElement.dataset.theme).toBe('dark')
    cleanup()
    vi.unstubAllGlobals()
  })
})
