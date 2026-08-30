import '@testing-library/jest-dom/vitest'
import { cleanup } from '@testing-library/react'
import { afterEach, vi } from 'vitest'

import i18n from '../i18n'

Object.defineProperty(window, 'matchMedia', {
  configurable: true,
  value: vi.fn().mockReturnValue({
    matches: false,
    addEventListener: vi.fn(),
    removeEventListener: vi.fn(),
  }),
})

afterEach(async () => {
  cleanup()
  window.localStorage.clear()
  document.documentElement.removeAttribute('data-theme')
  await i18n.changeLanguage('en')
})
