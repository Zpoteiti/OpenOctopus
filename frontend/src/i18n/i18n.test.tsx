import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, describe, expect, it } from 'vitest'

import i18n, { LANGUAGE_OPTIONS, LANGUAGE_STORAGE_KEY, normalizeLanguage } from './index'
import { LanguageSelector } from './LanguageSelector'
import { en, zhCN } from './resources'

function leafKeys(value: object, prefix = ''): string[] {
  return Object.entries(value).flatMap(([key, child]) => {
    const path = prefix ? `${prefix}.${key}` : key
    return typeof child === 'object' && child !== null ? leafKeys(child, path) : [path]
  })
}

afterEach(async () => {
  window.localStorage.clear()
  await i18n.changeLanguage('en')
})

describe('application language', () => {
  it('keeps every supported locale structurally complete', () => {
    expect(leafKeys(zhCN).sort()).toEqual(leafKeys(en).sort())
  })

  it('registers every selectable language in one extensible catalog', () => {
    expect(LANGUAGE_OPTIONS.map(({ code, label }) => ({ code, label }))).toEqual([
      { code: 'en', label: 'English' },
      { code: 'zh-CN', label: '简体中文' },
    ])
    expect(normalizeLanguage('zh-CN')).toBe('zh-CN')
    expect(normalizeLanguage('unsupported')).toBe('en')
  })

  it('defaults to English and persists an explicit language choice', async () => {
    await i18n.changeLanguage('en')
    render(<LanguageSelector />)
    expect(screen.getByRole('combobox', { name: 'Language' })).toHaveValue('en')

    await userEvent.selectOptions(screen.getByRole('combobox', { name: 'Language' }), 'zh-CN')
    await waitFor(() => expect(document.documentElement.lang).toBe('zh-CN'))
    expect(window.localStorage.getItem(LANGUAGE_STORAGE_KEY)).toBe('zh-CN')
    expect(screen.getByRole('combobox', { name: '语言' })).toHaveValue('zh-CN')
  })
})
