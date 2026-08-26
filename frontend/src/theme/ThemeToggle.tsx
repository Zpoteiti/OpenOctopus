import { useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { applyTheme, cycleTheme, getStoredTheme, storeTheme, type ThemePreference } from './theme'

const ICONS: Record<ThemePreference, string> = {
  auto: '◐',
  light: '☀',
  dark: '☾',
}

export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const { t } = useTranslation()
  const [theme, setTheme] = useState(getStoredTheme)
  useEffect(() => applyTheme(theme), [theme])
  const mode = t(`theme.${theme}`)
  const label = t('theme.label', { mode })

  const change = (): void => {
    const next = cycleTheme(theme)
    storeTheme(next)
    setTheme(next)
  }

  return (
    <button
      className="theme-toggle"
      type="button"
      onClick={change}
      aria-label={label}
      title={label}
    >
      <span aria-hidden="true">{ICONS[theme]}</span>
      {compact ? null : <span>{mode}</span>}
    </button>
  )
}
