import { createContext, type ReactNode, useContext, useEffect, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { applyTheme, cycleTheme, getStoredTheme, storeTheme, type ThemePreference } from './theme'

const ICONS: Record<ThemePreference, string> = {
  auto: '◐',
  light: '☀',
  dark: '☾',
}

type ThemeContextValue = {
  theme: ThemePreference
  setTheme: (theme: ThemePreference) => void
}

const ThemeContext = createContext<ThemeContextValue | null>(null)

export function ThemeProvider({ children }: { children: ReactNode }): ReactNode {
  const [theme, setThemeState] = useState(getStoredTheme)
  useEffect(() => applyTheme(theme), [theme])

  const setTheme = (next: ThemePreference): void => {
    storeTheme(next)
    setThemeState(next)
  }

  return <ThemeContext.Provider value={{ theme, setTheme }}>{children}</ThemeContext.Provider>
}

export function ThemeToggle({ compact = false }: { compact?: boolean }) {
  const { t } = useTranslation()
  const preference = useContext(ThemeContext)
  if (!preference) throw new Error('ThemeToggle must be rendered inside ThemeProvider')
  const { theme, setTheme } = preference
  const mode = t(`theme.${theme}`)
  const label = t('theme.label', { mode })

  const change = (): void => {
    const next = cycleTheme(theme)
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
