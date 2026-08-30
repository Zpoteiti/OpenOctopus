export type ThemePreference = 'auto' | 'light' | 'dark'
type AppliedTheme = Exclude<ThemePreference, 'auto'>

const STORAGE_KEY = 'openoctopus-theme'

export function getStoredTheme(): ThemePreference {
  try {
    const value = window.localStorage.getItem(STORAGE_KEY)
    return value === 'light' || value === 'dark' || value === 'auto' ? value : 'auto'
  } catch {
    return 'auto'
  }
}

export function storeTheme(theme: ThemePreference): void {
  try {
    window.localStorage.setItem(STORAGE_KEY, theme)
  } catch {
    // Browser privacy settings can disable storage without disabling the UI.
  }
}

export function cycleTheme(theme: ThemePreference): ThemePreference {
  if (theme === 'auto') return 'light'
  if (theme === 'light') return 'dark'
  return 'auto'
}

export function applyTheme(theme: ThemePreference): () => void {
  const media = window.matchMedia('(prefers-color-scheme: dark)')
  const render = (): void => {
    const applied: AppliedTheme = theme === 'auto' ? (media.matches ? 'dark' : 'light') : theme
    document.documentElement.dataset.theme = applied
    document.documentElement.style.colorScheme = applied
  }
  render()
  if (theme === 'auto') media.addEventListener('change', render)
  return () => media.removeEventListener('change', render)
}
