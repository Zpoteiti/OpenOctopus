import { useTranslation } from 'react-i18next'

import { LANGUAGE_OPTIONS, normalizeLanguage } from './index'

export function LanguageSelector() {
  const { i18n, t } = useTranslation()
  const language = normalizeLanguage(i18n.resolvedLanguage)

  return (
    <label className="language-selector">
      <span aria-hidden="true">文</span>
      <span className="sr-only">{t('language.label')}</span>
      <select
        aria-label={t('language.label')}
        value={language}
        onChange={(event) => void i18n.changeLanguage(event.target.value)}
      >
        {LANGUAGE_OPTIONS.map(({ code, label }) => <option key={code} value={code}>{label}</option>)}
      </select>
    </label>
  )
}
