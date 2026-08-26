import type { ReactNode } from 'react'
import { useTranslation } from 'react-i18next'

import { ApiError } from '../api/client'

export function PageHeader({
  eyebrow,
  title,
  description,
  actions,
}: {
  eyebrow?: string
  title: string
  description?: string
  actions?: ReactNode
}): ReactNode {
  return (
    <header className="page-header">
      <div>
        {eyebrow ? <span className="eyebrow">{eyebrow}</span> : null}
        <h1>{title}</h1>
        {description ? <p>{description}</p> : null}
      </div>
      {actions ? <div className="page-actions">{actions}</div> : null}
    </header>
  )
}

export function Card({
  title,
  description,
  actions,
  children,
  tone,
}: {
  title?: string
  description?: string
  actions?: ReactNode
  children: ReactNode
  tone?: 'danger'
}): ReactNode {
  return (
    <section className={`card${tone ? ` card-${tone}` : ''}`}>
      {title || description || actions ? (
        <header className="card-header">
          <div>
            {title ? <h2>{title}</h2> : null}
            {description ? <p>{description}</p> : null}
          </div>
          {actions}
        </header>
      ) : null}
      <div className="card-body">{children}</div>
    </section>
  )
}

export function ErrorNotice({ error }: { error: unknown }): ReactNode {
  const { t } = useTranslation()
  if (!error) return null
  const message = error instanceof Error ? error.message : t('common.requestFailed')
  const code = error instanceof ApiError && error.code && !message.includes(error.code) ? error.code : null
  return <p className="form-error" role="alert">{message}{code ? <> <code>{code}</code></> : null}</p>
}

export function StatusBadge({ children, tone = 'neutral' }: { children: ReactNode; tone?: 'neutral' | 'success' | 'warning' | 'danger' }): ReactNode {
  return <span className={`status-badge status-${tone}`}>{children}</span>
}
