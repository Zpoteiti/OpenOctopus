import { useQuery } from '@tanstack/react-query'
import type { ReactNode } from 'react'
import { Link, NavLink, Outlet } from 'react-router-dom'
import { useTranslation } from 'react-i18next'

import { Brand } from '../auth/auth'
import { useAuthenticatedUser } from '../auth/context'
import { loadSessions } from '../chat/chatApi'

const NAV_ITEMS = [
  { to: '/chat', icon: 'C', labelKey: 'nav.chat' },
  { to: '/workspace', icon: 'W', labelKey: 'nav.workspace' },
  { to: '/devices', icon: 'D', labelKey: 'nav.devices' },
] as const

export function AppShell(): ReactNode {
  const { i18n, t } = useTranslation()
  const user = useAuthenticatedUser()
  const sessions = useQuery({
    queryKey: ['sessions'],
    queryFn: loadSessions,
    staleTime: 15_000,
  })
  const initials = user.name.trim().slice(0, 2).toUpperCase() || user.email.slice(0, 2).toUpperCase()

  return (
    <main className="stage">
      <div className="app-shell">
        <aside className="sidebar">
          <Brand />
          <Link className="new-session" to="/chat"><span aria-hidden="true">＋</span> {t('nav.newChat')}</Link>
          <nav className="nav" aria-label={t('nav.mainLabel')}>
            {NAV_ITEMS.map((item) => (
              <NavLink key={item.to} to={item.to} className="nav-item">
                <span className="nav-icon" aria-hidden="true">{item.icon}</span>
                {t(item.labelKey)}
              </NavLink>
            ))}
            {user.is_admin ? (
              <NavLink to="/admin/settings" className="nav-item">
                <span className="nav-icon" aria-hidden="true">A</span>
                {t('nav.admin')}
              </NavLink>
            ) : null}
          </nav>
          <div className="section-label"><span>{t('nav.recentChats')}</span></div>
          <div className="session-list">
            {sessions.data?.length ? (
              sessions.data.map((session) => (
                <Link className="session-item" to={`/chat/${session.id}`} key={session.id}>
                  <span className="session-copy">
                    <strong className="session-title">{session.title}</strong>
                    <small className="session-meta">{formatSessionTime(session.last_inbound_at, i18n.language, t)}</small>
                  </span>
                  {session.unread ? <span className="session-dot" aria-label={t('nav.unread')} /> : null}
                </Link>
              ))
            ) : (
              <p className="empty-sidebar">{sessions.isPending ? t('nav.loadingChats') : t('nav.noChats')}</p>
            )}
          </div>
          <footer className="sidebar-footer">
            <Link className="profile-button" to="/account" aria-label={t('account.title')} title={t('account.title')}>
              <span className="avatar">{initials}</span>
              <span className="profile-copy">
                <strong className="profile-name">{user.name}</strong>
                <small className="profile-role">{user.is_admin ? t('nav.administrator') : t('nav.member')}</small>
              </span>
            </Link>
          </footer>
        </aside>
        <section className="workspace"><Outlet /></section>
      </div>
    </main>
  )
}

function formatSessionTime(value: string | null, language: string, t: (key: string, options?: Record<string, unknown>) => string): string {
  if (!value) return t('nav.neverAsked')
  const elapsed = Date.now() - new Date(value).getTime()
  if (elapsed < 60_000) return t('nav.askedRecently')
  if (elapsed < 3_600_000) return t('nav.askedMinutes', { count: Math.max(1, Math.floor(elapsed / 60_000)) })
  if (elapsed < 86_400_000) return t('nav.askedHours', { count: Math.floor(elapsed / 3_600_000) })
  const date = new Intl.DateTimeFormat(language, { month: 'short', day: 'numeric' }).format(new Date(value))
  return t('nav.askedDate', { date })
}
