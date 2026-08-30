import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { FormEvent, ReactNode } from 'react'
import { useLayoutEffect, useRef, useState } from 'react'
import { Link, NavLink, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'

import type { Session } from '../api/types'
import { Brand } from '../auth/auth'
import { useAuthenticatedUser } from '../auth/context'
import { deleteSession, loadSessions, renameSession } from '../chat/chatApi'

const NAV_ITEMS = [
  { to: '/chat', icon: 'C', labelKey: 'nav.chat' },
  { to: '/workspace', icon: 'W', labelKey: 'nav.workspace' },
  { to: '/devices', icon: 'D', labelKey: 'nav.devices' },
] as const

export function AppShell(): ReactNode {
  const { i18n, t } = useTranslation()
  const user = useAuthenticatedUser()
  const queryClient = useQueryClient()
  const location = useLocation()
  const navigate = useNavigate()
  const latestPath = useRef(location.pathname)
  const sessions = useQuery({
    queryKey: ['sessions'],
    queryFn: loadSessions,
    staleTime: 15_000,
  })
  const [openMenuId, setOpenMenuId] = useState<string | null>(null)
  const [editingSessionId, setEditingSessionId] = useState<string | null>(null)
  const [titleDraft, setTitleDraft] = useState('')
  const [confirmDeleteId, setConfirmDeleteId] = useState<string | null>(null)
  const [managingSessions, setManagingSessions] = useState(false)
  const [selectedSessionIds, setSelectedSessionIds] = useState<Set<string>>(() => new Set())
  const [confirmBulkDelete, setConfirmBulkDelete] = useState(false)
  const [pendingSessionAction, setPendingSessionAction] = useState<string | null>(null)
  const [sessionActionError, setSessionActionError] = useState<string | null>(null)
  const newSessionLink = useRef<HTMLAnchorElement>(null)
  const manageSessionsButton = useRef<HTMLButtonElement>(null)
  const selectAllButton = useRef<HTMLButtonElement>(null)
  const deleteSelectedButton = useRef<HTMLButtonElement>(null)
  const confirmBulkDeleteButton = useRef<HTMLButtonElement>(null)
  const confirmSingleDeleteButton = useRef<HTMLButtonElement>(null)
  const sessionMenuButtons = useRef(new Map<string, HTMLButtonElement>())
  const focusTarget = useRef<
    | { kind: 'new-session' | 'manage' | 'select-all' | 'bulk-delete' | 'bulk-confirm' | 'single-confirm' }
    | { kind: 'session-menu'; sessionId: string }
    | null
  >(null)
  const visibleSessionIds = new Set(sessions.data?.map((session) => session.id) ?? [])
  const selectedVisibleSessionIds = new Set([...selectedSessionIds].filter((id) => visibleSessionIds.has(id)))
  const initials = user.name.trim().slice(0, 2).toUpperCase() || user.email.slice(0, 2).toUpperCase()

  useLayoutEffect(() => {
    latestPath.current = location.pathname
  }, [location.pathname])

  useLayoutEffect(() => {
    const target = focusTarget.current
    if (!target) return
    let element: HTMLElement | undefined | null
    if (target.kind === 'new-session') element = newSessionLink.current
    else if (target.kind === 'manage') element = manageSessionsButton.current
    else if (target.kind === 'select-all') element = selectAllButton.current
    else if (target.kind === 'bulk-delete') element = deleteSelectedButton.current
    else if (target.kind === 'bulk-confirm') element = confirmBulkDeleteButton.current
    else if (target.kind === 'single-confirm') element = confirmSingleDeleteButton.current
    else if ('sessionId' in target) element = sessionMenuButtons.current.get(target.sessionId)
    element?.focus()
    focusTarget.current = null
  }, [confirmBulkDelete, confirmDeleteId, editingSessionId, managingSessions])

  const updateCachedSession = (updated: Session): void => {
    queryClient.setQueryData<Session[]>(['sessions'], (current) => (
      current?.map((session) => session.id === updated.id ? updated : session)
    ))
  }

  const removeCachedSession = (sessionId: string): void => {
    queryClient.setQueryData<Session[]>(['sessions'], (current) => (
      current?.filter((session) => session.id !== sessionId)
    ))
  }

  const startRename = (session: Session): void => {
    setOpenMenuId(null)
    setConfirmDeleteId(null)
    setEditingSessionId(session.id)
    setTitleDraft(session.title)
    setSessionActionError(null)
  }

  const submitRename = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault()
    const targetSessionId = editingSessionId
    const nextTitle = titleDraft.trim()
    if (!targetSessionId || !nextTitle || pendingSessionAction) return
    setPendingSessionAction(targetSessionId)
    setSessionActionError(null)
    try {
      const updated = await renameSession(targetSessionId, nextTitle)
      updateCachedSession(updated)
      focusTarget.current = { kind: 'session-menu', sessionId: targetSessionId }
      setEditingSessionId((current) => current === targetSessionId ? null : current)
      void queryClient.invalidateQueries({ queryKey: ['sessions'] })
    } catch (error) {
      setSessionActionError(errorMessage(error, t('chat.renameFailed')))
    } finally {
      setPendingSessionAction((current) => current === targetSessionId ? null : current)
    }
  }

  const confirmSingleDelete = async (sessionId: string): Promise<void> => {
    if (pendingSessionAction) return
    setPendingSessionAction(sessionId)
    setSessionActionError(null)
    try {
      await deleteSession(sessionId)
      focusTarget.current = { kind: 'new-session' }
      removeCachedSession(sessionId)
      setConfirmDeleteId((current) => current === sessionId ? null : current)
      if (sessionIdFromPath(latestPath.current) === sessionId) navigate('/chat', { replace: true })
      void queryClient.invalidateQueries({ queryKey: ['sessions'] })
    } catch (error) {
      const refreshed = await sessions.refetch()
      if (!refreshed.data?.some((session) => session.id === sessionId)) {
        focusTarget.current = { kind: 'new-session' }
        setConfirmDeleteId((current) => current === sessionId ? null : current)
        setSessionActionError(null)
        if (sessionIdFromPath(latestPath.current) === sessionId) navigate('/chat', { replace: true })
      } else {
        setSessionActionError(errorMessage(error, t('chat.deleteFailed')))
      }
    } finally {
      setPendingSessionAction((current) => current === sessionId ? null : current)
    }
  }

  const toggleSelectedSession = (sessionId: string): void => {
    setSelectedSessionIds((current) => {
      const next = new Set(current)
      if (next.has(sessionId)) next.delete(sessionId)
      else next.add(sessionId)
      return next
    })
    setConfirmBulkDelete(false)
  }

  const leaveSessionManagement = (): void => {
    focusTarget.current = { kind: 'manage' }
    setManagingSessions(false)
    setSelectedSessionIds(new Set())
    setConfirmBulkDelete(false)
  }

  const deleteSelectedSessions = async (): Promise<void> => {
    const selectedIds = [...selectedVisibleSessionIds]
    if (!selectedIds.length || pendingSessionAction) return
    setPendingSessionAction('bulk')
    setSessionActionError(null)
    const requestFailures = new Map<string, string>()
    const confirmedDeleted = new Set<string>()
    try {
      for (const sessionId of selectedIds) {
        try {
          await deleteSession(sessionId)
          confirmedDeleted.add(sessionId)
          removeCachedSession(sessionId)
        } catch (error) {
          requestFailures.set(sessionId, errorMessage(error, t('chat.deleteFailed')))
        }
      }
      const refreshed = await sessions.refetch()
      const remainingIds = new Set((refreshed.data ?? sessions.data ?? []).map((session) => session.id))
      for (const sessionId of confirmedDeleted) remainingIds.delete(sessionId)
      const failed = new Set(selectedIds.filter((sessionId) => (
        !confirmedDeleted.has(sessionId) && remainingIds.has(sessionId)
      )))
      const activeSessionId = sessionIdFromPath(latestPath.current)
      if (activeSessionId && !remainingIds.has(activeSessionId)) navigate('/chat', { replace: true })
      focusTarget.current = { kind: failed.size ? 'bulk-delete' : 'new-session' }
      setConfirmBulkDelete(false)
      setSelectedSessionIds(failed)
      setManagingSessions(failed.size > 0)
      if (failed.size) {
        const firstFailure = requestFailures.get([...failed][0])
        const summary = t('nav.bulkDeleteFailed', { count: failed.size })
        setSessionActionError(firstFailure ? `${summary} ${firstFailure}` : summary)
      }
    } finally {
      setPendingSessionAction(null)
    }
  }

  return (
    <main className="stage">
      <div className="app-shell">
        <aside className="sidebar">
          <Brand />
          <Link ref={newSessionLink} className="new-session" to="/chat"><span aria-hidden="true">＋</span> {t('nav.newChat')}</Link>
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
          <div className="section-label">
            <span>{t('nav.recentChats')}</span>
            {Boolean(sessions.data?.length) && !managingSessions ? (
              <button
                ref={manageSessionsButton}
                type="button"
                onClick={() => {
                  focusTarget.current = { kind: 'select-all' }
                  setManagingSessions(true)
                  setOpenMenuId(null)
                  setEditingSessionId(null)
                  setConfirmDeleteId(null)
                  setSessionActionError(null)
                }}
              >{t('nav.manageChats')}</button>
            ) : null}
          </div>
          {managingSessions ? (
            <div className="session-bulk-toolbar">
              {confirmBulkDelete ? (
                <>
                  <button
                    ref={confirmBulkDeleteButton}
                    type="button"
                    className="session-danger-action"
                    disabled={pendingSessionAction !== null}
                    onClick={() => void deleteSelectedSessions()}
                  >{t('nav.confirmBulkDelete', { count: selectedVisibleSessionIds.size })}</button>
                  <button
                    type="button"
                    disabled={pendingSessionAction !== null}
                    onClick={() => {
                      focusTarget.current = { kind: 'bulk-delete' }
                      setConfirmBulkDelete(false)
                    }}
                  >{t('common.cancel')}</button>
                </>
              ) : (
                <>
                  <button
                    ref={selectAllButton}
                    type="button"
                    disabled={pendingSessionAction !== null}
                    onClick={() => setSelectedSessionIds(selectedVisibleSessionIds.size === sessions.data?.length
                      ? new Set()
                      : new Set(sessions.data?.map((session) => session.id) ?? []))}
                  >{selectedVisibleSessionIds.size === sessions.data?.length ? t('nav.clearSelection') : t('nav.selectAll')}</button>
                  <button
                    ref={deleteSelectedButton}
                    type="button"
                    className="session-danger-action"
                    disabled={!selectedVisibleSessionIds.size || pendingSessionAction !== null}
                    onClick={() => {
                      focusTarget.current = { kind: 'bulk-confirm' }
                      setConfirmBulkDelete(true)
                    }}
                  >{t('nav.deleteSelected', { count: selectedVisibleSessionIds.size })}</button>
                  <button type="button" disabled={pendingSessionAction !== null} onClick={leaveSessionManagement}>{t('common.cancel')}</button>
                </>
              )}
            </div>
          ) : null}
          {sessionActionError ? <p className="session-action-error" role="alert">{sessionActionError}</p> : null}
          <div className="session-list">
            {sessions.data?.length ? (
              sessions.data.map((session) => managingSessions ? (
                <label className="session-select-row" key={session.id}>
                  <input
                    type="checkbox"
                    aria-label={t('nav.selectChat', { title: session.title })}
                    checked={selectedSessionIds.has(session.id)}
                    disabled={pendingSessionAction !== null}
                    onChange={() => toggleSelectedSession(session.id)}
                  />
                  <SessionCopy session={session} language={i18n.language} t={t} />
                </label>
              ) : editingSessionId === session.id ? (
                <form className="session-inline-rename" key={session.id} onSubmit={(event) => void submitRename(event)}>
                  <input
                    aria-label={t('chat.sessionTitle')}
                    value={titleDraft}
                    maxLength={120}
                    disabled={pendingSessionAction !== null}
                    autoFocus
                    onChange={(event) => setTitleDraft(event.target.value)}
                  />
                  <div>
                    <button type="submit" disabled={!titleDraft.trim() || pendingSessionAction !== null}>{t('common.save')}</button>
                    <button
                      type="button"
                      disabled={pendingSessionAction !== null}
                      onClick={() => {
                        focusTarget.current = { kind: 'session-menu', sessionId: session.id }
                        setEditingSessionId(null)
                      }}
                    >{t('common.cancel')}</button>
                  </div>
                </form>
              ) : confirmDeleteId === session.id ? (
                <div className="session-delete-confirm" key={session.id}>
                  <strong>{session.title}</strong>
                  <div>
                    <button
                      ref={confirmSingleDeleteButton}
                      type="button"
                      className="session-danger-action"
                      disabled={pendingSessionAction !== null}
                      onClick={() => void confirmSingleDelete(session.id)}
                    >{t('chat.confirmDelete')}</button>
                    <button
                      type="button"
                      disabled={pendingSessionAction !== null}
                      onClick={() => {
                        focusTarget.current = { kind: 'session-menu', sessionId: session.id }
                        setConfirmDeleteId(null)
                      }}
                    >{t('common.cancel')}</button>
                  </div>
                </div>
              ) : (
                <div className="session-row" key={session.id}>
                  <Link className="session-item" to={`/chat/${session.id}`} onClick={() => setOpenMenuId(null)}>
                    <SessionCopy session={session} language={i18n.language} t={t} />
                    {session.unread ? <span className="session-dot" aria-label={t('nav.unread')} /> : null}
                  </Link>
                  <button
                    ref={(element) => {
                      if (element) sessionMenuButtons.current.set(session.id, element)
                      else sessionMenuButtons.current.delete(session.id)
                    }}
                    type="button"
                    className="session-menu-button"
                    aria-label={t('nav.sessionActions', { title: session.title })}
                    aria-expanded={openMenuId === session.id}
                    onClick={() => setOpenMenuId((current) => current === session.id ? null : session.id)}
                  >•••</button>
                  {openMenuId === session.id ? (
                    <div className="session-action-menu">
                      <button type="button" onClick={() => startRename(session)}>{t('chat.rename')}</button>
                      <button
                        type="button"
                        className="session-danger-action"
                        onClick={() => {
                          focusTarget.current = { kind: 'single-confirm' }
                          setOpenMenuId(null)
                          setEditingSessionId(null)
                          setConfirmDeleteId(session.id)
                          setSessionActionError(null)
                        }}
                      >{t('common.delete')}</button>
                    </div>
                  ) : null}
                </div>
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

function SessionCopy({
  session,
  language,
  t,
}: {
  session: Session
  language: string
  t: (key: string, options?: Record<string, unknown>) => string
}): ReactNode {
  return (
    <span className="session-copy">
      <strong className="session-title">{session.title}</strong>
      <small className="session-meta">{formatSessionTime(session.last_inbound_at, language, t)}</small>
    </span>
  )
}

function sessionIdFromPath(pathname: string): string | null {
  const match = /^\/chat\/([^/]+)$/.exec(pathname)
  return match ? decodeURIComponent(match[1]) : null
}

function errorMessage(error: unknown, fallback: string): string {
  return error instanceof Error && error.message ? error.message : fallback
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
