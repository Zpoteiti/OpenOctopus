import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { FormEvent, ReactNode } from 'react'
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate, useParams } from 'react-router-dom'
import remarkGfm from 'remark-gfm'

import { ApiError, apiJson } from '../api/client'
import type { Device, Effort, Session } from '../api/types'
import {
  MessageStreamError,
  cancelSession,
  deleteSession,
  loadMessageHistory,
  loadSessions,
  renameSession,
  sendChatMessage,
  type StreamEvent,
} from './chatApi'
import {
  emptyHistory,
  mergeHistory,
  upsertMessage,
  type ChatMessage,
  type ContentBlock,
  type MessageHistory,
} from './model'
import './ChatPage.css'

const MAX_RECOVERY_POLLS = 300
const HISTORY_PAGE_LIMIT = 200

export interface ChatPageProps {
  pollIntervalMs?: number
  idFactory?: () => string
}

interface HistoryState {
  sessionId: string
  history: MessageHistory
  error: string | null
}

interface NoticeState {
  sessionId: string | null
  message: string
}

export function ChatPage({
  pollIntervalMs = 1_000,
  idFactory = randomUuid,
}: ChatPageProps): ReactNode {
  const { t } = useTranslation()
  const { sessionId } = useParams<{ sessionId: string }>()
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const sessions = useQuery({
    queryKey: ['sessions'],
    queryFn: loadSessions,
    staleTime: 15_000,
  })
  const devices = useQuery({
    queryKey: ['devices'],
    queryFn: () => apiJson<Device[]>('/api/devices'),
    staleTime: 15_000,
  })
  const session = sessions.data?.find((candidate) => candidate.id === sessionId)
  const [locallyCreatedSessionId, setLocallyCreatedSessionId] = useState<string | null>(null)
  const [historyVersion, setHistoryVersion] = useState(0)
  const { history, historyError, updateHistory } = useRecoveredHistory(
    sessionId,
    pollIntervalMs,
    historyVersion,
  )
  const [text, setText] = useState('')
  const [files, setFiles] = useState<File[]>([])
  const [effort, setEffort] = useState<Effort>('off')
  const [sending, setSending] = useState(false)
  const [notice, setNotice] = useState<NoticeState | null>(null)
  const [liveText, setLiveText] = useState('')
  const [liveThinking, setLiveThinking] = useState('')
  const [toolProgress, setToolProgress] = useState<string | null>(null)
  const [streamSessionId, setStreamSessionId] = useState<string | null>(null)
  const [visibleLatestKey, setVisibleLatestKey] = useState<string | null>(null)
  const [renaming, setRenaming] = useState(false)
  const [titleDraft, setTitleDraft] = useState('')
  const [draftSessionId, setDraftSessionId] = useState(sessionId)
  const [deletingSessionId, setDeletingSessionId] = useState<string | null>(null)
  const fileInput = useRef<HTMLInputElement>(null)
  const chatScroll = useRef<HTMLDivElement>(null)
  const latestMessageMarker = useRef<HTMLSpanElement>(null)
  const lastReadRequest = useRef<string | null>(null)
  const streamGeneration = useRef(0)
  const activeViewSession = useRef<string | null>(sessionId ?? locallyCreatedSessionId)

  const isLocalWebSession = sessionId !== undefined && sessionId === locallyCreatedSessionId
  const writable = sessionId === undefined || session?.channel === 'web' || isLocalWebSession
  const title = session?.title && session.title !== 'New chat' ? session.title : t('nav.newChat')
  const viewSessionId = sessionId ?? locallyCreatedSessionId
  const renderedLastMessageId = history?.messages.at(-1)?.id ?? null
  const showingStream = streamSessionId !== null && streamSessionId === viewSessionId
  const sendingHere = sending && showingStream
  const visibleNotice = notice?.sessionId === viewSessionId ? notice.message : null
  const initiallyScrolledSession = useRef<string | null>(null)
  const followLatestSession = useRef<string | null>(null)

  if (draftSessionId !== sessionId) {
    setDraftSessionId(sessionId)
    setText('')
    setFiles([])
    setRenaming(false)
    setTitleDraft('')
  }

  useLayoutEffect(() => {
    activeViewSession.current = viewSessionId
  }, [viewSessionId])

  useLayoutEffect(() => {
    const root = chatScroll.current
    if (!root || !history || !viewSessionId) return
    const isInitialScroll = initiallyScrolledSession.current !== viewSessionId
    const isFollowingSend = followLatestSession.current === viewSessionId
    if (!isInitialScroll && !isFollowingSend) return

    root.scrollTop = root.scrollHeight
    if (isInitialScroll) {
      initiallyScrolledSession.current = viewSessionId
    }
    if (isFollowingSend && !sendingHere && history.status !== 'running') {
      followLatestSession.current = null
    }
  }, [history, liveText, liveThinking, sendingHere, toolProgress, viewSessionId])

  const refreshSessions = useCallback(async () => {
    await queryClient.invalidateQueries({ queryKey: ['sessions'] })
  }, [queryClient])

  useEffect(() => {
    const recoverWhenVisible = (): void => {
      if (document.visibilityState === 'visible' && sessionId) {
        setHistoryVersion((current) => current + 1)
      }
    }
    document.addEventListener('visibilitychange', recoverWhenVisible)
    return () => document.removeEventListener('visibilitychange', recoverWhenVisible)
  }, [sessionId])

  useEffect(() => {
    const marker = latestMessageMarker.current
    const root = chatScroll.current
    const messageId = renderedLastMessageId
    if (!marker || !root || !sessionId || !messageId || typeof IntersectionObserver === 'undefined') return
    const requestKey = `${sessionId}:${messageId}`
    const observer = new IntersectionObserver((entries) => {
      if (entries.some((entry) => entry.isIntersecting)) setVisibleLatestKey(requestKey)
    }, { root })
    observer.observe(marker)
    return () => observer.disconnect()
  }, [renderedLastMessageId, sessionId])

  useEffect(() => {
    const messageId = renderedLastMessageId
    if (!sessionId || !session?.unread || !messageId || document.visibilityState === 'hidden') return
    const requestKey = `${sessionId}:${messageId}`
    if (visibleLatestKey !== requestKey) return
    if (lastReadRequest.current === requestKey) return
    lastReadRequest.current = requestKey
    void apiJson<Session>(`/api/sessions/${encodeURIComponent(sessionId)}`, {
      method: 'PATCH',
      body: JSON.stringify({ read_through_message_id: messageId }),
    }).then(refreshSessions).catch(() => {
      if (lastReadRequest.current === requestKey) lastReadRequest.current = null
    })
  }, [historyVersion, refreshSessions, renderedLastMessageId, session?.unread, sessionId, visibleLatestKey])

  const processEvent = useCallback((
    event: StreamEvent,
    targetSessionId: string,
    sentText: string,
    sentEffort: Effort,
  ) => {
    if (event.type === 'message_accepted') {
      const pendingContent: ContentBlock[] = sentText.trim()
        ? [{ type: 'text', text: sentText }]
        : []
      updateHistory(targetSessionId, (current) => ({
        ...current,
        status: 'running',
        pending_messages: [
          ...current.pending_messages.filter((message) => message.id !== event.message_id),
          {
            id: event.message_id,
            session_id: targetSessionId,
            content: pendingContent,
            effort: sentEffort,
            received_at: new Date().toISOString(),
          },
        ],
        pending_count: current.pending_messages.some((message) => message.id === event.message_id)
          ? current.pending_count
          : current.pending_count + 1,
      }))
      return
    }

    if (event.type === 'turn_started') {
      setToolProgress(null)
      updateHistory(targetSessionId, (current) => ({ ...current, status: 'running', active_turn_id: event.turn_id }))
      return
    }

    if (event.type === 'token_delta') {
      if (event.channel === 'text') setLiveText((current) => current + event.text)
      else setLiveThinking((current) => current + event.text)
      return
    }

    if (event.type === 'tool_progress') {
      const progress = event.kind === 'tool_started'
        ? t('chat.progressStarted', { defaultValue: 'running' })
        : event.kind === 'tool_finished'
          ? t('chat.progressFinished', { defaultValue: 'completed' })
          : event.kind
      setToolProgress(t('chat.toolRunning', {
        tool: event.tool_name,
        progress: ` · ${progress}`,
        defaultValue: 'Running: {{tool}}{{progress}}',
      }))
      return
    }

    if (event.type === 'message_persisted') {
      updateHistory(targetSessionId, (current) => ({
        ...current,
        messages: upsertMessage(current.messages, event.message),
        pending_messages: current.pending_messages.filter((message) => message.id !== event.message.id),
        pending_count: Math.max(0, current.pending_count - (
          current.pending_messages.some((message) => message.id === event.message.id) ? 1 : 0
        )),
        last_message_id: event.message.id,
      }))
      if (event.message.role === 'assistant') {
        setLiveText('')
        setLiveThinking('')
      }
      return
    }

    if (event.type === 'turn_finished') {
      setToolProgress(null)
      return
    }

    if (event.type === 'stream_replaced') {
      setNotice({
        sessionId: targetSessionId,
        message: t('chat.streamReplaced', {
          defaultValue: 'The message is queued. Live preview moved to a newer message; this conversation will recover from saved history.',
        }),
      })
      return
    }

    if (event.type === 'session_deleted') navigate('/chat', { replace: true })
  }, [navigate, t, updateHistory])

  async function handleSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    if (sending || (!text.trim() && files.length === 0)) return

    const sentText = text
    const sentFiles = files
    const sentEffort = effort
    const targetSessionId = sessionId ?? idFactory()
    const isNewSession = sessionId === undefined
    let shouldRecover = false
    const generation = streamGeneration.current + 1
    streamGeneration.current = generation
    activeViewSession.current = targetSessionId
    followLatestSession.current = targetSessionId

    setSending(true)
    setText('')
    setStreamSessionId(targetSessionId)
    setNotice(null)
    setLiveText('')
    setLiveThinking('')
    setToolProgress(null)
    if (isNewSession) setLocallyCreatedSessionId(targetSessionId)

    try {
      await sendChatMessage({
        sessionId: targetSessionId,
        text: sentText,
        files: sentFiles,
        effort: sentEffort,
        idFactory,
        onEvent: (streamEvent) => {
          if (streamGeneration.current !== generation || activeViewSession.current !== targetSessionId) return
          if (streamEvent.type === 'message_accepted') {
            shouldRecover = true
            if (isNewSession) navigate(`/chat/${targetSessionId}`, { replace: true })
          }
          processEvent(streamEvent, targetSessionId, sentText, sentEffort)
        },
      })
      if (streamGeneration.current === generation && activeViewSession.current === targetSessionId) {
        setFiles([])
      }
    } catch (error) {
      if (streamGeneration.current !== generation || activeViewSession.current !== targetSessionId) return
      if (error instanceof MessageStreamError) {
        shouldRecover = true
        setNotice({ sessionId: targetSessionId, message: error.message })
        if (error.accepted) {
          setText('')
          setFiles([])
        } else {
          setText(sentText)
          setFiles(sentFiles)
        }
        if (isNewSession) navigate(`/chat/${targetSessionId}`, { replace: true })
      } else {
        setText(sentText)
        setFiles(sentFiles)
        setNotice({
          sessionId: targetSessionId,
          message: chatErrorMessage(error, t('chat.sendFailed', {
            defaultValue: 'The message could not be sent. Try again.',
          })),
        })
      }
    } finally {
      if (streamGeneration.current === generation) {
        setSending(false)
        setStreamSessionId(null)
        if (shouldRecover && activeViewSession.current === targetSessionId) {
          setHistoryVersion((current) => current + 1)
        }
      }
      await refreshSessions()
    }
  }

  async function handleRename(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const targetSessionId = sessionId
    const nextTitle = titleDraft.trim()
    if (!targetSessionId || !nextTitle) return
    try {
      await renameSession(targetSessionId, nextTitle)
      await refreshSessions()
      if (activeViewSession.current !== targetSessionId) return
      setRenaming(false)
      setNotice(null)
    } catch (error) {
      if (activeViewSession.current !== targetSessionId) return
      setNotice({
        sessionId: targetSessionId,
        message: chatErrorMessage(error, t('chat.renameFailed', {
          defaultValue: 'The conversation could not be renamed.',
        })),
      })
    }
  }

  async function handleCancel(): Promise<void> {
    if (!sessionId) return
    try {
      const result = await cancelSession(sessionId)
      setNotice({
        sessionId,
        message: result.cancel_requested
          ? t('chat.cancelRequested', { defaultValue: 'A stop was requested at the next supported stop point.' })
          : t('chat.nothingRunning', { defaultValue: 'No task is currently running.' }),
      })
      setHistoryVersion((current) => current + 1)
      await refreshSessions()
    } catch (error) {
      setNotice({
        sessionId,
        message: chatErrorMessage(error, t('chat.cancelFailed', {
          defaultValue: 'The stop request failed.',
        })),
      })
    }
  }

  async function handleDelete(targetSessionId: string): Promise<void> {
    if (deletingSessionId) return
    setDeletingSessionId(targetSessionId)
    try {
      await deleteSession(targetSessionId)
      await refreshSessions()
      if (activeViewSession.current === targetSessionId) navigate('/chat', { replace: true })
    } catch (error) {
      setNotice({
        sessionId: targetSessionId,
        message: chatErrorMessage(error, t('chat.deleteFailed', {
          defaultValue: 'The conversation could not be deleted.',
        })),
      })
    } finally {
      setDeletingSessionId((current) => current === targetSessionId ? null : current)
    }
  }

  const readOnlyMessage = session && session.channel !== 'web'
    ? t('chat.readOnly', {
        channel: session.channel,
        defaultValue: 'This {{channel}} conversation is read-only in the browser.',
      })
    : sessionId && sessions.isSuccess && !session && !isLocalWebSession
      ? t('chat.notFound', {
          defaultValue: 'This conversation was not found or is not available to this account.',
        })
      : null

  return (
    <>
      <header className="workspace-header chat-workspace-header">
        <div className="breadcrumbs"><span>{t('nav.chat')}</span><span aria-hidden="true">/</span><strong>{title}</strong></div>
        <div className="chat-header-actions">
          <DeviceMenu devices={devices.data ?? []} />
          {sessionId ? (
            <div className="chat-session-controls">
            <button
              type="button"
              className="chat-secondary-button"
              onClick={() => setHistoryVersion((current) => current + 1)}
            >{t('common.refresh')}</button>
            {history?.status === 'running' || session?.cancel_requested ? (
              <button type="button" className="chat-secondary-button" onClick={() => void handleCancel()}>
                {t('chat.stop', { defaultValue: 'Stop' })}
              </button>
            ) : null}
            <button
              type="button"
              className="chat-secondary-button"
              onClick={() => {
                setTitleDraft(title)
                setRenaming(true)
              }}
            >{t('chat.rename', { defaultValue: 'Rename' })}</button>
            <DeleteSessionButton
              key={sessionId}
              sessionId={sessionId}
              disabled={deletingSessionId !== null}
              onDelete={handleDelete}
            />
            </div>
          ) : null}
        </div>
      </header>

      <div className="chat-page">
        <div ref={chatScroll} className="chat-scroll" aria-live="polite">
          <div className="chat-content">
            {sessionId ? <h1 className="sr-only">{t('nav.chat')}: {title}</h1> : null}
            {renaming ? (
              <form className="chat-rename" onSubmit={(event) => void handleRename(event)}>
                <label htmlFor="chat-title">{t('chat.sessionTitle', { defaultValue: 'Conversation title' })}</label>
                <input id="chat-title" value={titleDraft} onChange={(event) => setTitleDraft(event.target.value)} maxLength={120} autoFocus />
                <button type="submit" className="primary-button">{t('common.save')}</button>
                <button type="button" className="chat-secondary-button" onClick={() => setRenaming(false)}>{t('common.cancel')}</button>
              </form>
            ) : null}
            {readOnlyMessage ? <p className="chat-banner">{readOnlyMessage}</p> : null}
            {historyError ? <p className="chat-banner chat-banner-error" role="alert">{historyError}</p> : null}
            {visibleNotice ? <p className="chat-banner chat-banner-error" role="alert">{visibleNotice}</p> : null}

            {!sessionId && !history?.messages.length ? (
              <div className="chat-empty">
                <span className="eyebrow">{t('draftChat.eyebrow')}</span>
                <h1>{t('draftChat.heading')}</h1>
                <p>{t('draftChat.description')}</p>
              </div>
            ) : null}

            {history?.has_more_before ? (
              <p className="chat-history-note">
                {t('chat.historyLimit', { defaultValue: 'Showing the 200 most recent saved messages.' })}
              </p>
            ) : null}
            {history ? (
              <Transcript
                messages={history.messages}
                running={history.status === 'running'}
                toolProgress={toolProgress}
              />
            ) : null}
            {renderedLastMessageId ? <span ref={latestMessageMarker} className="chat-latest-marker" aria-hidden="true" /> : null}
            {history?.pending_messages.map((message) => (
              <article key={message.id} className="chat-message chat-message-user chat-message-pending">
                <header>
                  <strong>{t('chat.you', { defaultValue: 'You' })}</strong>
                  <span>{t('chat.pending', { defaultValue: 'Pending' })}</span>
                </header>
                <ContentBlocks blocks={message.content} />
              </article>
            ))}
            {showingStream && (liveThinking || liveText) ? (
              <article className="chat-message chat-message-assistant chat-message-live">
                <header><strong>OpenOctopus</strong><span>{t('chat.generating', { defaultValue: 'Generating' })}</span></header>
                {liveThinking ? (
                  <details className="chat-thinking">
                    <summary>{t('chat.thinking', { defaultValue: 'Thinking' })}</summary>
                    <p>{liveThinking}</p>
                  </details>
                ) : null}
                {liveText ? <ReactMarkdown remarkPlugins={[remarkGfm]}>{liveText}</ReactMarkdown> : null}
              </article>
            ) : null}
            {showingStream && toolProgress ? <p className="chat-tool-progress">{toolProgress}</p> : null}
          </div>
        </div>

        {writable ? (
          <form className="composer chat-composer" onSubmit={(event) => void handleSubmit(event)}>
            {files.length ? (
              <ul className="chat-attachments" aria-label={t('chat.pendingAttachments', { defaultValue: 'Attachments to send' })}>
                {files.map((file, index) => (
                  <li key={`${file.name}-${file.lastModified}-${index}`}>
                    <span>{file.name}</span>
                    <button
                      type="button"
                      disabled={sending}
                      onClick={() => setFiles((current) => current.filter((_, itemIndex) => itemIndex !== index))}
                    >
                      {t('chat.remove', { defaultValue: 'Remove' })}
                    </button>
                  </li>
                ))}
              </ul>
            ) : null}
            <textarea
              aria-label={t('draftChat.message')}
              placeholder={t('draftChat.placeholder')}
              rows={2}
              value={text}
              onChange={(event) => setText(event.target.value)}
              onKeyDown={(event) => {
                if (
                  event.key !== 'Enter'
                  || event.shiftKey
                  || event.nativeEvent.isComposing
                  || event.nativeEvent.keyCode === 229
                ) return
                event.preventDefault()
                event.currentTarget.form?.requestSubmit()
              }}
              disabled={sending}
            />
            <div className="composer-actions">
              <div className="composer-tools">
                <input
                  ref={fileInput}
                  className="chat-file-input"
                  type="file"
                  multiple
                  disabled={sending}
                  aria-label={t('chat.selectAttachments', { defaultValue: 'Choose attachments' })}
                  onChange={(event) => {
                    const selectedFiles = Array.from(event.target.files ?? [])
                    setFiles((current) => [...current, ...selectedFiles])
                    event.target.value = ''
                  }}
                />
                <button type="button" className="text-button" disabled={sending} onClick={() => fileInput.current?.click()}>
                  ＋ {t('draftChat.attachment')}
                </button>
                <label className="composer-effort">
                  <span>{t('chat.reasoningEffort')}</span>
                  <select
                    aria-label={t('chat.reasoningEffort')}
                    value={effort}
                    disabled={sending}
                    onChange={(event) => setEffort(event.target.value as Effort)}
                  >
                    <option value="off">{t('chat.effortOff')}</option>
                    <option value="low">{t('chat.effortLow')}</option>
                    <option value="medium">{t('chat.effortMedium')}</option>
                    <option value="high">{t('chat.effortHigh')}</option>
                    <option value="xhigh">{t('chat.effortXHigh')}</option>
                    <option value="max">{t('chat.effortMax')}</option>
                  </select>
                </label>
              </div>
              <button
                className="send-button"
                aria-label={t('draftChat.send')}
                disabled={sending || (!text.trim() && files.length === 0)}
              >{sending ? '…' : '↑'}</button>
            </div>
          </form>
        ) : null}
      </div>
    </>
  )
}

function useRecoveredHistory(
  sessionId: string | undefined,
  pollIntervalMs: number,
  version: number,
): {
  history: MessageHistory | null
  historyError: string | null
  updateHistory: (targetSessionId: string, updater: (history: MessageHistory) => MessageHistory) => void
} {
  const { t } = useTranslation()
  const [state, setState] = useState<HistoryState | null>(null)
  const recoveryCursor = useRef<{ sessionId: string; messageId: string | null } | null>(null)

  useEffect(() => {
    if (!sessionId) return
    let disposed = false
    let timer: ReturnType<typeof setTimeout> | undefined

    const load = async (after: string | null, pollCount: number): Promise<void> => {
      try {
        const incoming = await loadMessageHistory(sessionId, after)
        if (disposed) return
        setState((current) => ({
          sessionId,
          history: current?.sessionId === sessionId ? mergeHistory(current.history, incoming) : incoming,
          error: null,
        }))
        const pageCursor = incoming.messages.at(-1)?.id ?? after
        recoveryCursor.current = { sessionId, messageId: pageCursor }
        const caughtUp = incoming.last_message_id === null || pageCursor === incoming.last_message_id
        const hasRecoveryWork = incoming.status === 'running' || incoming.pending_count > 0 || !caughtUp
        if (hasRecoveryWork && pollCount < MAX_RECOVERY_POLLS) {
          timer = setTimeout(() => {
            void load(pageCursor, pollCount + 1)
          }, incoming.messages.length === HISTORY_PAGE_LIMIT && !caughtUp ? 0 : pollIntervalMs)
        } else if (hasRecoveryWork) {
          setState((current) => current?.sessionId === sessionId
            ? {
                ...current,
                error: t('chat.recoveryPaused', {
                  defaultValue: 'Live recovery polling paused. Refresh the page to continue checking the task.',
                }),
              }
            : current)
        }
      } catch (error) {
        if (disposed) return
        setState((current) => ({
          sessionId,
          history: current?.sessionId === sessionId ? current.history : emptyHistory(),
          error: chatErrorMessage(error, t('chat.historyLoadFailed', {
            defaultValue: 'The conversation history could not be loaded.',
          })),
        }))
      }
    }

    const cursor = recoveryCursor.current
    const resumeAfter = cursor?.sessionId === sessionId
      ? cursor.messageId
      : null
    void load(resumeAfter, 0)
    return () => {
      disposed = true
      if (timer) clearTimeout(timer)
    }
  }, [pollIntervalMs, sessionId, t, version])

  const updateHistory = useCallback((targetSessionId: string, updater: (history: MessageHistory) => MessageHistory) => {
    setState((current) => ({
      sessionId: targetSessionId,
      history: updater(current?.sessionId === targetSessionId ? current.history : emptyHistory()),
      error: current?.sessionId === targetSessionId ? current.error : null,
    }))
  }, [])

  return {
    history: state && state.sessionId === sessionId ? state.history : null,
    historyError: state && state.sessionId === sessionId ? state.error : null,
    updateHistory,
  }
}

function DeviceMenu({ devices }: { devices: Device[] }): ReactNode {
  const { t } = useTranslation()
  const onlineCount = devices.filter((device) => device.online).length
  return (
    <details className="chat-device-menu">
      <summary>{t('chat.devicesOnline', {
        count: onlineCount,
        defaultValue: '{{count}} devices online',
      })}</summary>
      <div className="chat-device-popover">
        {devices.length ? devices.map((device) => (
          <Link key={device.id} to={`/devices/${encodeURIComponent(device.name)}`}>
            <span>{device.name}</span>
            <small>{device.online ? t('common.online') : t('common.offline')}</small>
          </Link>
        )) : <p>{t('chat.noDevices', { defaultValue: 'No devices' })}</p>}
      </div>
    </details>
  )
}

function DeleteSessionButton({
  sessionId,
  disabled,
  onDelete,
}: {
  sessionId: string
  disabled: boolean
  onDelete: (sessionId: string) => Promise<void>
}): ReactNode {
  const { t } = useTranslation()
  const [confirm, setConfirm] = useState(false)
  return (
    <button
      type="button"
      className="chat-danger-button"
      disabled={disabled}
      onClick={() => {
        if (!confirm) {
          setConfirm(true)
          return
        }
        setConfirm(false)
        void onDelete(sessionId)
      }}
    >
      {confirm
        ? t('chat.confirmDelete', { defaultValue: 'Confirm deletion' })
        : t('common.delete')}
    </button>
  )
}

function MessageRow({ message }: { message: ChatMessage }): ReactNode {
  const { i18n, t } = useTranslation()
  const isHuman = message.message_kind === 'human'
  const isToolResult = message.message_kind === 'tool_result' || message.message_kind === 'synthetic_tool_result'
  const label = isHuman
    ? t('chat.you', { defaultValue: 'You' })
    : isToolResult
      ? t('chat.toolResult', { defaultValue: 'Tool result' })
      : 'OpenOctopus'
  return (
    <article className={`chat-message chat-message-${isHuman ? 'user' : 'assistant'}${message.is_compacted ? ' chat-message-compacted' : ''}`}>
      <header>
        <strong>{label}</strong>
        <span>{message.message_kind === 'compaction_summary'
          ? t('chat.compactionSummary', { defaultValue: 'Context summary' })
          : formatTime(message.created_at, i18n.resolvedLanguage)}</span>
      </header>
      <ContentBlocks blocks={message.content} />
      {message.delivery_refs.length ? (
        <ul className="chat-deliveries">
          {message.delivery_refs.map((delivery, index) => (
            <li key={`${String(delivery.type ?? 'file')}-${index}`}>
              {t('chat.generatedFile', {
                filename: String(delivery.filename ?? delivery.path ?? t('chat.file', { defaultValue: 'file' })),
                defaultValue: 'Generated file: {{filename}}',
              })}
            </li>
          ))}
        </ul>
      ) : null}
    </article>
  )
}

function Transcript({
  messages,
  running,
  toolProgress,
}: {
  messages: ChatMessage[]
  running: boolean
  toolProgress: string | null
}): ReactNode {
  const { t } = useTranslation()
  const groups: ChatMessage[][] = []

  for (const message of messages) {
    if (message.message_kind === 'compaction_summary') {
      groups.push([message])
      continue
    }
    if (message.message_kind === 'human' || groups.length === 0) {
      groups.push([message])
      continue
    }
    const current = groups.at(-1)
    if (!current || current[0]?.message_kind === 'compaction_summary') {
      groups.push([message])
    } else {
      current.push(message)
    }
  }

  let activeGroupIndex = -1
  for (let index = groups.length - 1; index >= 0; index -= 1) {
    if (groups[index]?.[0]?.message_kind !== 'compaction_summary') {
      activeGroupIndex = index
      break
    }
  }

  return groups.map((group, groupIndex) => {
    if (group.length === 1 && group[0]?.message_kind === 'compaction_summary') {
      return <MessageRow key={group[0].id} message={group[0]} />
    }

    const human = group[0]?.message_kind === 'human' ? group[0] : null
    const responses = human ? group.slice(1) : group
    let finalIndex = -1
    for (let index = responses.length - 1; index >= 0; index -= 1) {
      if (isFinalReply(responses[index])) {
        finalIndex = index
        break
      }
    }
    const finalReply = finalIndex >= 0 ? responses[finalIndex] : null
    const process = responses.filter((_, index) => index !== finalIndex)
    const active = running && groupIndex === activeGroupIndex
    const latestTool = findLatestTool(process)
    const summary = active
      ? toolProgress ?? (latestTool
          ? t('chat.currentWork', { tool: latestTool, defaultValue: 'Working · {{tool}}' })
          : t('chat.workInProgress', { defaultValue: 'Working…' }))
      : t('chat.workDetails', {
          count: process.length,
          defaultValue: 'Work details · {{count}} steps',
        })

    return (
      <div className="chat-turn" key={human?.id ?? group[0]?.id ?? groupIndex}>
        {human ? <MessageRow message={human} /> : null}
        {process.length ? (
          <details className={`chat-work-log${active ? ' chat-work-log-active' : ''}`}>
            <summary><span aria-hidden="true" className="chat-work-status" />{summary}</summary>
            <div className="chat-work-log-messages">
              {process.map((message) => <MessageRow key={message.id} message={message} />)}
            </div>
          </details>
        ) : null}
        {finalReply ? <MessageRow message={finalReply} /> : null}
      </div>
    )
  })
}

function isFinalReply(message: ChatMessage): boolean {
  if (!['assistant', 'synthetic_assistant_error'].includes(message.message_kind)) return false
  if (message.content.some((block) => block.type === 'tool_use')) return false
  return message.delivery_refs.length > 0 || message.content.some((block) => (
    block.type === 'text' && typeof block.text === 'string' && block.text.trim().length > 0
  ))
}

function findLatestTool(messages: ChatMessage[]): string | null {
  for (let messageIndex = messages.length - 1; messageIndex >= 0; messageIndex -= 1) {
    const message = messages[messageIndex]
    for (let blockIndex = message.content.length - 1; blockIndex >= 0; blockIndex -= 1) {
      const block = message.content[blockIndex]
      if (block.type === 'tool_use' && typeof block.name === 'string') return block.name
    }
  }
  return null
}

function ContentBlocks({ blocks }: { blocks: ContentBlock[] }): ReactNode {
  const { t } = useTranslation()
  return blocks.map((block, index) => {
    if (block.type === 'text' && typeof block.text === 'string') {
      return <ReactMarkdown key={index} remarkPlugins={[remarkGfm]}>{block.text}</ReactMarkdown>
    }
    if (block.type === 'thinking' && typeof block.thinking === 'string') {
      return (
        <details key={index} className="chat-thinking">
          <summary>{t('chat.thinking', { defaultValue: 'Thinking' })}</summary>
          <p>{block.thinking}</p>
        </details>
      )
    }
    if (block.type === 'tool_use') {
      return (
        <details key={index} className="chat-tool-block">
          <summary>{t('chat.callTool', {
            tool: String(block.name ?? t('chat.unknownTool', { defaultValue: 'unknown tool' })),
            defaultValue: 'Tool call: {{tool}}',
          })}</summary>
          <pre>{formatUnknown(block.input)}</pre>
        </details>
      )
    }
    if (block.type === 'tool_result') {
      return (
        <details key={index} className="chat-tool-block">
          <summary>{block.is_error
            ? t('chat.toolFailed', { defaultValue: 'Tool failed' })
            : t('chat.toolResult', { defaultValue: 'Tool result' })}</summary>
          <pre>{formatUnknown(block.content)}</pre>
        </details>
      )
    }
    if (block.type === 'image') return <p key={index} className="chat-muted">{t('chat.image', { defaultValue: '[Image]' })}</p>
    return null
  })
}

function formatUnknown(value: unknown): string {
  if (typeof value === 'string') return value
  return JSON.stringify(value, null, 2)
}

function formatTime(value: string, language: string | undefined): string {
  return new Intl.DateTimeFormat(language === 'zh-CN' ? 'zh-CN' : 'en', {
    hour: '2-digit',
    minute: '2-digit',
  }).format(new Date(value))
}

function randomUuid(): string {
  return crypto.randomUUID()
}

function chatErrorMessage(error: unknown, fallback: string): string {
  if (error instanceof ApiError) {
    const codeSuffix = `(${error.code})`
    return error.message.includes(codeSuffix) ? error.message : `${error.message} ${codeSuffix}`
  }
  return error instanceof Error ? error.message : fallback
}
