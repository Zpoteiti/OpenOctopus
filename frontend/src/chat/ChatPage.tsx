import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ClipboardEvent, FormEvent, ReactNode } from 'react'
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import ReactMarkdown from 'react-markdown'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'
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
  uploadBrowserAttachment,
  validateBrowserAttachmentFiles,
  type MessageAttachmentRef,
  type StreamEvent,
} from './chatApi'
import { AttachmentPicker, type AttachmentPickerSource } from './AttachmentPicker'
import {
  emptyHistory,
  mergeHistory,
  upsertMessage,
  type ChatMessage,
  type ChannelContext,
  type ChannelDelivery,
  type ContentBlock,
  type MessageHistory,
  type MessageSender,
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

interface DraftAttachment {
  id: string
  name: string
  source: string
  status: 'uploading' | 'ready' | 'failed'
  file?: File
  ref?: MessageAttachmentRef
}

export function ChatPage({
  pollIntervalMs = 1_000,
  idFactory = randomUuid,
}: ChatPageProps): ReactNode {
  const { t } = useTranslation()
  const { sessionId } = useParams<{ sessionId: string }>()
  const [searchParams] = useSearchParams()
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
  const requestedAutomation = automationChannel(searchParams.get('automation'))
  const source = automationChannel(session?.channel) ?? (!session ? requestedAutomation : null)
  const externalSource = externalChannel(session?.channel)
  const [locallyCreatedSessionId, setLocallyCreatedSessionId] = useState<string | null>(null)
  const [historyReload, setHistoryReload] = useState({ version: 0, fromStart: true })
  const requestHistoryReload = useCallback((fromStart: boolean) => {
    setHistoryReload((current) => ({ version: current.version + 1, fromStart }))
  }, [])
  const { history, historyError, updateHistory } = useRecoveredHistory(
    sessionId,
    pollIntervalMs,
    historyReload.version,
    historyReload.fromStart,
  )
  const [text, setText] = useState('')
  const [attachments, setAttachments] = useState<DraftAttachment[]>([])
  const attachmentsRef = useRef<DraftAttachment[]>(attachments)
  const attachmentTasks = useRef(new Map<string, Promise<void>>())
  const draftGeneration = useRef(0)
  const draftTransfer = useRef<{
    sessionId: string
    text: string
    attachments: DraftAttachment[]
  } | null>(null)
  const [attachmentMenuOpen, setAttachmentMenuOpen] = useState(false)
  const [pickerSource, setPickerSource] = useState<AttachmentPickerSource | null>(null)
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
  const attachmentsSendable = attachments.every(canSubmitDraftAttachment)
  const initiallyScrolledSession = useRef<string | null>(null)
  const followLatestSession = useRef<string | null>(null)

  function replaceAttachments(next: DraftAttachment[]): void {
    attachmentsRef.current = next
    setAttachments(next)
  }

  function updateAttachments(updater: (current: DraftAttachment[]) => DraftAttachment[]): void {
    replaceAttachments(updater(attachmentsRef.current))
  }

  function handleComposerPaste(event: ClipboardEvent<HTMLTextAreaElement>): void {
    const images = clipboardImageFiles(event)
    if (images.length) void addBrowserFiles(images)
  }

  useLayoutEffect(() => {
    activeViewSession.current = viewSessionId
  }, [viewSessionId])

  useLayoutEffect(() => {
    const transferred = sessionId && draftTransfer.current?.sessionId === sessionId
      ? draftTransfer.current
      : null
    const nextAttachments = transferred?.attachments ?? []
    draftGeneration.current += 1
    attachmentsRef.current = nextAttachments
    if (transferred) draftTransfer.current = null
    setText(transferred?.text ?? '')
    setAttachments(nextAttachments)
    setAttachmentMenuOpen(false)
    setPickerSource(null)
    setRenaming(false)
    setTitleDraft('')
  }, [sessionId])

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
        requestHistoryReload(true)
      }
    }
    document.addEventListener('visibilitychange', recoverWhenVisible)
    return () => document.removeEventListener('visibilitychange', recoverWhenVisible)
  }, [requestHistoryReload, sessionId])

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
  }, [historyReload.version, refreshSessions, renderedLastMessageId, session?.unread, sessionId, visibleLatestKey])

  const processEvent = useCallback((
    event: StreamEvent,
    targetSessionId: string,
    sentText: string,
    sentEffort: Effort,
    sentAttachments: MessageAttachmentRef[],
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
            attachment_refs: sentAttachments,
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

  async function addBrowserFiles(selectedFiles: File[]): Promise<void> {
    if (!selectedFiles.length) return
    const generation = draftGeneration.current
    const currentAttachments = attachmentsRef.current
    if (currentAttachments.length + selectedFiles.length > 10) {
      setNotice({
        sessionId: viewSessionId,
        message: t('chat.tooManyAttachments', { defaultValue: 'A message can include at most 10 attachments.' }),
      })
      return
    }
    const drafts = selectedFiles.map((file) => ({
      id: idFactory(),
      name: file.name,
      source: t('chat.thisComputer', { defaultValue: 'This computer' }),
      status: 'uploading' as const,
      file,
    }))
    const existingBrowserFiles = currentAttachments.flatMap((attachment) => attachment.file ? [attachment.file] : [])
    updateAttachments((current) => [...current, ...drafts])
    setNotice(null)

    startBrowserAttachmentTask(drafts, [...existingBrowserFiles, ...selectedFiles], generation)
  }

  function startBrowserAttachmentTask(
    drafts: DraftAttachment[],
    filesToValidate: File[],
    generation: number,
  ): void {
    const draftIds = new Set(drafts.map((draft) => draft.id))
    updateAttachments((current) => current.map((attachment) => draftIds.has(attachment.id)
      ? { ...attachment, status: 'uploading' }
      : attachment))
    const task = (async () => {
      let failure: unknown
      try {
        await validateBrowserAttachmentFiles(filesToValidate)
        if (generation !== draftGeneration.current) return
        const results = await Promise.allSettled(drafts.map(async (draft) => {
          if (!attachmentsRef.current.some((attachment) => attachment.id === draft.id)) return
          try {
            const ref = await uploadBrowserAttachment(draft.file, draft.id)
            if (generation !== draftGeneration.current) return
            updateAttachments((current) => current.map((attachment) => attachment.id === draft.id
              ? { ...attachment, status: 'ready', ref }
              : attachment))
          } catch (caught) {
            if (generation === draftGeneration.current) {
              updateAttachments((current) => current.map((attachment) => attachment.id === draft.id
                ? { ...attachment, status: 'failed' }
                : attachment))
            }
            throw caught
          }
        }))
        failure = results.find((result) => result.status === 'rejected')?.reason ?? null
      } catch (caught) {
        failure = caught
        if (generation === draftGeneration.current) {
          updateAttachments((current) => current.map((attachment) => draftIds.has(attachment.id)
            ? { ...attachment, status: 'failed' }
            : attachment))
        }
      }
      if (failure && generation === draftGeneration.current) {
        setNotice({
          sessionId: activeViewSession.current,
          message: chatErrorMessage(failure, t('chat.attachmentUploadFailed', {
            defaultValue: 'The attachment could not be uploaded.',
          })),
        })
      }
    })()
    for (const draft of drafts) attachmentTasks.current.set(draft.id, task)
    void task.finally(() => {
      for (const draft of drafts) {
        if (attachmentTasks.current.get(draft.id) === task) attachmentTasks.current.delete(draft.id)
      }
    })
  }

  async function waitForAttachmentTasks(draftIds: Set<string>): Promise<void> {
    const tasks = [...new Set([...draftIds].flatMap((draftId) => {
      const task = attachmentTasks.current.get(draftId)
      return task ? [task] : []
    }))]
    await Promise.allSettled(tasks)
  }

  function addExistingAttachment(ref: MessageAttachmentRef): void {
    if (attachmentsRef.current.length >= 10) {
      setNotice({
        sessionId: viewSessionId,
        message: t('chat.tooManyAttachments', { defaultValue: 'A message can include at most 10 attachments.' }),
      })
      return
    }
    updateAttachments((current) => [...current, {
      id: idFactory(),
      name: attachmentFilename(ref.path),
      source: ref.openoctopus_device === 'server'
        ? t('chat.serverWorkspace', { defaultValue: 'Server Workspace' })
        : ref.openoctopus_device,
      status: 'ready',
      ref,
    }])
    setPickerSource(null)
    setNotice(null)
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const currentAttachments = attachmentsRef.current
    if (sending || !currentAttachments.every(canSubmitDraftAttachment) || (!text.trim() && currentAttachments.length === 0)) return

    const sentText = text
    let sentDraftAttachments = currentAttachments
    let sentAttachments: MessageAttachmentRef[] = []
    const sentEffort = effort
    const targetSessionId = sessionId ?? idFactory()
    const isNewSession = sessionId === undefined
    const attachmentGeneration = draftGeneration.current
    const attachmentIds = new Set(currentAttachments.map((attachment) => attachment.id))
    const failedAtStart = currentAttachments.filter((attachment) => attachment.status === 'failed' && attachment.file)
    let shouldRecover = false
    const generation = streamGeneration.current + 1
    streamGeneration.current = generation

    setSending(true)
    setNotice(null)

    try {
      if (failedAtStart.length) {
        startBrowserAttachmentTask(
          failedAtStart,
          currentAttachments.flatMap((attachment) => attachment.file ? [attachment.file] : []),
          attachmentGeneration,
        )
      }
      await waitForAttachmentTasks(attachmentIds)
      if (attachmentGeneration !== draftGeneration.current) return

      const settledAttachments = [...attachmentIds].flatMap((attachmentId) => {
        const attachment = attachmentsRef.current.find((candidate) => candidate.id === attachmentId)
        return attachment ? [attachment] : []
      })
      if (
        settledAttachments.length !== attachmentIds.size
        || !settledAttachments.every((attachment) => attachment.status === 'ready' && attachment.ref)
      ) {
        setNotice({
          sessionId: viewSessionId,
          message: t('chat.attachmentUploadFailed', { defaultValue: 'The attachment could not be uploaded.' }),
        })
        return
      }

      sentDraftAttachments = settledAttachments
      sentAttachments = settledAttachments.flatMap((attachment) => attachment.ref ? [attachment.ref] : [])
      activeViewSession.current = targetSessionId
      followLatestSession.current = targetSessionId
      setText('')
      replaceAttachments([])
      setStreamSessionId(targetSessionId)
      setLiveText('')
      setLiveThinking('')
      setToolProgress(null)
      if (isNewSession) setLocallyCreatedSessionId(targetSessionId)

      await sendChatMessage({
        sessionId: targetSessionId,
        text: sentText,
        attachments: sentAttachments,
        effort: sentEffort,
        onEvent: (streamEvent) => {
          if (streamGeneration.current !== generation || activeViewSession.current !== targetSessionId) return
          if (streamEvent.type === 'message_accepted') {
            shouldRecover = true
            if (isNewSession) navigate(`/chat/${targetSessionId}`, { replace: true })
          }
          processEvent(streamEvent, targetSessionId, sentText, sentEffort, sentAttachments)
        },
      })
    } catch (error) {
      if (streamGeneration.current !== generation || activeViewSession.current !== targetSessionId) return
      if (error instanceof MessageStreamError) {
        shouldRecover = true
        setNotice({ sessionId: targetSessionId, message: error.message })
        if (error.accepted) {
          setText('')
        } else {
          setText(sentText)
          replaceAttachments(sentDraftAttachments)
          if (isNewSession) {
            draftTransfer.current = {
              sessionId: targetSessionId,
              text: sentText,
              attachments: sentDraftAttachments,
            }
          }
        }
        if (isNewSession) navigate(`/chat/${targetSessionId}`, { replace: true })
      } else {
        setText(sentText)
        replaceAttachments(sentDraftAttachments)
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
          requestHistoryReload(false)
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
      requestHistoryReload(false)
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
      if (activeViewSession.current === targetSessionId) navigate(source ? '/automations' : '/chat', { replace: true })
    } catch (error) {
      if (error instanceof ApiError && error.status === 404) {
        await refreshSessions()
        if (activeViewSession.current === targetSessionId) navigate(source ? '/automations' : '/chat', { replace: true })
        return
      }
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
        <div className="breadcrumbs">
          {source
            ? <Link to="/automations">{t('nav.automations')}</Link>
            : externalSource
              ? <Link to="/channels">{t('nav.channels')}</Link>
              : <span>{t('nav.chat')}</span>}
          <span aria-hidden="true">/</span>
          {source ? <span className="status-badge">{t(`automations.${source}`)}</span> : null}
          {externalSource ? <span className="status-badge">{t(`channels.platform.${externalSource}`)}</span> : null}
          <strong>{title}</strong>
          {source ? <Link className="chat-automation-back" to="/automations">{t('automations.back')}</Link> : null}
        </div>
        <div className="chat-header-actions">
          <DeviceMenu devices={devices.data ?? []} />
          {sessionId ? (
            <div className="chat-session-controls">
            <button
              type="button"
              className="chat-secondary-button chat-session-control-optional"
              onClick={() => requestHistoryReload(true)}
            >{t('common.refresh')}</button>
            {history?.status === 'running' || session?.cancel_requested ? (
              <button type="button" className="chat-secondary-button" onClick={() => void handleCancel()}>
                {t('chat.stop', { defaultValue: 'Stop' })}
              </button>
            ) : null}
            <button
              type="button"
              className="chat-secondary-button chat-session-control-optional"
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
                  <MessageAuthor sender={message.sender} fallback={t('chat.you', { defaultValue: 'You' })} />
                  <span>{t('chat.pending', { defaultValue: 'Pending' })}</span>
                </header>
                <ContentBlocks blocks={message.content} />
                <AttachmentRefs refs={message.attachment_refs} />
                <ChannelContextDetails context={message.channel_context} />
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
            {attachments.length ? (
              <ul className="chat-attachments" aria-label={t('chat.pendingAttachments', { defaultValue: 'Attachments to send' })}>
                {attachments.map((attachment) => (
                  <li key={attachment.id} data-status={attachment.status}>
                    <span><strong>{attachment.name}</strong><small>{attachment.source}</small></span>
                    <em>{attachment.status === 'uploading'
                      ? t('chat.attachmentUploading', { defaultValue: 'Uploading' })
                      : attachment.status === 'ready'
                        ? t('chat.attachmentReady', { defaultValue: 'Ready' })
                        : t('chat.attachmentFailed', { defaultValue: 'Failed' })}</em>
                    <button
                      type="button"
                      disabled={sending}
                      onClick={() => updateAttachments((current) => current.filter((item) => item.id !== attachment.id))}
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
              onPaste={handleComposerPaste}
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
                    event.target.value = ''
                    void addBrowserFiles(selectedFiles)
                  }}
                />
                <div className="chat-attachment-source">
                  <button
                    type="button"
                    className="text-button"
                    aria-label={t('draftChat.attachment')}
                    aria-expanded={attachmentMenuOpen}
                    disabled={sending || attachments.length >= 10}
                    onClick={() => setAttachmentMenuOpen((current) => !current)}
                  >＋ {t('draftChat.attachment')}</button>
                  {attachmentMenuOpen ? (
                    <div className="chat-attachment-source-menu" role="menu">
                      <button type="button" role="menuitem" onClick={() => {
                        setAttachmentMenuOpen(false)
                        fileInput.current?.click()
                      }}>{t('chat.thisComputer', { defaultValue: 'This computer' })}</button>
                      <button type="button" role="menuitem" onClick={() => {
                        setAttachmentMenuOpen(false)
                        setPickerSource({ kind: 'server' })
                      }}>{t('chat.serverWorkspaces', { defaultValue: 'Server Workspaces' })}</button>
                      {(devices.data ?? []).filter((device) => device.online).map((device) => (
                        <button key={device.id} type="button" role="menuitem" onClick={() => {
                          setAttachmentMenuOpen(false)
                          setPickerSource({ kind: 'device', device })
                        }}>{device.name}</button>
                      ))}
                    </div>
                  ) : null}
                </div>
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
                disabled={sending || !attachmentsSendable || (!text.trim() && attachments.length === 0)}
              >{sending ? '…' : '↑'}</button>
            </div>
          </form>
        ) : null}
        {pickerSource ? (
          <AttachmentPicker
            source={pickerSource}
            onSelect={addExistingAttachment}
            onClose={() => setPickerSource(null)}
          />
        ) : null}
      </div>
    </>
  )
}

function useRecoveredHistory(
  sessionId: string | undefined,
  pollIntervalMs: number,
  version: number,
  fromStart: boolean,
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

    const load = async (
      after: string | null,
      pollCount: number,
      terminalSnapshot = false,
    ): Promise<void> => {
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
        if (!hasRecoveryWork && after !== null && !terminalSnapshot) {
          await load(null, pollCount, true)
          return
        }
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
    const resumeAfter = !fromStart && cursor?.sessionId === sessionId
      ? cursor.messageId
      : null
    void load(resumeAfter, 0)
    return () => {
      disposed = true
      if (timer) clearTimeout(timer)
    }
  }, [fromStart, pollIntervalMs, sessionId, t, version])

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
  const label = isToolResult
    ? t('chat.toolResult', { defaultValue: 'Tool result' })
    : isHuman
      ? t('chat.you', { defaultValue: 'You' })
      : 'OpenOctopus'
  return (
    <article className={`chat-message chat-message-${isHuman ? 'user' : 'assistant'}${message.is_compacted ? ' chat-message-compacted' : ''}`}>
      <header>
        <MessageAuthor sender={isHuman ? message.sender : null} fallback={label} />
        <span>{message.message_kind === 'compaction_summary'
          ? t('chat.compactionSummary', { defaultValue: 'Context summary' })
          : formatTime(message.created_at, i18n.resolvedLanguage)}</span>
      </header>
      <ContentBlocks blocks={message.content} />
      <AttachmentRefs refs={message.attachment_refs} />
      <ChannelContextDetails context={message.channel_context} />
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
      <ChannelDeliveries deliveries={message.deliveries} />
    </article>
  )
}

function MessageAuthor({
  sender,
  fallback,
}: {
  sender: MessageSender | null | undefined
  fallback: string
}): ReactNode {
  const { t } = useTranslation()
  if (!sender || sender.classification === 'internal') return <strong>{fallback}</strong>
  return (
    <span className="chat-message-author">
      <strong>{sender.display_name || sender.id}</strong>
      <small className="chat-sender-badge">
        {sender.classification === 'owner' ? t('chat.senderOwner') : t('chat.senderAllowed')}
      </small>
      <code title={t('chat.senderId', { id: sender.id })}>{sender.id}</code>
    </span>
  )
}

function ChannelContextDetails({ context }: { context: ChannelContext | null | undefined }): ReactNode {
  const { i18n, t } = useTranslation()
  if (!context || (context.included_count === 0 && context.omitted_count === 0)) return null
  if (context.included_count === 0) {
    return (
      <p className="chat-channel-context chat-channel-context-omitted">
        {t('chat.omittedContext', { count: context.omitted_count })}
      </p>
    )
  }
  return (
    <details
      className="chat-channel-context"
      aria-label={t('chat.channelContext', { count: context.included_count })}
    >
      <summary>{t('chat.channelContext', { count: context.included_count })}</summary>
      <div className="chat-channel-context-body">
        <strong>{t('chat.untrustedContext')}</strong>
        <ul>
          {context.entries.map((entry, index) => (
            <li key={`${entry.source_message_id ?? 'context'}:${index}`}>
              <header>
                <strong>{entry.sender_display_name || entry.sender_id || '—'}</strong>
                {entry.sent_at ? <span>{formatTime(entry.sent_at, i18n.resolvedLanguage)}</span> : null}
              </header>
              <p>{entry.text}</p>
              {entry.attachment_summaries.length ? (
                <small>{t('chat.contextAttachments', { attachments: entry.attachment_summaries.join(', ') })}</small>
              ) : null}
            </li>
          ))}
        </ul>
        {context.omitted_count ? <p>{t('chat.omittedContext', { count: context.omitted_count })}</p> : null}
      </div>
    </details>
  )
}

function ChannelDeliveries({ deliveries }: { deliveries: ChannelDelivery[] | undefined }): ReactNode {
  const { t } = useTranslation()
  if (!deliveries?.length) return null
  return (
    <ul className="chat-channel-deliveries" aria-label={t('chat.deliveryTitle')}>
      {deliveries.map((delivery, index) => {
        const platform = t(`channels.platform.${delivery.channel}`)
        const needsNewMessage = delivery.status === 'partial'
          || delivery.status === 'failed'
          || delivery.status === 'unknown'
        return (
          <li key={`${delivery.channel}:${delivery.chat_id}:${delivery.created_at}:${index}`}>
            <div>
              <strong>{platform}</strong>
              <span className={`status-badge status-${deliveryTone(delivery.status)}`}>
                {t(`chat.delivery${capitalize(delivery.status)}`)}
              </span>
              <small>{t('chat.deliveryProgress', {
                sent: delivery.visible_sent_actions,
                total: delivery.total_actions,
              })}</small>
            </div>
            {needsNewMessage ? <p>{t('chat.deliveryRetry', { channel: platform })}</p> : null}
          </li>
        )
      })}
    </ul>
  )
}

function deliveryTone(status: ChannelDelivery['status']): 'neutral' | 'success' | 'warning' | 'danger' {
  if (status === 'sent') return 'success'
  if (status === 'partial' || status === 'unknown' || status === 'attempting') return 'warning'
  if (status === 'failed') return 'danger'
  return 'neutral'
}

function capitalize(value: string): string {
  return `${value.slice(0, 1).toUpperCase()}${value.slice(1)}`
}

function AttachmentRefs({ refs }: { refs: MessageAttachmentRef[] }): ReactNode {
  const { t } = useTranslation()
  if (!refs.length) return null
  return (
    <ul className="chat-attachment-refs">
      {refs.map((ref, index) => (
        <li key={`${attachmentKey(ref)}:${index}`}>
          <strong>{attachmentFilename(ref.path)}</strong>
          <small>{ref.openoctopus_device === 'server'
            ? t('chat.serverWorkspace', { defaultValue: 'Server Workspace' })
            : ref.openoctopus_device}</small>
        </li>
      ))}
    </ul>
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

function attachmentFilename(path: string): string {
  const parts = path.split('/').filter(Boolean)
  return parts.at(-1) ?? path
}

function canSubmitDraftAttachment(attachment: DraftAttachment): boolean {
  return Boolean(attachment.ref || attachment.file)
}

function clipboardImageFiles(event: ClipboardEvent<HTMLTextAreaElement>): File[] {
  return Array.from(event.clipboardData.items)
    .filter((item) => item.kind === 'file' && item.type.startsWith('image/'))
    .flatMap((item, index) => {
      const file = item.getAsFile()
      if (!file) return []
      if (file.name) return [file]
      return [new File([file], pastedImageFilename(file.type, index), {
        type: file.type,
        lastModified: file.lastModified,
      })]
    })
}

function pastedImageFilename(type: string, index: number): string {
  const subtype = type.split('/')[1]?.split('+')[0]
  const extension = subtype === 'jpeg'
    ? 'jpg'
    : subtype && /^[a-z0-9]+$/i.test(subtype) ? subtype : 'bin'
  return `pasted-image${index ? `-${index + 1}` : ''}.${extension}`
}

function attachmentKey(ref: MessageAttachmentRef): string {
  return `${'device_id' in ref ? ref.device_id : 'server'}:${ref.path}`
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

function automationChannel(value: string | null | undefined): 'cron' | 'heartbeat' | null {
  return value === 'cron' || value === 'heartbeat' ? value : null
}

function externalChannel(value: string | null | undefined): 'discord' | 'dingtalk' | null {
  return value === 'discord' || value === 'dingtalk' ? value : null
}
