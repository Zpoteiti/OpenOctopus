import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { ClipboardEvent, FormEvent, ReactNode } from 'react'
import { useCallback, useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate, useParams, useSearchParams } from 'react-router-dom'

import { ApiError, apiJson } from '../api/client'
import type { Device, Effort, Session } from '../api/types'
import {
  MessageStreamError,
  cancelSession,
  deleteSession,
  chatErrorMessage,
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
  upsertMessage,
  type ContentBlock,
} from './model'
import {
  attachmentFilename, canSubmitDraftAttachment, clipboardImageFiles,
  type DraftAttachment,
} from './attachments'
import { AttachmentRefs, ChannelContextDetails, ContentBlocks, MessageAuthor, Transcript } from './Transcript'
import { useRecoveredHistory } from './useRecoveredHistory'
import './ChatPage.css'

export interface ChatPageProps {
  pollIntervalMs?: number
  idFactory?: () => string
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
  const writable = sessionId === undefined || (session?.channel === 'web' && !session.parent_session_id) || isLocalWebSession
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
      const startedIds = new Set(event.message_ids)
      updateHistory(targetSessionId, (current) => {
        const startedMessages = current.pending_messages.filter((message) => startedIds.has(message.id))
        const messages = startedMessages.reduce((saved, message) => (
          saved.some((existing) => existing.id === message.id) ? saved : upsertMessage(saved, {
            ...message,
            role: 'user',
            message_kind: 'human',
            delivery_refs: [],
            created_at: message.received_at,
          })
        ), current.messages)
        return {
          ...current,
          status: 'running',
          active_turn_id: event.turn_id,
          messages,
          pending_messages: current.pending_messages.filter((message) => !startedIds.has(message.id)),
          pending_count: Math.max(0, current.pending_count - startedMessages.length),
          last_message_id: messages.at(-1)?.id ?? current.last_message_id,
        }
      })
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

  const readOnlyMessage = session?.parent_session_id
    ? t('chat.delegateReadOnly', { defaultValue: 'This delegate runs independently. Continue the task in its parent conversation.' })
    : session && session.channel !== 'web'
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
            {history?.status === 'running' || (history && history.active_delegate_count > 0) || session?.cancel_requested ? (
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
                streamingChannel={showingStream ? liveText ? 'text' : liveThinking ? 'thinking' : null : null}
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
                <header><strong>OpenOctopus</strong><span>{t(liveText ? 'chat.generating' : 'chat.thinkingInProgress')}</span></header>
                {liveThinking ? (
                  <details className="chat-thinking" open>
                    <summary>{t('chat.thinking', { defaultValue: 'Thinking' })}</summary>
                    <p>{liveThinking}</p>
                  </details>
                ) : null}
                {liveText ? <ContentBlocks blocks={[{ type: 'text', text: liveText }]} /> : null}
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

function randomUuid(): string {
  return crypto.randomUUID()
}

function automationChannel(value: string | null | undefined): 'cron' | 'heartbeat' | null {
  return value === 'cron' || value === 'heartbeat' ? value : null
}

function externalChannel(value: string | null | undefined): 'discord' | 'dingtalk' | null {
  return value === 'discord' || value === 'dingtalk' ? value : null
}
