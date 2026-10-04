import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { chatErrorMessage, loadMessageHistory } from './chatApi'
import { emptyHistory, mergeHistory, type MessageHistory } from './model'

const MAX_RECOVERY_POLLS = 300
const HISTORY_PAGE_LIMIT = 200

interface HistoryState {
  sessionId: string
  history: MessageHistory
  error: string | null
}

export function useRecoveredHistory(
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
        const hasRecoveryWork = incoming.status === 'running' || incoming.active_delegate_count > 0
          || incoming.pending_count > 0 || !caughtUp
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
