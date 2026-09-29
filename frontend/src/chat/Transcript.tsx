import type { ReactNode } from 'react'
import ReactMarkdown from 'react-markdown'
import { useTranslation } from 'react-i18next'
import remarkGfm from 'remark-gfm'

import { attachmentFilename, attachmentKey } from './attachments'
import type {
  ChatMessage, ChannelContext, ChannelDelivery, ContentBlock, MessageAttachmentRef, MessageSender,
} from './model'

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

export function MessageAuthor({
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

export function ChannelContextDetails({ context }: { context: ChannelContext | null | undefined }): ReactNode {
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

export function AttachmentRefs({ refs }: { refs: MessageAttachmentRef[] }): ReactNode {
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

export function Transcript({
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

export function ContentBlocks({ blocks }: { blocks: ContentBlock[] }): ReactNode {
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
