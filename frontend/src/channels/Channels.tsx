import { useQuery, useQueryClient } from '@tanstack/react-query'
import type { FormEvent, ReactNode } from 'react'
import { useEffect, useLayoutEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { ApiError } from '../api/client'
import { Card, PageHeader, StatusBadge } from '../components/Page'
import {
  channelRefetchInterval,
  deleteChannel,
  generatePairingCode,
  loadChannels,
  saveChannel,
  stoppedChannel,
  type ChannelConfig,
  type ChannelConfigPatch,
  type ChannelState,
  type ExternalChannel,
} from './api'
import './Channels.css'

const CHANNEL_QUERY_KEY = ['channels'] as const
const CHANNEL_ORDER: ExternalChannel[] = ['discord', 'dingtalk']
const MAX_ALLOW_LIST_IDS = 256
const DISCORD_USER_ID = /^[0-9]{1,20}$/
const CONTROL_CHARACTER = /\p{Cc}/u

export function ChannelsPage(): ReactNode {
  const { t } = useTranslation()
  const queryClient = useQueryClient()
  const channels = useQuery({
    queryKey: CHANNEL_QUERY_KEY,
    queryFn: loadChannels,
    refetchInterval: (query) => channelRefetchInterval(
      query.state.data,
      document.visibilityState === 'visible',
    ),
    refetchIntervalInBackground: false,
    refetchOnWindowFocus: true,
  })
  const refetchChannels = channels.refetch

  useEffect(() => {
    const refresh = (): void => {
      if (document.visibilityState === 'visible') void refetchChannels()
    }
    window.addEventListener('focus', refresh)
    return () => {
      window.removeEventListener('focus', refresh)
    }
  }, [refetchChannels])

  const updateConfig = (updated: ChannelConfig): void => {
    queryClient.setQueryData<ChannelConfig[]>(CHANNEL_QUERY_KEY, (current) => {
      const configs = current ?? []
      return configs.some((item) => item.channel === updated.channel)
        ? configs.map((item) => item.channel === updated.channel ? updated : item)
        : [...configs, updated]
    })
  }

  const removeConfig = (channel: ExternalChannel): void => {
    updateConfig(stoppedChannel(channel))
  }

  return (
    <div className="page-scroll channels-page">
      <PageHeader
        eyebrow={t('channels.eyebrow')}
        title={t('channels.title')}
        description={t('channels.description')}
      />
      {channels.isPending ? <p className="page-status">{t('channels.loading')}</p> : null}
      {channels.isError ? (
        <div className="settings-stack">
          <p className="form-error" role="alert">{channelErrorMessage(channels.error, t)}</p>
          <button className="secondary-button channels-retry" type="button" onClick={() => void channels.refetch()}>
            {t('common.retry')}
          </button>
        </div>
      ) : null}
      {channels.isSuccess ? (
        <div className="settings-stack">
          {!channels.data.length ? <p className="form-notice">{t('channels.empty')}</p> : null}
          {CHANNEL_ORDER.map((channel) => (
            <ChannelCard
              key={channel}
              config={channels.data.find((item) => item.channel === channel) ?? stoppedChannel(channel)}
              onChange={updateConfig}
              onDelete={removeConfig}
            />
          ))}
        </div>
      ) : null}
    </div>
  )
}

function ChannelCard({
  config,
  onChange,
  onDelete,
}: {
  config: ChannelConfig
  onChange: (config: ChannelConfig) => void
  onDelete: (channel: ExternalChannel) => void
}): ReactNode {
  const { t } = useTranslation()
  const [allowList, setAllowList] = useState(config.allow_list.join('\n'))
  const [botToken, setBotToken] = useState('')
  const [clientId, setClientId] = useState('')
  const [clientSecret, setClientSecret] = useState('')
  const [issuedPairing, setIssuedPairing] = useState<{ code: string; expires_at: string } | null>(null)
  const [busy, setBusy] = useState<'save' | 'pairing' | 'delete' | null>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const [notice, setNotice] = useState<string | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [copied, setCopied] = useState(false)
  const saveButtonRef = useRef<HTMLButtonElement>(null)
  const deleteButtonRef = useRef<HTMLButtonElement>(null)
  const confirmDeleteButtonRef = useRef<HTMLButtonElement>(null)
  const deleteFocusTarget = useRef<'confirm' | 'trigger' | 'save' | null>(null)
  const displayName = t(`channels.platform.${config.channel}`)

  useLayoutEffect(() => {
    const target = deleteFocusTarget.current
    if (target === 'confirm' && confirmDelete) {
      confirmDeleteButtonRef.current?.focus()
      deleteFocusTarget.current = null
    } else if (target === 'trigger' && !confirmDelete) {
      deleteButtonRef.current?.focus()
      deleteFocusTarget.current = null
    } else if (target === 'save' && busy === null && !config.configured) {
      saveButtonRef.current?.focus()
      deleteFocusTarget.current = null
    }
  }, [busy, config.configured, confirmDelete])

  const submit = async (event: FormEvent<HTMLFormElement>): Promise<void> => {
    event.preventDefault()
    if (busy) return
    setNotice(null)
    setError(null)
    const parsed = parseAllowList(allowList)
    const allowListError = validateAllowList(config.channel, parsed)
    if (allowListError) {
      setError(t(`channels.${allowListError}`))
      return
    }

    const patch: ChannelConfigPatch = { allow_list: parsed.ids }
    if (config.channel === 'discord') {
      if (!config.configured && !botToken.trim()) {
        setError(t('channels.discordTokenRequired'))
        return
      }
      if (botToken) patch.bot_token = botToken
    } else {
      const hasClientId = Boolean(clientId.trim())
      const hasClientSecret = Boolean(clientSecret.trim())
      if ((!config.configured && (!hasClientId || !hasClientSecret)) || hasClientId !== hasClientSecret) {
        setError(t('channels.dingtalkCredentialsRequired'))
        return
      }
      if (hasClientId && hasClientSecret) {
        patch.client_id = clientId
        patch.client_secret = clientSecret
      }
    }

    setBusy('save')
    try {
      const updated = await saveChannel(config.channel, patch)
      onChange(updated)
      setAllowList(updated.allow_list.join('\n'))
      setBotToken('')
      setClientId('')
      setClientSecret('')
      setNotice(t('channels.saved'))
      const newPairing = pairingCodeFromMutation(updated)
      if (newPairing) setIssuedPairing(newPairing)
    } catch (requestError) {
      setError(channelErrorMessage(requestError, t))
    } finally {
      setBusy(null)
    }
  }

  const rotatePairing = async (): Promise<void> => {
    if (busy) return
    setBusy('pairing')
    setNotice(null)
    setError(null)
    setCopied(false)
    try {
      const updated = await generatePairingCode(config.channel)
      onChange(updated)
      setIssuedPairing(pairingCodeFromMutation(updated))
    } catch (requestError) {
      setError(channelErrorMessage(requestError, t))
    } finally {
      setBusy(null)
    }
  }

  const remove = async (): Promise<void> => {
    if (busy) return
    setBusy('delete')
    setError(null)
    try {
      await deleteChannel(config.channel)
      onDelete(config.channel)
      setAllowList('')
      setBotToken('')
      setClientId('')
      setClientSecret('')
      setIssuedPairing(null)
      setNotice(t('channels.deleted'))
      deleteFocusTarget.current = 'save'
      setConfirmDelete(false)
    } catch (requestError) {
      setError(channelErrorMessage(requestError, t))
    } finally {
      setBusy(null)
    }
  }

  return (
    <section>
      <Card
        title={displayName}
        description={t(`channels.setup.${config.channel}`)}
        actions={<StatusBadge tone={statusTone(config.state)}>{t(`channels.state.${config.state}`)}</StatusBadge>}
      >
        <div className="channel-card-body">
          {config.configured ? <ChannelIdentity config={config} /> : <p className="channel-empty">{t('channels.notConfiguredHelp')}</p>}
          <form className="form-grid channel-form" onSubmit={(event) => void submit(event)}>
            {config.channel === 'discord' ? (
              <label className="full-row">
                <span>{t('channels.botToken')}</span>
                <input
                  aria-label={t('channels.botToken')}
                  type="password"
                  autoComplete="new-password"
                  value={botToken}
                  onChange={(event) => setBotToken(event.target.value)}
                  placeholder={config.configured ? t('channels.secretSavedPlaceholder') : undefined}
                />
                <small>{t('channels.secretHelp')}</small>
              </label>
            ) : (
              <>
                <label>
                  <span>{t('channels.clientId')}</span>
                  <input
                    aria-label={t('channels.clientId')}
                    type="password"
                    autoComplete="new-password"
                    value={clientId}
                    onChange={(event) => setClientId(event.target.value)}
                    placeholder={config.configured ? t('channels.secretSavedPlaceholder') : undefined}
                  />
                </label>
                <label>
                  <span>{t('channels.clientSecret')}</span>
                  <input
                    aria-label={t('channels.clientSecret')}
                    type="password"
                    autoComplete="new-password"
                    value={clientSecret}
                    onChange={(event) => setClientSecret(event.target.value)}
                    placeholder={config.configured ? t('channels.secretSavedPlaceholder') : undefined}
                  />
                </label>
                <small className="field-help full-row">{t('channels.secretHelp')}</small>
              </>
            )}
            <label className="full-row">
              <span>{t('channels.allowList')}</span>
              <textarea
                aria-label={t('channels.allowListFor', { platform: displayName })}
                rows={4}
                value={allowList}
                onChange={(event) => {
                  setAllowList(event.target.value)
                }}
              />
              <small>{t('channels.allowListHelp')}{config.channel === 'dingtalk' ? ` ${t('channels.dingtalkScopeHelp')}` : ''}</small>
            </label>
            {notice ? <p className="form-notice full-row" role="status">{notice}</p> : null}
            {error ? <p className="form-error full-row" role="alert">{error}</p> : null}
            {config.state === 'degraded' && config.last_error ? (
              <p className="form-error full-row" role="alert">{stableChannelError(config.last_error.code, t)}</p>
            ) : null}
            <div className="form-actions full-row">
              <button ref={saveButtonRef} className="primary-button" type="submit" disabled={busy !== null}>
                {busy === 'save' ? t('channels.saving') : t('channels.savePlatform', { platform: displayName })}
              </button>
            </div>
          </form>

          {config.configured && !config.owner ? (
            <section className="channel-pairing" aria-labelledby={`${config.channel}-pairing-heading`}>
              <h3 id={`${config.channel}-pairing-heading`}>{t('channels.pairingTitle')}</h3>
              <p>{t('channels.pairingHelp')}</p>
              {issuedPairing ? (
                <PairingCode
                  pairing={issuedPairing}
                  copied={copied}
                  onCopy={() => {
                    void navigator.clipboard.writeText(issuedPairing.code).then(() => setCopied(true))
                  }}
                />
              ) : null}
              <button className="secondary-button" type="button" disabled={busy !== null} onClick={() => void rotatePairing()}>
                {busy === 'pairing' ? t('channels.generatingCode') : t('channels.generateCode')}
              </button>
            </section>
          ) : null}

          {config.configured ? (
            <section className="channel-danger">
              {confirmDelete ? (
                <div className="channel-delete-confirm" role="group" aria-label={t('channels.deleteConfirmTitle', { platform: displayName })}>
                  <p>{t('channels.deleteWarning', { platform: displayName })}</p>
                  <div>
                    <button ref={confirmDeleteButtonRef} className="danger-button" type="button" disabled={busy !== null} onClick={() => void remove()}>
                      {t('channels.confirmDelete', { platform: displayName })}
                    </button>
                    <button className="secondary-button" type="button" disabled={busy !== null} onClick={() => {
                      deleteFocusTarget.current = 'trigger'
                      setConfirmDelete(false)
                    }}>
                      {t('common.cancel')}
                    </button>
                  </div>
                </div>
              ) : (
                <button ref={deleteButtonRef} className="danger-button" type="button" disabled={busy !== null} onClick={() => {
                  deleteFocusTarget.current = 'confirm'
                  setConfirmDelete(true)
                }}>
                  {t('channels.deletePlatform', { platform: displayName })}
                </button>
              )}
            </section>
          ) : null}
        </div>
      </Card>
    </section>
  )
}

function ChannelIdentity({ config }: { config: ChannelConfig }): ReactNode {
  const { t } = useTranslation()
  return (
    <div className="channel-identity">
      {config.bot?.avatar_url ? <img src={config.bot.avatar_url} alt="" /> : <span className="channel-avatar" aria-hidden="true">{config.channel === 'discord' ? 'D' : '钉'}</span>}
      <dl>
        <div><dt>{t('channels.botName')}</dt><dd>{config.bot?.name ?? '—'}</dd></div>
        <div><dt>{t('channels.botId')}</dt><dd><code>{config.bot?.id ?? '—'}</code></dd></div>
        <div><dt>{t('channels.credentials')}</dt><dd>{config.credential_hint ?? '—'}</dd></div>
        {config.owner ? (
          <>
            <div><dt>{t('channels.ownerId')}</dt><dd><code>{config.owner.id}</code></dd></div>
            <div><dt>{t('channels.ownerDmChatId')}</dt><dd><code>{config.owner.dm_chat_id}</code></dd></div>
          </>
        ) : null}
      </dl>
    </div>
  )
}

function PairingCode({
  pairing,
  copied,
  onCopy,
}: {
  pairing: { code: string; expires_at: string }
  copied: boolean
  onCopy: () => void
}): ReactNode {
  const { t } = useTranslation()
  const [now, setNow] = useState(() => Date.now())
  useEffect(() => {
    const timer = window.setInterval(() => setNow(Date.now()), 1_000)
    return () => window.clearInterval(timer)
  }, [])
  const remainingSeconds = Math.max(0, Math.ceil((new Date(pairing.expires_at).getTime() - now) / 1_000))
  return (
    <div className="channel-pairing-code">
      {remainingSeconds ? (
        <>
          <div className="secret-once">
            <code>{pairing.code}</code>
            <button className="secondary-button" type="button" onClick={onCopy}>
              {copied ? t('channels.codeCopied') : t('channels.copyCode')}
            </button>
          </div>
          <small>{t('channels.codeExpires', { time: formatRemaining(remainingSeconds) })}</small>
        </>
      ) : <p className="form-notice">{t('channels.codeExpired')}</p>}
    </div>
  )
}

function parseAllowList(value: string): { ids: string[]; duplicate: boolean } {
  const ids = value.split('\n').map((id) => id.trim()).filter(Boolean)
  return { ids, duplicate: new Set(ids).size !== ids.length }
}

function validateAllowList(
  channel: ExternalChannel,
  parsed: ReturnType<typeof parseAllowList>,
): 'allowListDuplicate' | 'allowListTooMany' | 'allowListDiscordInvalid' | 'allowListDingTalkInvalid' | null {
  if (parsed.duplicate) return 'allowListDuplicate'
  if (parsed.ids.length > MAX_ALLOW_LIST_IDS) return 'allowListTooMany'
  if (channel === 'discord') {
    return parsed.ids.every((id) => DISCORD_USER_ID.test(id))
      ? null
      : 'allowListDiscordInvalid'
  }
  return parsed.ids.every((id) => (
    Array.from(id).length <= 256 && !CONTROL_CHARACTER.test(id)
  )) ? null : 'allowListDingTalkInvalid'
}

function pairingCodeFromMutation(config: ChannelConfig): { code: string; expires_at: string } | null {
  return config.pairing?.code
    ? { code: config.pairing.code, expires_at: config.pairing.expires_at }
    : null
}

function statusTone(state: ChannelState): 'neutral' | 'success' | 'warning' | 'danger' {
  if (state === 'ready') return 'success'
  if (state === 'degraded') return 'danger'
  if (state === 'connecting' || state === 'awaiting_pairing') return 'warning'
  return 'neutral'
}

function formatRemaining(seconds: number): string {
  const minutes = Math.floor(seconds / 60)
  return `${minutes}:${String(seconds % 60).padStart(2, '0')}`
}

type Translator = (key: string, options?: Record<string, unknown>) => string

function stableChannelError(code: string, t: Translator): string {
  const normalized = code.toUpperCase()
  const knownCodes = new Set([
    'CHANNEL_NOT_SUPPORTED',
    'CHANNEL_CONFIG_NOT_FOUND',
    'CHANNEL_CREDENTIALS_INVALID',
    'CHANNEL_CREDENTIALS_UNVERIFIED',
    'CHANNEL_BOT_ALREADY_BOUND',
    'CHANNEL_PAIRING_UNAVAILABLE',
    'CHANNEL_PAIRING_EXPIRED',
    'CHANNEL_NOT_READY',
    'CHANNEL_TARGET_FORBIDDEN',
    'CHANNEL_ATTACHMENT_NOT_ALLOWED',
    'CHANNEL_PAYLOAD_TOO_LARGE',
    'CHANNEL_DELIVERY_FAILED',
    'CHANNEL_DELIVERY_UNKNOWN',
    'CHANNEL_RETRY_REQUIRES_NEW_TURN',
    'CHANNEL_RUNTIME_FACTORY_FAILED',
    'CHANNEL_RUNTIME_PLATFORM_MISMATCH',
    'CHANNEL_RUNTIME_START_FAILED',
    'CHANNEL_RUNTIME_EXITED',
    'CHANNEL_RUNTIME_CLOSED',
    'CHANNEL_PENDING_RECOVERY_FAILED',
    'CHANNEL_RUNTIME_STOP_TIMEOUT',
    'CHANNEL_RUNTIME_STOP_FAILED',
    'CHANNEL_RUNTIME_UNAVAILABLE',
    'CHANNEL_PAIRING_CONFIRMATION_UNAVAILABLE',
    'CHANNEL_PAIRING_CONFIRMATION_FAILED',
  ])
  return knownCodes.has(normalized)
    ? t(`channels.errors.${normalized}`)
    : t('channels.errors.default')
}

function channelErrorMessage(error: unknown, t: Translator): string {
  return error instanceof ApiError
    ? stableChannelError(error.code, t)
    : t('channels.errors.default')
}
