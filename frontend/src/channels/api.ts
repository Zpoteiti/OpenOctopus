import { apiJson } from '../api/client'

export type ExternalChannel = 'discord' | 'dingtalk'
export type ChannelState = 'stopped' | 'connecting' | 'awaiting_pairing' | 'ready' | 'degraded'

export interface ChannelConfig {
  channel: ExternalChannel
  configured: boolean
  state: ChannelState
  bot: { id: string; name: string | null; avatar_url: string | null } | null
  owner: { id: string; dm_chat_id: string } | null
  allow_list: string[]
  credential_hint: 'Configured' | null
  pairing: { expires_at: string; code: string | null } | null
  last_error: { code: string; message: string; at: string } | null
}

export interface ChannelConfigPatch {
  bot_token?: string
  client_id?: string
  client_secret?: string
  allow_list: string[]
}

export function loadChannels(): Promise<ChannelConfig[]> {
  return apiJson<ChannelConfig[]>('/api/channels')
}

export function saveChannel(
  channel: ExternalChannel,
  patch: ChannelConfigPatch,
): Promise<ChannelConfig> {
  return apiJson<ChannelConfig>(`/api/channels/${channel}`, {
    method: 'PATCH',
    body: JSON.stringify(patch),
  })
}

export function generatePairingCode(channel: ExternalChannel): Promise<ChannelConfig> {
  return apiJson<ChannelConfig>(`/api/channels/${channel}/pairing`, { method: 'POST' })
}

export function deleteChannel(channel: ExternalChannel): Promise<void> {
  return apiJson<void>(`/api/channels/${channel}`, { method: 'DELETE' })
}

export function stoppedChannel(channel: ExternalChannel): ChannelConfig {
  return {
    channel,
    configured: false,
    state: 'stopped',
    bot: null,
    owner: null,
    allow_list: [],
    credential_hint: null,
    pairing: null,
    last_error: null,
  }
}

export function channelRefetchInterval(
  configs: ChannelConfig[] | undefined,
  visible: boolean,
): number | false {
  if (!visible) return false
  return configs?.some((config) => config.configured && (
    config.state === 'connecting'
    || config.state === 'awaiting_pairing'
    || config.state === 'degraded'
  )) ? 3_000 : 30_000
}
