import { apiJson } from '../api/client'
import type { components } from '../api/openapi'

export type DreamStatus = components['schemas']['JevStatus']
export type DreamItem = components['schemas']['DreamRunResponse']
export type DreamPage = components['schemas']['DreamRunsResponse']
export type DreamDetail = components['schemas']['DreamRunDetail']

export function listDream(offset: number): Promise<DreamPage> {
  return apiJson<DreamPage>(`/api/dream?limit=50&offset=${offset}`)
}

export function getDream(id: string): Promise<DreamDetail> {
  return apiJson<DreamDetail>(`/api/dream/${encodeURIComponent(id)}`)
}

export function restoreDream(id: string): Promise<DreamDetail> {
  return apiJson<DreamDetail>(`/api/dream/${encodeURIComponent(id)}/restore`, { method: 'POST' })
}
