import { apiJson } from '../api/client'
import type { CronJob, CronJobPage } from '../api/types'

export type { CronJob, CronJobPage, CronJobSummary, CronSchedule } from '../api/types'

export type CronWrite = {
  name?: string
  message: string
} & (
  | { every_seconds: number }
  | { cron_expr: string; tz: string }
  | { at: string; tz: string }
)

export function listCronJobs(offset: number): Promise<CronJobPage> {
  return apiJson(`/api/cron?limit=50&offset=${offset}`)
}

export function getCronJob(id: string): Promise<CronJob> {
  return apiJson(`/api/cron/${encodeURIComponent(id)}`)
}

export function createCronJob(body: CronWrite): Promise<CronJob> {
  return apiJson('/api/cron', {
    method: 'POST',
    body: JSON.stringify(body),
  })
}

export function updateCronJob(id: string, body: CronWrite): Promise<CronJob> {
  return apiJson(`/api/cron/${encodeURIComponent(id)}`, {
    method: 'PATCH',
    body: JSON.stringify(body),
  })
}

export function deleteCronJob(id: string): Promise<undefined> {
  return apiJson(`/api/cron/${encodeURIComponent(id)}`, { method: 'DELETE' })
}
