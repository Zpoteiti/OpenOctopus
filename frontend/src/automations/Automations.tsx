import { useInfiniteQuery, useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { type FormEvent, type ReactNode, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Link } from 'react-router-dom'

import { ApiError, apiJson } from '../api/client'
import type { User } from '../api/types'
import { Card, ErrorNotice, PageHeader } from '../components/Page'
import { readTextFile, saveTextFile } from '../workspace/api'
import {
  createCronJob,
  deleteCronJob,
  getCronJob,
  listCronJobs,
  updateCronJob,
  type CronJob,
  type CronJobSummary,
  type CronSchedule,
  type CronWrite,
} from './api'

const HEARTBEAT_PATH = 'HEARTBEAT.md'
const HEARTBEAT_TEMPLATE = `# Heartbeat

## Active Tasks

<!-- Add recurring checks here. Use Cron for exact execution times. -->
`

type ScheduleKind = CronSchedule['type']

interface CronDraft {
  id: string | null
  name: string
  message: string
  kind: ScheduleKind
  everySeconds: string
  cronExpr: string
  at: string
  timezone: string
}

interface HeartbeatFile {
  content: string
  etag: string | null
}

export function AutomationsPage({ user }: { user: User }): ReactNode {
  const { i18n, t } = useTranslation()
  const queryClient = useQueryClient()
  const timezone = userTimezone(user)
  const [draft, setDraft] = useState<CronDraft | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<CronJobSummary | null>(null)
  const [actionError, setActionError] = useState<unknown>(null)
  const [heartbeatDraft, setHeartbeatDraft] = useState<HeartbeatFile | null>(null)

  const cron = useInfiniteQuery({
    queryKey: ['cron-jobs'],
    initialPageParam: 0,
    queryFn: ({ pageParam }) => listCronJobs(pageParam),
    getNextPageParam: (lastPage) => lastPage.next_offset ?? undefined,
  })
  const heartbeat = useQuery({
    queryKey: ['heartbeat-file'],
    queryFn: loadHeartbeatFile,
  })
  const heartbeatHistory = useQuery({
    queryKey: ['heartbeat-history', user.id],
    queryFn: () => heartbeatHistoryExists(user.id),
  })

  const saveHeartbeat = useMutation({
    mutationFn: (file: HeartbeatFile) => saveTextFile(HEARTBEAT_PATH, 'server', file.content, file.etag),
    onSuccess: (result, file) => {
      const saved = {
        content: file.content,
        etag: result.etag,
      }
      setHeartbeatDraft(saved)
      queryClient.setQueryData<HeartbeatFile>(['heartbeat-file'], saved)
    },
  })

  const saveCron = useMutation({
    mutationFn: ({ id, body }: { id: string | null; body: CronWrite }) => (
      id ? updateCronJob(id, body) : createCronJob(body)
    ),
    onSuccess: async () => {
      setDraft(null)
      await queryClient.invalidateQueries({ queryKey: ['cron-jobs'] })
    },
  })

  const removeCron = useMutation({
    mutationFn: (id: string) => deleteCronJob(id),
    onSuccess: async () => {
      setDeleteTarget(null)
      await queryClient.invalidateQueries({ queryKey: ['cron-jobs'] })
    },
  })

  const startEdit = async (job: CronJobSummary): Promise<void> => {
    setActionError(null)
    try {
      setDraft(cronDraft(await getCronJob(job.id), timezone))
    } catch (error) {
      setActionError(error)
    }
  }

  const jobs = cron.data?.pages.flatMap((page) => page.items) ?? []
  const heartbeatFile = heartbeatDraft ?? heartbeat.data ?? null

  return (
    <div className="page-scroll">
      <PageHeader
        eyebrow={t('automations.eyebrow')}
        title={t('automations.title')}
        description={t('automations.description')}
      />
      <ErrorNotice error={actionError ?? cron.error ?? saveCron.error ?? removeCron.error} />
      <div className="settings-stack automations-stack">
        <Card
          title={t('automations.cronTitle')}
          description={t('automations.cronDescription')}
          actions={draft ? null : (
            <button type="button" className="primary-button" onClick={() => {
              setActionError(null)
              setDraft(emptyCronDraft(timezone))
            }}>
              {t('automations.createAutomation')}
            </button>
          )}
        >
          {draft ? (
            <CronForm
              draft={draft}
              pending={saveCron.isPending}
              onChange={setDraft}
              onCancel={() => setDraft(null)}
              onSubmit={(body) => saveCron.mutate({ id: draft.id, body })}
            />
          ) : null}

          {cron.isPending ? <p className="page-status">{t('automations.loadingCron')}</p> : null}
          {!cron.isPending && !jobs.length ? <p className="empty-card-copy">{t('automations.noCron')}</p> : null}
          {jobs.length ? (
            <div className="automation-list">
              {jobs.map((job) => (
                <article className="automation-row" key={job.id}>
                  <div className="automation-row-main">
                    <h3>{job.name}</h3>
                    <p><ScheduleLabel schedule={job.schedule} language={i18n.resolvedLanguage} t={t} /></p>
                    <dl className="automation-times">
                      <div>
                        <dt>{t('automations.lastRun')}</dt>
                        <dd>{job.last_fired_at
                          ? <Timestamp value={job.last_fired_at} timezone={timezone} language={i18n.resolvedLanguage} />
                          : t('automations.neverRun')}</dd>
                      </div>
                      <div>
                        <dt>{t('automations.nextRun')}</dt>
                        <dd><Timestamp value={job.next_fire_at} timezone={timezone} language={i18n.resolvedLanguage} /></dd>
                      </div>
                    </dl>
                  </div>
                  <div className="automation-row-actions">
                    {job.session_id ? (
                      <Link className="secondary-button" to={`/chat/${encodeURIComponent(job.session_id)}?automation=cron`}>
                        {t('automations.viewHistory')}
                      </Link>
                    ) : <span className="status-badge">{t('automations.noRuns')}</span>}
                    <button
                      type="button"
                      className="secondary-button"
                      aria-label={t('automations.editNamed', { name: job.name })}
                      onClick={() => void startEdit(job)}
                    >{t('common.edit')}</button>
                    <button
                      type="button"
                      className="danger-button"
                      aria-label={t('automations.deleteNamed', { name: job.name })}
                      onClick={() => setDeleteTarget(job)}
                    >{t('common.delete')}</button>
                  </div>
                </article>
              ))}
            </div>
          ) : null}
          {cron.hasNextPage ? (
            <div className="form-actions automation-load-more">
              <button
                type="button"
                className="secondary-button"
                disabled={cron.isFetchingNextPage}
                onClick={() => void cron.fetchNextPage()}
              >{t('automations.loadMore')}</button>
            </div>
          ) : null}
        </Card>

        <Card title={t('automations.heartbeatTitle')} description={t('automations.heartbeatDescription')}>
          <div className="heartbeat-meta">
            <span>{t('automations.accountTimezone', { timezone })}</span>
            <Link to="/account">{t('automations.changeTimezone')}</Link>
            {heartbeatHistory.data ? (
              <Link to={`/chat/${encodeURIComponent(user.id)}?automation=heartbeat`}>{t('automations.viewHistory')}</Link>
            ) : heartbeatHistory.isSuccess ? <span>{t('automations.noHeartbeatRuns')}</span> : null}
          </div>
          <p className="field-help">{t('automations.heartbeatTimingHelp')}</p>
          {heartbeat.isPending || heartbeatFile === null ? (
            <p className="page-status">{t('automations.loadingHeartbeat')}</p>
          ) : (
            <div className="heartbeat-editor">
              <label>
                {t('automations.heartbeatFile')}
                <textarea
                  rows={13}
                  maxLength={32000}
                  value={heartbeatFile.content}
                  onChange={(event) => setHeartbeatDraft({ ...heartbeatFile, content: event.target.value })}
                />
              </label>
              <div className="form-actions">
                <button
                  type="button"
                  className="primary-button"
                  disabled={saveHeartbeat.isPending}
                  onClick={() => saveHeartbeat.mutate(heartbeatFile)}
                >{t('automations.saveHeartbeat')}</button>
              </div>
            </div>
          )}
          <ErrorNotice error={heartbeat.error ?? heartbeatHistory.error ?? saveHeartbeat.error} />
        </Card>
      </div>

      {deleteTarget ? (
        <div className="modal-backdrop" role="presentation">
          <div
            className="modal-card"
            role="dialog"
            aria-modal="true"
            aria-labelledby="delete-cron-title"
            onKeyDown={(event) => {
              if (event.key === 'Escape') setDeleteTarget(null)
            }}
          >
            <h2 id="delete-cron-title">{t('automations.deleteTitle')}</h2>
            <p>{t('automations.deleteWarning', { name: deleteTarget.name })}</p>
            <div className="form-actions">
              <button type="button" className="secondary-button" onClick={() => setDeleteTarget(null)}>{t('common.cancel')}</button>
              <button
                type="button"
                className="danger-button"
                disabled={removeCron.isPending}
                autoFocus
                onClick={() => removeCron.mutate(deleteTarget.id)}
              >{t('automations.confirmDelete')}</button>
            </div>
          </div>
        </div>
      ) : null}
    </div>
  )
}

function CronForm({
  draft,
  pending,
  onChange,
  onCancel,
  onSubmit,
}: {
  draft: CronDraft
  pending: boolean
  onChange: (draft: CronDraft) => void
  onCancel: () => void
  onSubmit: (body: CronWrite) => void
}): ReactNode {
  const { t } = useTranslation()
  const submit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    onSubmit(cronWrite(draft))
  }
  return (
    <form className="form-grid automation-form" onSubmit={submit}>
      <label>{t('automations.name')}<input required value={draft.name} maxLength={120} onChange={(event) => onChange({ ...draft, name: event.target.value })} /></label>
      <label>
        {t('automations.scheduleType')}
        <select value={draft.kind} onChange={(event) => onChange({ ...draft, kind: event.target.value as ScheduleKind })}>
          <option value="every">{t('automations.every')}</option>
          <option value="cron">{t('automations.cron')}</option>
          <option value="at">{t('automations.once')}</option>
        </select>
      </label>
      <label className="full-row">{t('automations.task')}<textarea rows={5} required maxLength={32000} value={draft.message} onChange={(event) => onChange({ ...draft, message: event.target.value })} /></label>
      {draft.kind === 'every' ? (
        <label>{t('automations.everySeconds')}<input type="number" min="60" max="31536000" required value={draft.everySeconds} onChange={(event) => onChange({ ...draft, everySeconds: event.target.value })} /></label>
      ) : null}
      {draft.kind === 'cron' ? (
        <label>{t('automations.cronExpression')}<input required maxLength={256} value={draft.cronExpr} placeholder="0 9 * * 1-5" onChange={(event) => onChange({ ...draft, cronExpr: event.target.value })} /></label>
      ) : null}
      {draft.kind === 'at' ? (
        <label>{t('automations.runAt')}<input required maxLength={128} value={draft.at} placeholder="2026-09-03T10:00:00+08:00" onChange={(event) => onChange({ ...draft, at: event.target.value })} /></label>
      ) : null}
      {draft.kind !== 'every' ? (
        <label>{t('automations.timezone')}<input required maxLength={64} value={draft.timezone} onChange={(event) => onChange({ ...draft, timezone: event.target.value })} /></label>
      ) : null}
      <div className="form-actions full-row">
        <button type="button" className="secondary-button" disabled={pending} onClick={onCancel}>{t('common.cancel')}</button>
        <button className="primary-button" disabled={pending}>{draft.id ? t('automations.saveChanges') : t('automations.create')}</button>
      </div>
    </form>
  )
}

async function loadHeartbeatFile(): Promise<HeartbeatFile> {
  try {
    const file = await readTextFile(HEARTBEAT_PATH, 'server')
    return file
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) {
      return { content: HEARTBEAT_TEMPLATE, etag: null }
    }
    throw error
  }
}

async function heartbeatHistoryExists(userId: string): Promise<boolean> {
  try {
    await apiJson(`/api/sessions/${encodeURIComponent(userId)}/messages?limit=1`)
    return true
  } catch (error) {
    if (error instanceof ApiError && error.status === 404) return false
    throw error
  }
}

function emptyCronDraft(timezone: string): CronDraft {
  return {
    id: null,
    name: '',
    message: '',
    kind: 'every',
    everySeconds: '3600',
    cronExpr: '0 9 * * 1-5',
    at: '',
    timezone,
  }
}

function cronDraft(job: CronJob, fallbackTimezone: string): CronDraft {
  const draft = emptyCronDraft(fallbackTimezone)
  if (job.schedule.type === 'every') {
    return { ...draft, id: job.id, name: job.name, message: job.message, everySeconds: String(job.schedule.every_seconds) }
  }
  if (job.schedule.type === 'cron') {
    return { ...draft, id: job.id, name: job.name, message: job.message, kind: 'cron', cronExpr: job.schedule.cron_expr, timezone: job.schedule.tz }
  }
  return {
    ...draft,
    id: job.id,
    name: job.name,
    message: job.message,
    kind: 'at',
    at: instantInTimezone(job.schedule.at, job.schedule.tz),
    timezone: job.schedule.tz,
  }
}

function cronWrite(draft: CronDraft): CronWrite {
  const base = { name: draft.name, message: draft.message }
  if (draft.kind === 'every') return { ...base, every_seconds: Number(draft.everySeconds) }
  if (draft.kind === 'cron') return { ...base, cron_expr: draft.cronExpr, tz: draft.timezone }
  return { ...base, at: draft.at, tz: draft.timezone }
}

function userTimezone(user: User): string {
  return user.timezone
}

function ScheduleLabel({
  schedule,
  language,
  t,
}: {
  schedule: CronSchedule
  language?: string
  t: (key: string, options?: Record<string, unknown>) => string
}): ReactNode {
  if (schedule.type === 'every') return t('automations.everySchedule', { count: schedule.every_seconds })
  if (schedule.type === 'cron') return `${schedule.cron_expr} · ${schedule.tz}`
  return <><time dateTime={schedule.at}>{formatTimestamp(schedule.at, schedule.tz, language)}</time> · {schedule.tz}</>
}

function Timestamp({ value, timezone, language }: { value: string; timezone: string; language?: string }): ReactNode {
  return <time dateTime={value}>{formatTimestamp(value, timezone, language)}</time>
}

function formatTimestamp(value: string, timezone: string, language?: string): string {
  return new Intl.DateTimeFormat(language, {
    year: 'numeric',
    month: 'short',
    day: 'numeric',
    hour: '2-digit',
    minute: '2-digit',
    timeZoneName: 'short',
    timeZone: timezone,
  }).format(new Date(value))
}

function instantInTimezone(value: string, timezone: string): string {
  const instant = new Date(value)
  if (Number.isNaN(instant.getTime())) return value
  const parts = new Intl.DateTimeFormat('en-CA', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
    hourCycle: 'h23',
    timeZone: timezone,
  }).formatToParts(instant)
  const values = Object.fromEntries(parts.map((part) => [part.type, part.value]))
  const wallTime = Date.UTC(
    Number(values.year),
    Number(values.month) - 1,
    Number(values.day),
    Number(values.hour),
    Number(values.minute),
    Number(values.second),
  )
  const instantAtSecond = Math.floor(instant.getTime() / 1000) * 1000
  const offsetMinutes = Math.round((wallTime - instantAtSecond) / 60000)
  const offsetSign = offsetMinutes < 0 ? '-' : '+'
  const absoluteOffset = Math.abs(offsetMinutes)
  const offset = `${offsetSign}${String(Math.floor(absoluteOffset / 60)).padStart(2, '0')}:${String(absoluteOffset % 60).padStart(2, '0')}`
  const fraction = /\.\d+(?=Z|[+-]\d{2}:\d{2}$)/.exec(value)?.[0] ?? ''
  return `${values.year}-${values.month}-${values.day}T${values.hour}:${values.minute}:${values.second}${fraction}${offset}`
}
