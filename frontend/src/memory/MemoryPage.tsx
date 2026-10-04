import { useCallback, useEffect, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'

import { ApiError, apiJson } from '../api/client'
import { Card, PageHeader } from '../components/Page'

interface Note { path: string; content: string; version: string | null }
interface Page { paths: string[]; next_offset: number | null }

export function MemoryPage() {
  const { t } = useTranslation()
  const [paths, setPaths] = useState<string[]>([])
  const [nextOffset, setNextOffset] = useState<number | null>(null)
  const [note, setNote] = useState<Note | null>(null)
  const [content, setContent] = useState('')
  const [newPath, setNewPath] = useState('')
  const [busy, setBusy] = useState(true)
  const [error, setError] = useState<unknown>(null)
  const [saved, setSaved] = useState(false)
  const request = useRef(0)
  const url = (path: string) => `/api/memory/${encodeURIComponent(path)}`

  const load = useCallback((path: string) => {
    const id = ++request.current
    return Promise.all([
      apiJson<Page>('/api/memory'), apiJson<Note>(url(path)),
    ]).then(([page, selected]) => {
      if (id !== request.current) return
      setPaths(page.paths); setNextOffset(page.next_offset)
      setNote(selected); setContent(selected.content)
    }).catch((failure: unknown) => {
      if (id === request.current) setError(failure)
    }).finally(() => {
      if (id === request.current) setBusy(false)
    })
  }, [])

  useEffect(() => { void load('MEMORY.md'); return () => { request.current += 1 } }, [load])

  async function open(path: string) {
    setBusy(true); setError(null); setSaved(false)
    await load(path)
  }

  async function save() {
    if (!note) return
    setBusy(true); setError(null); setSaved(false)
    try {
      const updated = await apiJson<Note>(url(note.path), {
        method: 'PUT', body: JSON.stringify({ content, expected_version: note.version }),
      })
      setNote(updated); setSaved(true)
      setPaths((current) => [...new Set([...current, updated.path])].sort())
    } catch (failure) {
      setError(failure)
    } finally { setBusy(false) }
  }

  async function remove() {
    if (!note?.version) return
    setBusy(true); setError(null); setSaved(false)
    try {
      await apiJson(`${url(note.path)}?expected_version=${encodeURIComponent(note.version)}`, { method: 'DELETE' })
      await load('MEMORY.md')
    } catch (failure) {
      setError(failure)
    } finally { setBusy(false) }
  }

  async function more() {
    if (nextOffset === null) return
    setBusy(true); setError(null)
    try {
      const page = await apiJson<Page>(`/api/memory?offset=${nextOffset}`)
      setPaths((current) => [...new Set([...current, ...page.paths])]); setNextOffset(page.next_offset)
    } catch (failure) { setError(failure) }
    finally { setBusy(false) }
  }

  return <div className="page-scroll">
    <PageHeader title={t('memory.title')} description={t('memory.description')} />
    <div className="settings-stack">
    {error != null && <p className="form-error" role="alert">{error instanceof ApiError && error.status === 409
      ? t('memory.conflict') : error instanceof Error ? error.message : t('memory.failed')}</p>}
    {saved && <p role="status">{t('memory.saved')}</p>}
    <Card title={t('memory.notes')}>
    <nav className="page-actions" aria-label={t('memory.notes')}>
      {[...new Set(['MEMORY.md', ...paths])].map((path) => <button
        key={path} type="button" className="secondary-button" disabled={busy}
        aria-current={note?.path === path ? 'page' : undefined} onClick={() => void open(path)}
      >{path}</button>)}
      {nextOffset !== null && <button type="button" className="secondary-button" disabled={busy} onClick={() => void more()}>{t('memory.more')}</button>}
    </nav>
    <form className="form-grid" onSubmit={(event) => { event.preventDefault(); if (newPath.trim()) void open(newPath.trim()) }}>
      <label className="full-row">{t('memory.newPath')} <input value={newPath} disabled={busy}
        onChange={(event) => setNewPath(event.target.value)} placeholder="topics/preferences.md" /></label>
      <div className="form-actions full-row">
        <button type="submit" className="secondary-button" disabled={busy || !newPath.trim()}>{t('memory.open')}</button>
      </div>
    </form>
    </Card>
    {note && <Card title={note.path}>
      <div className="form-grid">
      <label className="full-row">{t('memory.content')}<textarea aria-label={t('memory.content')} rows={20}
        maxLength={65536} value={content} disabled={busy}
        onChange={(event) => { setContent(event.target.value); setSaved(false) }} /></label>
      <div className="form-actions full-row">
      <button type="button" className="primary-button" disabled={busy} onClick={() => void save()}>{t('memory.save')}</button>
      <button type="button" className="secondary-button" disabled={busy} onClick={() => void open(note.path)}>{t('memory.reload')}</button>
      <button type="button" className="danger-button" disabled={busy || !note.version} onClick={() => void remove()}>{t('memory.delete')}</button>
      </div>
      </div>
    </Card>}
    </div>
  </div>
}
