import type { TFunction } from 'i18next'
import { useCallback, useEffect, useMemo, useRef, useState, type FormEvent, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'
import { useParams } from 'react-router-dom'

import { ApiError } from '../api/client'
import { useAuthenticatedUser } from '../auth/context'
import i18n from '../i18n'
import {
  MAX_TEXT_PREVIEW_BYTES,
  addMember,
  createSharedWorkspace,
  fileUrl,
  listDevices,
  listDirectory,
  listMembers,
  listWorkspaces,
  readTextFile,
  removeMember,
  saveTextFile,
  uploadFile,
  type Device,
  type ListDirEntry,
  type Workspace,
  type WorkspaceMember,
} from './api'
import './workspace.css'

interface Location {
  key: string
  name: string
  device: string
  root: string
  workspace?: Workspace
}

interface EditorState {
  content: string
  etag: string
}

const TEXT_EXTENSIONS = new Set([
  'c', 'conf', 'cpp', 'css', 'csv', 'go', 'h', 'html', 'ini', 'java', 'js', 'json',
  'jsx', 'log', 'md', 'markdown', 'properties', 'ps1', 'py', 'rb', 'rs', 'sh', 'sql',
  'toml', 'ts', 'tsx', 'tsv', 'txt', 'xml', 'yaml', 'yml',
])

export function WorkspacePage(): ReactNode {
  const { t } = useTranslation()
  const currentUser = useAuthenticatedUser()
  const { workspaceRef } = useParams<{ workspaceRef: string }>()
  const [workspaces, setWorkspaces] = useState<Workspace[]>([])
  const [devices, setDevices] = useState<Device[]>([])
  const [selectedKey, setSelectedKey] = useState('')
  const [currentPath, setCurrentPath] = useState('.')
  const [entries, setEntries] = useState<ListDirEntry[]>([])
  const [selectedEntry, setSelectedEntry] = useState<ListDirEntry | null>(null)
  const [editor, setEditor] = useState<EditorState | null>(null)
  const [members, setMembers] = useState<WorkspaceMember[]>([])
  const [loading, setLoading] = useState(true)
  const [directoryLoading, setDirectoryLoading] = useState(false)
  const [directoryTruncated, setDirectoryTruncated] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [notice, setNotice] = useState<string | null>(null)
  const [upload, setUpload] = useState<File | null>(null)
  const [showCreate, setShowCreate] = useState(false)
  const [createName, setCreateName] = useState('')
  const [createQuota, setCreateQuota] = useState('500')
  const [memberEmail, setMemberEmail] = useState('')
  const [pendingMemberRemoval, setPendingMemberRemoval] = useState<string | null>(null)
  const [busy, setBusy] = useState(false)
  const locationRequest = useRef(0)
  const fileReadRequest = useRef(0)
  const operationRequest = useRef(0)

  const locations = useMemo<Location[]>(() => [
    ...workspaces.map(workspaceLocation),
    ...devices.filter((device) => device.online).map(deviceLocation),
  ], [devices, workspaces])
  const selectedLocation = locations.find((location) => location.key === selectedKey)

  const invalidateOperations = useCallback(() => {
    operationRequest.current += 1
    setBusy(false)
  }, [])

  const activateLocation = useCallback((location: Location) => {
    const request = ++locationRequest.current
    fileReadRequest.current += 1
    invalidateOperations()
    setSelectedKey(location.key)
    setCurrentPath(location.root)
    setSelectedEntry(null)
    setEditor(null)
    setEntries([])
    setMembers([])
    setPendingMemberRemoval(null)
    setDirectoryTruncated(false)
    setError(null)
    setNotice(null)
    setDirectoryLoading(true)

    const tasks: Promise<unknown>[] = [
      listDirectory(location.root, location.device).then((page) => {
        if (request !== locationRequest.current) return
        setEntries(page.items)
        setDirectoryTruncated(page.truncated)
      }),
    ]
    const shared = location.workspace?.type === 'shared' ? location.workspace : undefined
    if (shared?.ref) {
      tasks.push(listMembers(shared.ref).then((page) => {
        if (request === locationRequest.current) setMembers(page.items)
      }))
    }
    Promise.all(tasks)
      .catch((caught: unknown) => {
        if (request === locationRequest.current) setError(errorMessage(caught))
      })
      .finally(() => {
        if (request === locationRequest.current) setDirectoryLoading(false)
      })
  }, [invalidateOperations])

  useEffect(() => {
    let active = true
    Promise.all([listWorkspaces(), listDevices()])
      .then(([workspacePage, deviceList]) => {
        if (!active) return
        setWorkspaces(workspacePage.items)
        setDevices(deviceList)
        const requested = workspaceRef
          ? workspacePage.items.find((workspace) => workspace.ref === workspaceRef)
          : undefined
        const initial = requested ?? workspacePage.items.find((workspace) => workspace.type === 'personal') ?? workspacePage.items[0]
        if (initial) activateLocation(workspaceLocation(initial))
      })
      .catch((caught: unknown) => {
        if (active) setError(errorMessage(caught))
      })
      .finally(() => {
        if (active) setLoading(false)
      })
    return () => {
      active = false
      locationRequest.current += 1
      fileReadRequest.current += 1
      operationRequest.current += 1
    }
  }, [activateLocation, workspaceRef])

  async function openDirectory(path: string): Promise<void> {
    if (!selectedLocation) return
    const request = ++locationRequest.current
    fileReadRequest.current += 1
    invalidateOperations()
    setDirectoryLoading(true)
    setError(null)
    setNotice(null)
    setSelectedEntry(null)
    setEditor(null)
    try {
      const page = await listDirectory(path, selectedLocation.device)
      if (request !== locationRequest.current) return
      setEntries(page.items)
      setDirectoryTruncated(page.truncated)
      setCurrentPath(path)
    } catch (caught) {
      if (request === locationRequest.current) setError(errorMessage(caught))
    } finally {
      if (request === locationRequest.current) setDirectoryLoading(false)
    }
  }

  async function selectEntry(entry: ListDirEntry): Promise<void> {
    if (entry.kind === 'directory') {
      await openDirectory(entry.path)
      return
    }
    const request = ++fileReadRequest.current
    invalidateOperations()
    setSelectedEntry(entry)
    setEditor(null)
    setNotice(null)
    setError(null)
    if (entry.kind !== 'file' || !isTextFile(entry.name) || !selectedLocation) return
    if (entry.size > MAX_TEXT_PREVIEW_BYTES) {
      setError(textPreviewTooLarge())
      return
    }
    try {
      const nextEditor = await readTextFile(entry.path, selectedLocation.device)
      if (request !== fileReadRequest.current) return
      setEditor(nextEditor)
    } catch (caught) {
      if (request !== fileReadRequest.current) return
      if (caught instanceof ApiError && caught.code === 'file_preview_too_large') {
        setError(withApiCode(caught, textPreviewTooLarge()))
      } else if (caught instanceof ApiError && caught.code === 'file_not_utf8') {
        setError(withApiCode(caught, i18n.t('workspace.previewNotUtf8', {
          defaultValue: 'This file is not valid UTF-8 and cannot be edited as text. You can still download it.',
        })))
      } else {
        setError(errorMessage(caught))
      }
    }
  }

  async function saveEditor(): Promise<void> {
    if (!selectedEntry || !selectedLocation || !editor) return
    const request = ++operationRequest.current
    setBusy(true)
    setError(null)
    setNotice(null)
    try {
      const saved = await saveTextFile(
        selectedEntry.path,
        selectedLocation.device,
        editor.content,
        editor.etag,
      )
      if (request !== operationRequest.current) return
      setEditor({ content: editor.content, etag: saved.etag })
      setSelectedEntry({ ...selectedEntry, size: saved.mutation.size })
      setEntries((current) => current.map((entry) => (
        entry.path === selectedEntry.path ? { ...entry, size: saved.mutation.size } : entry
      )))
      setNotice(i18n.t('workspace.saveSuccess', { defaultValue: 'File saved.' }))
    } catch (caught) {
      if (request !== operationRequest.current) return
      if (caught instanceof ApiError && caught.code === 'workspace_file_changed') {
        setError(withApiCode(caught, i18n.t('workspace.fileChanged', {
          defaultValue: 'The file changed elsewhere. Reload it before trying again.',
        })))
      } else {
        setError(errorMessage(caught))
      }
    } finally {
      if (request === operationRequest.current) setBusy(false)
    }
  }

  async function submitUpload(): Promise<void> {
    if (!upload || !selectedLocation) return
    const request = ++operationRequest.current
    setBusy(true)
    setError(null)
    setNotice(null)
    const filename = safeFilename(upload.name)
    const path = currentPath === '.' ? filename : `${currentPath.replace(/\/$/, '')}/${filename}`
    try {
      await uploadFile(path, selectedLocation.device, upload)
      if (request !== operationRequest.current) return
      setUpload(null)
      const page = await listDirectory(currentPath, selectedLocation.device)
      if (request !== operationRequest.current) return
      setEntries(page.items)
      setDirectoryTruncated(page.truncated)
      setNotice(i18n.t('workspace.uploadSuccess', { defaultValue: 'File uploaded.' }))
    } catch (caught) {
      if (request !== operationRequest.current) return
      setError(errorMessage(caught))
    } finally {
      if (request === operationRequest.current) setBusy(false)
    }
  }

  async function submitCreate(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const quotaMiB = Number(createQuota)
    if (!createName.trim() || !Number.isFinite(quotaMiB) || quotaMiB <= 0) {
      setError(i18n.t('workspace.invalidCreate', {
        defaultValue: 'Enter a Workspace name and a quota greater than 0.',
      }))
      return
    }
    setBusy(true)
    setError(null)
    try {
      const created = await createSharedWorkspace(createName.trim(), Math.round(quotaMiB * 1024 * 1024))
      setWorkspaces((current) => [...current, created])
      setCreateName('')
      setCreateQuota('500')
      setShowCreate(false)
      setNotice(i18n.t('workspace.createSuccess', { defaultValue: 'Shared Workspace created.' }))
    } catch (caught) {
      setError(errorMessage(caught))
    } finally {
      setBusy(false)
    }
  }

  async function submitMember(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault()
    const shared = selectedLocation?.workspace
    if (!shared?.ref || !memberEmail.trim()) return
    const request = ++operationRequest.current
    setBusy(true)
    setError(null)
    try {
      const member = await addMember(shared.ref, memberEmail.trim())
      if (request !== operationRequest.current) return
      setMembers((current) => [...current.filter((item) => item.user_id !== member.user_id), member])
      setMemberEmail('')
    } catch (caught) {
      if (request !== operationRequest.current) return
      setError(errorMessage(caught))
    } finally {
      if (request === operationRequest.current) setBusy(false)
    }
  }

  async function deleteMember(member: WorkspaceMember): Promise<void> {
    const shared = selectedLocation?.workspace
    if (!shared?.ref) return
    const request = ++operationRequest.current
    setBusy(true)
    setError(null)
    try {
      await removeMember(shared.ref, member.user_id)
      if (member.user_id === currentUser.id) {
        const remaining = workspaces.filter((workspace) => workspace.id !== shared.id)
        setWorkspaces(remaining)
        if (request === operationRequest.current) {
          setPendingMemberRemoval(null)
          const replacement = remaining.find((workspace) => workspace.type === 'personal') ?? remaining[0]
          if (replacement) activateLocation(workspaceLocation(replacement))
        }
        return
      }
      if (request !== operationRequest.current) return
      setPendingMemberRemoval(null)
      setMembers((current) => current.filter((item) => item.user_id !== member.user_id))
    } catch (caught) {
      if (request !== operationRequest.current) return
      setError(errorMessage(caught))
    } finally {
      if (request === operationRequest.current) setBusy(false)
    }
  }

  const canGoUp = Boolean(selectedLocation && currentPath !== selectedLocation.root)
  const sharedWorkspace = selectedLocation?.workspace?.type === 'shared'
    ? selectedLocation.workspace
    : undefined

  return (
    <>
      <header className="workspace-header">
        <div className="breadcrumbs">
          <span>{t('workspace.title', { defaultValue: 'Workspace' })}</span><span aria-hidden="true">/</span>
          <strong>{selectedLocation?.name ?? t('common.loading', { defaultValue: 'Loading…' })}</strong>
        </div>
        <button className="primary-button" type="button" onClick={() => setShowCreate((value) => !value)}>
          {t('workspace.newShared', { defaultValue: 'New shared Workspace' })}
        </button>
      </header>

      <div className="oo-workspace-page">
        <h1 className="sr-only">
          {t('workspace.title', { defaultValue: 'Workspace' })} — {selectedLocation?.name ?? t('common.loading')}
        </h1>
        {showCreate ? (
          <form className="oo-workspace-create" onSubmit={(event) => void submitCreate(event)}>
            <label>{t('workspace.name', { defaultValue: 'Workspace name' })}<input value={createName} onChange={(event) => setCreateName(event.target.value)} /></label>
            <label>{t('workspace.quotaMiB', { defaultValue: 'Quota (MiB)' })}<input type="number" min="1" value={createQuota} onChange={(event) => setCreateQuota(event.target.value)} /></label>
            <button className="primary-button" disabled={busy}>{t('workspace.create', { defaultValue: 'Create Workspace' })}</button>
          </form>
        ) : null}

        {error ? <p className="oo-workspace-alert" role="alert">{error}</p> : null}
        {notice ? <p className="oo-workspace-notice" role="status">{notice}</p> : null}

        <div className="oo-workspace-browser">
          <aside className="oo-workspace-locations" aria-label={t('workspace.locations', { defaultValue: 'Workspace locations' })}>
            <h2>{t('workspace.serverLocations', { defaultValue: 'Server Workspaces' })}</h2>
            {locations.filter((location) => location.device === 'server').map((location) => (
              <LocationButton key={location.key} location={location} active={location.key === selectedKey} onSelect={activateLocation} />
            ))}
            <h2>{t('workspace.devices', { defaultValue: 'Devices' })}</h2>
            {locations.filter((location) => location.device !== 'server').map((location) => (
              <LocationButton key={location.key} location={location} active={location.key === selectedKey} onSelect={activateLocation} />
            ))}
            {!loading && locations.length === 0 ? <p>{t('workspace.noLocations', { defaultValue: 'No locations are available.' })}</p> : null}
          </aside>

          <section className="oo-workspace-files" aria-label={t('workspace.filesRegion', { defaultValue: 'Workspace files' })}>
            <div className="oo-workspace-toolbar">
              <div>
                <button type="button" disabled={!canGoUp || directoryLoading} onClick={() => void openDirectory(parentPath(currentPath, selectedLocation?.root ?? '.'))}>← {t('workspace.up', { defaultValue: 'Up' })}</button>
                <code>{currentPath}</code>
              </div>
              <div className="oo-workspace-upload">
                <label>
                  {t('workspace.chooseUpload', { defaultValue: 'Choose file' })}
                  <input type="file" onChange={(event) => setUpload(event.target.files?.[0] ?? null)} />
                </label>
                <button type="button" disabled={!upload || busy} onClick={() => void submitUpload()}>{t('workspace.upload', { defaultValue: 'Upload file' })}</button>
              </div>
            </div>

            {directoryLoading ? <p className="oo-workspace-empty">{t('workspace.loadingDirectory', { defaultValue: 'Loading directory…' })}</p> : null}
            {directoryTruncated ? <p className="oo-workspace-alert">{t('workspace.directoryTruncated', { defaultValue: 'The Server scan limit was reached. This list may be incomplete.' })}</p> : null}
            {!directoryLoading && entries.length === 0 ? <p className="oo-workspace-empty">{t('workspace.emptyDirectory', { defaultValue: 'This directory is empty.' })}</p> : null}
            <ul className="oo-workspace-file-list" aria-label={t('workspace.fileList', { defaultValue: 'Workspace files' })}>
              {entries.map((entry) => {
                const tag = specialTag(entry, t)
                return (
                  <li key={entry.path} className={tag ? 'special' : undefined}>
                    <button type="button" disabled={!['file', 'directory'].includes(entry.kind)} onClick={() => void selectEntry(entry)}>
                      <span aria-hidden="true" className={`oo-workspace-file-icon ${entry.kind}`}>{entry.kind === 'directory' ? '▰' : '▤'}</span>
                      <span className="oo-workspace-file-name">{entry.name}</span>
                      {tag ? <span className="oo-workspace-file-tag">{tag}</span> : null}
                      <span className="oo-workspace-file-kind">{kindLabel(entry.kind, t)}</span>
                      <span className="oo-workspace-file-size">{entry.kind === 'file' ? formatBytes(entry.size) : '—'}</span>
                    </button>
                  </li>
                )
              })}
            </ul>
          </section>

          <aside className="oo-workspace-inspector" aria-label={t('workspace.fileDetails', { defaultValue: 'File details' })}>
            {!selectedEntry ? <p>{t('workspace.selectFile', { defaultValue: 'Select a file to view its details.' })}</p> : (
              <>
                <span aria-hidden="true" className="oo-workspace-preview-icon">{selectedEntry.kind === 'file' ? '▤' : '▰'}</span>
                <h2>{selectedEntry.name}</h2>
                <dl>
                  <div><dt>{t('workspace.type', { defaultValue: 'Type' })}</dt><dd>{kindLabel(selectedEntry.kind, t)}</dd></div>
                  <div><dt>{t('workspace.size', { defaultValue: 'Size' })}</dt><dd>{selectedEntry.kind === 'file' ? formatBytes(selectedEntry.size) : '—'}</dd></div>
                  <div><dt>{t('workspace.location', { defaultValue: 'Location' })}</dt><dd><code>{selectedEntry.path}</code></dd></div>
                </dl>
                {selectedEntry.kind === 'file' ? (
                  <a className="oo-workspace-download" href={fileUrl(selectedEntry.path, selectedLocation?.device ?? 'server')} download>{t('workspace.download', { defaultValue: 'Download file' })}</a>
                ) : null}
                {selectedEntry.kind === 'file' && !isTextFile(selectedEntry.name) ? <p>{t('workspace.notText', { defaultValue: 'This file is not opened as text. You can download it instead.' })}</p> : null}
                {editor ? (
                  <div className="oo-workspace-editor">
                    <label>{t('workspace.fileContent', { defaultValue: 'File content' })}<textarea disabled={busy} value={editor.content} onChange={(event) => setEditor({ ...editor, content: event.target.value })} /></label>
                    <button className="primary-button" type="button" disabled={busy} onClick={() => void saveEditor()}>{t('workspace.saveFile', { defaultValue: 'Save file' })}</button>
                  </div>
                ) : null}
              </>
            )}
          </aside>
        </div>

        {sharedWorkspace?.ref ? (
          <section className="oo-workspace-members" aria-labelledby="workspace-members-title">
            <div>
              <h2 id="workspace-members-title">{t('workspace.members', { defaultValue: 'Shared members' })}</h2>
              <p><strong>{t('workspace.equalPermissions', { defaultValue: 'All members have equal permissions' })}</strong><span>{t('workspace.equalPermissionsHelp', { defaultValue: 'Any member can add or remove members, edit the Workspace, or leave it.' })}</span></p>
            </div>
            <form onSubmit={(event) => void submitMember(event)}>
              <label>{t('workspace.memberEmail', { defaultValue: 'Member email' })}<input type="email" value={memberEmail} onChange={(event) => setMemberEmail(event.target.value)} /></label>
              <button type="submit" disabled={busy || !memberEmail.trim()}>{t('workspace.addMember', { defaultValue: 'Add member' })}</button>
            </form>
            <ul>
              {members.map((member) => {
                const isCurrentUser = member.user_id === currentUser.id
                const isLastMember = isCurrentUser && members.length === 1
                const isPending = pendingMemberRemoval === member.user_id
                return (
                  <li key={member.user_id}>
                    <span><strong>{member.name}</strong><small>{member.email}</small></span>
                    {isPending ? (
                      <span className="oo-workspace-member-confirm">
                        {isLastMember ? (
                          <span className="oo-workspace-member-warning" role="alert">
                            {t('workspace.lastMemberWarning', {
                              defaultValue: 'You are the last member. Leaving permanently deletes this Workspace and its files.',
                            })}
                          </span>
                        ) : null}
                        <span>
                          <button type="button" disabled={busy} onClick={() => void deleteMember(member)}>
                            {isLastMember
                              ? t('workspace.confirmLeaveAndDelete', { defaultValue: 'Confirm leave and delete Workspace' })
                              : isCurrentUser
                                ? t('workspace.confirmLeave', { defaultValue: 'Confirm leave' })
                                : t('workspace.confirmMemberRemoval', { defaultValue: 'Confirm removal' })}
                          </button>
                          <button type="button" disabled={busy} onClick={() => setPendingMemberRemoval(null)}>
                            {t('common.cancel', { defaultValue: 'Cancel' })}
                          </button>
                        </span>
                      </span>
                    ) : (
                      <button type="button" disabled={busy} onClick={() => setPendingMemberRemoval(member.user_id)}>
                        {isCurrentUser
                          ? t('workspace.leave', { defaultValue: 'Leave Workspace' })
                          : t('workspace.removeMember', { defaultValue: 'Remove {{name}}', name: member.name })}
                      </button>
                    )}
                  </li>
                )
              })}
            </ul>
          </section>
        ) : null}
      </div>
    </>
  )
}

function LocationButton({
  location,
  active,
  onSelect,
}: {
  location: Location
  active: boolean
  onSelect: (location: Location) => void
}): ReactNode {
  const { t } = useTranslation()
  const detail = location.workspace?.type === 'personal'
    ? t('workspace.personalUsage', {
        defaultValue: 'Personal · {{used}} / {{quota}}',
        used: formatBytes(location.workspace.bytes_used),
        quota: formatBytes(location.workspace.quota_bytes),
      })
    : location.workspace?.type === 'shared'
      ? t('workspace.sharedUsage', {
          defaultValue: 'Shared · {{used}} / {{quota}}',
          used: formatBytes(location.workspace.bytes_used),
          quota: formatBytes(location.workspace.quota_bytes),
        })
      : t('workspace.onlineDevice', { defaultValue: 'Device · Online' })
  return (
    <button aria-pressed={active} className={active ? 'active' : undefined} type="button" onClick={() => onSelect(location)}>
      <span aria-hidden="true" className="oo-workspace-location-mark">{location.device === 'server' ? '◉' : '◇'}</span>
      <span><strong>{location.name}</strong><small>{detail}</small></span>
    </button>
  )
}

function specialTag(entry: ListDirEntry, t: TFunction): string | null {
  if (entry.name === 'SOUL.md') return t('workspace.specialSoul', { defaultValue: 'Agent identity' })
  if (entry.name === 'MEMORY.md') return t('workspace.specialMemory', { defaultValue: 'Long-term memory' })
  if (entry.name === 'skills' && entry.kind === 'directory') return t('workspace.specialSkills', { defaultValue: 'Skills' })
  if (entry.name === '.attachments' && entry.kind === 'directory') return t('workspace.specialAttachments', { defaultValue: 'Attachments' })
  if (entry.name === 'SKILL.md') return t('workspace.specialSkillDefinition', { defaultValue: 'Skill definition' })
  return null
}

function isTextFile(name: string): boolean {
  const extension = name.includes('.') ? name.split('.').pop()?.toLowerCase() : undefined
  return extension ? TEXT_EXTENSIONS.has(extension) : false
}

function parentPath(path: string, root: string): string {
  if (path === root) return root
  const separator = path.lastIndexOf('/')
  if (separator < 0) return root
  const parent = path.slice(0, separator)
  return parent.length < root.length ? root : parent
}

function safeFilename(name: string): string {
  const safe = Array.from(name, (character) => {
    const code = character.charCodeAt(0)
    return character === '/' || character === '\\' || code < 32 || code === 127 ? '_' : character
  }).join('').trim()
  return safe && safe !== '.' && safe !== '..' ? safe : 'upload'
}

function workspaceLocation(workspace: Workspace): Location {
  return {
    key: `workspace:${workspace.id}`,
    name: workspace.name,
    device: 'server',
    root: workspace.type === 'shared' && workspace.ref ? `/${workspace.ref}` : '.',
    workspace,
  }
}

function deviceLocation(device: Device): Location {
  return {
    key: `device:${device.id}`,
    name: device.name,
    device: device.name,
    root: '.',
  }
}

function kindLabel(kind: ListDirEntry['kind'], t: TFunction): string {
  if (kind === 'file') return t('workspace.kindFile', { defaultValue: 'File' })
  if (kind === 'directory') return t('workspace.kindDirectory', { defaultValue: 'Folder' })
  if (kind === 'symlink') return t('workspace.kindSymlink', { defaultValue: 'Symbolic link' })
  return t('workspace.kindOther', { defaultValue: 'Other' })
}

function formatBytes(bytes: number): string {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KiB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MiB`
}

function errorMessage(caught: unknown): string {
  if (caught instanceof ApiError) {
    if (caught.code === 'tool_device_unreachable') {
      return withApiCode(caught, i18n.t('workspace.deviceUnreachable', {
        defaultValue: 'This device is unreachable. Make sure its Client is online and try again.',
      }))
    }
    if (caught.code === 'workspace_soft_locked') {
      return withApiCode(caught, i18n.t('workspace.softLocked', {
        defaultValue: 'This Workspace is over quota. Delete files before writing more data.',
      }))
    }
    return withApiCode(caught, caught.message)
  }
  return caught instanceof Error
    ? caught.message
    : i18n.t('workspace.requestFailed', { defaultValue: 'Request failed. Try again.' })
}

function withApiCode(error: ApiError, message: string): string {
  return message.includes(error.code) ? message : `${message} (${error.code})`
}

function textPreviewTooLarge(): string {
  return i18n.t('workspace.previewTooLarge', {
    defaultValue: 'This file exceeds the 1 MiB text preview limit. You can still download it.',
  })
}
