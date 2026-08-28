import { useEffect, useRef, useState, type ReactNode } from 'react'
import { useTranslation } from 'react-i18next'

import type { Device } from '../api/types'
import {
  listDirectory,
  listWorkspaces,
  type ListDirEntry,
  type Workspace,
} from '../workspace/api'
import type { MessageAttachmentRef } from './chatApi'

export type AttachmentPickerSource =
  | { kind: 'server' }
  | { kind: 'device'; device: Device }

interface PickerLocation {
  key: string
  name: string
  device: string
  root: string
  deviceId?: string
}

export function AttachmentPicker({
  source,
  onSelect,
  onClose,
}: {
  source: AttachmentPickerSource
  onSelect: (attachment: MessageAttachmentRef) => void
  onClose: () => void
}): ReactNode {
  const { t } = useTranslation()
  const initialDeviceLocation = source.kind === 'device' ? deviceLocation(source.device) : null
  const [locations, setLocations] = useState<PickerLocation[]>(initialDeviceLocation ? [initialDeviceLocation] : [])
  const [selectedLocation, setSelectedLocation] = useState<PickerLocation | null>(initialDeviceLocation)
  const [currentPath, setCurrentPath] = useState('.')
  const [entries, setEntries] = useState<ListDirEntry[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)
  const directoryRequest = useRef(0)

  useEffect(() => {
    let active = true
    const request = ++directoryRequest.current
    const current = (): boolean => active && request === directoryRequest.current
    if (source.kind === 'device') {
      void listDirectory('.', source.device.name, source.device.id)
        .then((page) => {
          if (current()) setEntries(page.items)
        })
        .catch((caught: unknown) => {
          if (current()) setError(errorMessage(caught))
        })
        .finally(() => {
          if (current()) setLoading(false)
        })
      return () => {
        active = false
        if (directoryRequest.current === request) directoryRequest.current += 1
      }
    }
    void listWorkspaces()
      .then(async (page) => {
        if (!current()) return
        const nextLocations = page.items.map(workspaceLocation)
        const initial = page.items.find((workspace) => workspace.type === 'personal') ?? page.items[0]
        const initialLocation = initial ? workspaceLocation(initial) : null
        setLocations(nextLocations)
        setSelectedLocation(initialLocation)
        if (!initialLocation) {
          setLoading(false)
          return
        }
        setCurrentPath(initialLocation.root)
        const directory = await listDirectory(
          initialLocation.root,
          initialLocation.device,
          initialLocation.deviceId,
        )
        if (current()) setEntries(directory.items)
      })
      .catch((caught: unknown) => {
        if (!current()) return
        setError(errorMessage(caught))
        setLoading(false)
      })
      .finally(() => {
        if (current()) setLoading(false)
      })
    return () => {
      active = false
      if (directoryRequest.current === request) directoryRequest.current += 1
    }
  }, [source])

  async function activateLocation(location: PickerLocation): Promise<void> {
    const request = ++directoryRequest.current
    setSelectedLocation(location)
    setCurrentPath(location.root)
    setEntries([])
    setLoading(true)
    setError(null)
    try {
      const page = await listDirectory(location.root, location.device, location.deviceId)
      if (request !== directoryRequest.current) return
      setEntries(page.items)
    } catch (caught) {
      if (request !== directoryRequest.current) return
      setError(errorMessage(caught))
    } finally {
      if (request === directoryRequest.current) setLoading(false)
    }
  }

  async function openDirectory(path: string): Promise<void> {
    if (!selectedLocation) return
    const request = ++directoryRequest.current
    setLoading(true)
    setError(null)
    try {
      const page = await listDirectory(path, selectedLocation.device, selectedLocation.deviceId)
      if (request !== directoryRequest.current) return
      setEntries(page.items)
      setCurrentPath(path)
    } catch (caught) {
      if (request !== directoryRequest.current) return
      setError(errorMessage(caught))
    } finally {
      if (request === directoryRequest.current) setLoading(false)
    }
  }

  function selectFile(entry: ListDirEntry): void {
    if (!selectedLocation || entry.kind !== 'file') return
    onSelect(selectedLocation.deviceId
      ? {
          openoctopus_device: selectedLocation.device,
          device_id: selectedLocation.deviceId,
          path: entry.path,
        }
      : { openoctopus_device: 'server', path: entry.path })
  }

  return (
    <div className="chat-attachment-backdrop" role="presentation">
      <section
        aria-label={t('chat.chooseExistingAttachment', { defaultValue: 'Choose an existing file' })}
        aria-modal="true"
        className="chat-attachment-picker"
        onKeyDown={(event) => {
          if (event.key !== 'Escape') return
          event.preventDefault()
          onClose()
        }}
        role="dialog"
      >
        <header>
          <strong>{source.kind === 'server'
            ? t('chat.serverWorkspaces', { defaultValue: 'Server Workspaces' })
            : source.device.name}</strong>
          <button autoFocus type="button" onClick={onClose}>{t('common.close', { defaultValue: 'Close' })}</button>
        </header>
        <div className={`chat-attachment-picker-body${source.kind === 'device' ? ' chat-attachment-picker-body-device' : ''}`}>
          {source.kind === 'server' ? (
            <nav aria-label={t('workspace.locations', { defaultValue: 'Workspace locations' })}>
              {locations.map((location) => (
                <button
                  aria-pressed={location.key === selectedLocation?.key}
                  key={location.key}
                  type="button"
                  onClick={() => void activateLocation(location)}
                >{location.name}</button>
              ))}
            </nav>
          ) : null}
          <div className="chat-attachment-picker-files">
            <div className="chat-attachment-picker-toolbar">
              <button
                type="button"
                disabled={!selectedLocation || currentPath === selectedLocation.root || loading}
                onClick={() => void openDirectory(parentPath(currentPath, selectedLocation?.root ?? '.'))}
              >← {t('workspace.up', { defaultValue: 'Up' })}</button>
              <code>{currentPath}</code>
            </div>
            {error ? <p role="alert">{error}</p> : null}
            {loading ? <p>{t('common.loading', { defaultValue: 'Loading…' })}</p> : null}
            {!loading && !entries.length ? <p>{t('workspace.emptyDirectory', { defaultValue: 'This directory is empty.' })}</p> : null}
            <ul>
              {entries.filter((entry) => entry.kind === 'file' || entry.kind === 'directory').map((entry) => (
                <li key={entry.path}>
                  <button
                    type="button"
                    onClick={() => entry.kind === 'directory'
                      ? void openDirectory(entry.path)
                      : selectFile(entry)}
                  >
                    <span aria-hidden="true">{entry.kind === 'directory' ? '▰' : '▤'}</span>
                    <span>{entry.name}</span>
                    <small>{entry.kind === 'directory'
                      ? t('workspace.kindDirectory', { defaultValue: 'Folder' })
                      : t('workspace.kindFile', { defaultValue: 'File' })}</small>
                  </button>
                </li>
              ))}
            </ul>
          </div>
        </div>
      </section>
    </div>
  )
}

function workspaceLocation(workspace: Workspace): PickerLocation {
  return {
    key: `workspace:${workspace.id}`,
    name: workspace.name,
    device: 'server',
    root: workspace.type === 'shared' && workspace.ref ? `/${workspace.ref}` : '.',
  }
}

function deviceLocation(device: Device): PickerLocation {
  return {
    key: `device:${device.id}`,
    name: device.name,
    device: device.name,
    deviceId: device.id,
    root: '.',
  }
}

function parentPath(path: string, root: string): string {
  if (path === root) return root
  const separator = path.lastIndexOf('/')
  if (separator < 0) return root
  const parent = path.slice(0, separator)
  return parent.length < root.length ? root : parent
}

function errorMessage(caught: unknown): string {
  return caught instanceof Error ? caught.message : 'Request failed'
}
