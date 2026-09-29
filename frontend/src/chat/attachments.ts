import type { ClipboardEvent } from 'react'

import type { MessageAttachmentRef } from './model'

export interface DraftAttachment {
  id: string
  name: string
  source: string
  status: 'uploading' | 'ready' | 'failed'
  file?: File
  ref?: MessageAttachmentRef
}

export function attachmentFilename(path: string): string {
  const parts = path.split('/').filter(Boolean)
  return parts.at(-1) ?? path
}

export function canSubmitDraftAttachment(attachment: DraftAttachment): boolean {
  return Boolean(attachment.ref || attachment.file)
}

export function clipboardImageFiles(event: ClipboardEvent<HTMLTextAreaElement>): File[] {
  return Array.from(event.clipboardData.items)
    .filter((item) => item.kind === 'file' && item.type.startsWith('image/'))
    .flatMap((item, index) => {
      const file = item.getAsFile()
      if (!file) return []
      if (file.name) return [file]
      return [new File([file], pastedImageFilename(file.type, index), {
        type: file.type,
        lastModified: file.lastModified,
      })]
    })
}

function pastedImageFilename(type: string, index: number): string {
  const subtype = type.split('/')[1]?.split('+')[0]
  const extension = subtype === 'jpeg'
    ? 'jpg'
    : subtype && /^[a-z0-9]+$/i.test(subtype) ? subtype : 'bin'
  return `pasted-image${index ? `-${index + 1}` : ''}.${extension}`
}

export function attachmentKey(ref: MessageAttachmentRef): string {
  return `${'device_id' in ref ? ref.device_id : 'server'}:${ref.path}`
}
