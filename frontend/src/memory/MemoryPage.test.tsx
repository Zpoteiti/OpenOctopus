import { act, render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, expect, it, vi } from 'vitest'

import i18n from '../i18n'
import { MemoryPage } from './MemoryPage'

const json = (body: unknown, status = 200) => new Response(JSON.stringify(body), {
  status, headers: { 'Content-Type': 'application/json' },
})

beforeEach(async () => { await i18n.changeLanguage('en') })
afterEach(() => { vi.unstubAllGlobals() })

it('keeps an unsaved draft when the agent updated the same note', async () => {
  let current = { path: 'MEMORY.md', content: 'Original', version: 'v1' }
  const writes: unknown[] = []
  vi.stubGlobal('fetch', vi.fn(async (input: string, init?: RequestInit) => {
    if (input === '/api/memory') return json({ paths: ['MEMORY.md'], next_offset: null })
    if (init?.method === 'PUT') {
      writes.push(JSON.parse(String(init.body)))
      current = { ...current, content: 'Agent update', version: 'v2' }
      return json({ code: 'workspace_file_changed', message: 'changed' }, 409)
    }
    return json(current)
  }))
  render(<MemoryPage />)
  const actor = userEvent.setup()
  const editor = await screen.findByRole('textbox', { name: 'Note content' })
  await actor.clear(editor)
  await actor.type(editor, 'My draft')
  await actor.click(screen.getByRole('button', { name: 'Save' }))
  expect(await screen.findByRole('alert')).toHaveTextContent('Your draft is kept here')
  expect(editor).toHaveValue('My draft')
  expect(writes).toEqual([{ content: 'My draft', expected_version: 'v1' }])
  await act(async () => { await i18n.changeLanguage('zh-CN') })
  expect(editor).toHaveValue('My draft')
  await act(async () => { await i18n.changeLanguage('en') })
  await actor.click(screen.getByRole('button', { name: 'Reload saved version' }))
  await waitFor(() => expect(editor).toHaveValue('Agent update'))
})
