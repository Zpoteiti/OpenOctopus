import { expect, test } from '@playwright/test'

const user = {
  id: '11111111-1111-4111-8111-111111111111',
  email: 'browser@example.com',
  name: 'Browser User',
  is_admin: true,
  timezone: 'Asia/Shanghai',
  created_at: '2026-09-29T00:00:00Z',
}

const dreamRun = {
  id: 'dream-browser-run',
  started_at: '2026-09-28T16:00:00Z',
  finished_at: '2026-09-28T16:01:00Z',
  status: 'updated',
  message_count: 3,
  error: null,
  restored_at: null,
}

const dreamDetail = {
  ...dreamRun,
  before: 'Remember the old preference.',
  after: 'Remember the updated preference.',
}

test('built-in skills stay read-only and Jev recovery restores Dream memory through mocked APIs', async ({ page }) => {
  let jevState = 'unreachable'
  let dreamRestored = false
  await page.route('**/api/**', async (route) => {
    const request = route.request()
    const url = new URL(request.url())
    const method = request.method()
    const json = (body: unknown, status = 200) => route.fulfill({ status, contentType: 'application/json', body: JSON.stringify(body) })

    if (url.pathname === '/api/me' && method === 'GET') return json(user)
    if (url.pathname === '/api/sessions' || url.pathname.startsWith('/api/sessions/')) {
      return json({ code: 'session_not_found', message: 'No saved session.' }, 404)
    }
    if (url.pathname === '/api/workspaces' && method === 'GET') {
      return json({ items: [{ id: user.id, name: 'Personal', type: 'personal', quota_bytes: 500000000, bytes_used: 0, locked: false }], limit: 200, offset: 0, next_offset: null, truncated: false })
    }
    if (url.pathname === '/api/devices' && method === 'GET') return json([])
    if (url.pathname.startsWith('/api/workspace/list/')) {
      if (url.pathname === '/api/workspace/list//builtin/skills') {
        return json({ items: [{ name: 'creating-skills', path: '/builtin/skills/creating-skills', kind: 'directory', size: 0 }], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url.pathname === '/api/workspace/list//builtin/skills/creating-skills') {
        return json({ items: [{ name: 'SKILL.md', path: '/builtin/skills/creating-skills/SKILL.md', kind: 'file', size: 31 }], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
      if (url.pathname === '/api/workspace/list/.') {
        return json({ items: [{ name: 'MEMORY.md', path: 'MEMORY.md', kind: 'file', size: 13 }], limit: 200, offset: 0, next_offset: null, truncated: false })
      }
    }
    if (url.pathname === '/api/workspace/files//builtin/skills/creating-skills/SKILL.md' && method === 'GET') {
      return route.fulfill({ status: 200, contentType: 'text/plain', headers: { ETag: '"built-in-version"' }, body: '# Create skills\nRead this guide.' })
    }
    if (url.pathname === '/api/workspace/files/HEARTBEAT.md') {
      return json({ code: 'workspace_not_found', message: 'Not found.' }, 404)
    }
    if (url.pathname === '/api/admin/config' && method === 'GET') {
      return json({
        quota_bytes: 500000000, shared_workspace_quota_bytes: 500000000,
        llm_endpoint: null, llm_api_key: null, llm_model: null, llm_max_context_tokens: null,
        llm_compaction_threshold_tokens: null, llm_max_concurrent_requests: null,
        llm_max_output_tokens: 16384, default_soul: 'Default SOUL', web_fetch_denylist: [],
        jev_endpoint: 'https://jev.example', jev_api_key: '<redacted>',
        jev_status: { state: jevState, checked_at: '2026-09-29T00:00:00Z' },
      })
    }
    if (url.pathname === '/api/admin/config/jev/check' && method === 'POST') {
      jevState = 'available'
      return json({ state: jevState, checked_at: '2026-09-29T01:00:00Z' })
    }
    if (url.pathname === '/api/cron' && method === 'GET') return json({ items: [], next_offset: null })
    if (url.pathname === '/api/dream' && method === 'GET') {
      return json({
        availability: { state: 'available', checked_at: '2026-09-29T00:00:00Z' },
        next_run_at: '2026-09-29T16:00:00Z',
        items: [{ ...dreamRun, ...(dreamRestored ? { status: 'restored', restored_at: '2026-09-29T01:00:00Z' } : {}) }],
        next_offset: null,
      })
    }
    if (url.pathname === `/api/dream/${dreamRun.id}` && method === 'GET') return json(dreamDetail)
    if (url.pathname === `/api/dream/${dreamRun.id}/restore` && method === 'POST') {
      dreamRestored = true
      return json({ ...dreamDetail, status: 'restored', restored_at: '2026-09-29T01:00:00Z' })
    }

    return json({ code: 'e2e_unmocked', message: `${method} ${url.pathname} was not mocked.` }, 404)
  })

  await page.goto('/workspace')
  await page.getByRole('button', { name: 'Built-in skills' }).click()
  await page.getByRole('button', { name: /creating-skills.*Folder/ }).click()
  await page.getByRole('button', { name: /SKILL\.md.*File/ }).click()
  await expect(page.getByLabel('File content')).toHaveValue('# Create skills\nRead this guide.')
  await expect(page.getByRole('button', { name: 'New shared Workspace' })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'New file' })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Save file' })).toHaveCount(0)
  await expect(page.getByRole('button', { name: 'Delete file' })).toHaveCount(0)

  await page.getByRole('link', { name: 'Admin settings' }).click()
  await expect(page.getByText('Dream is not available')).toBeVisible()
  await expect(page.getByRole('status').filter({ hasText: /Dream is not available/ })).toContainText('Cannot reach Jev. Check the endpoint and connection.')
  await page.getByRole('button', { name: 'Check Jev connection' }).click()
  await expect(page.getByText('Dream is not available')).toHaveCount(0)
  await expect(page.getByText(/Jev is ready for Dream and Heartbeat decisions/)).toBeVisible()

  await page.getByRole('link', { name: 'Automations' }).click()
  const dreamRow = page.locator('.automation-row').filter({ hasText: 'Memory updated' })
  await dreamRow.getByRole('button', { name: 'Change details' }).click()
  await dreamRow.locator('summary').filter({ hasText: 'Before' }).click()
  await dreamRow.locator('summary').filter({ hasText: 'After' }).click()
  await expect(dreamRow.getByText('Remember the old preference.')).toBeVisible()
  await expect(dreamRow.getByText('Remember the updated preference.')).toBeVisible()
  await dreamRow.getByRole('button', { name: 'Restore previous memory' }).click()
  const restoredRow = page.locator('.automation-row').filter({ hasText: 'Restored' })
  await expect(restoredRow.getByText('Restored:')).toBeVisible()
  await expect(restoredRow.getByRole('button', { name: 'Restore previous memory' })).toHaveCount(0)
})
