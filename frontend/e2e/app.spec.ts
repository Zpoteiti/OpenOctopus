import { expect, test, type Page } from '@playwright/test'

test.describe.configure({ mode: 'serial' })

test('admin can configure and use the browser application', async ({ page }) => {
  const runId = `${Date.now()}-${Math.floor(Math.random() * 1_000_000)}`
  const email = `frontend-e2e-${runId}@example.com`
  const deviceName = `e2e-laptop-${runId}`
  const filename = `e2e-note-${runId}.txt`
  const attachmentName = `e2e-context-${runId}.txt`
  const initialContents = 'Created by the OpenOctopus browser smoke test.\n'
  const editedContents = `${initialContents}Edited through the Workspace UI.\n`

  await page.goto('/register')

  await expect(page.getByRole('heading', { name: 'Create account' })).toBeVisible()
  const language = page.getByLabel('Language')
  await language.selectOption('zh-CN')
  await expect(page.getByRole('heading', { name: '创建账号' })).toBeVisible()
  await page.reload()
  await expect(page.getByRole('heading', { name: '创建账号' })).toBeVisible()
  await page.getByLabel('语言').selectOption('en')
  await expect(page.getByRole('heading', { name: 'Create account' })).toBeVisible()

  await page.getByLabel('Name').fill('Frontend E2E Administrator')
  await page.getByLabel('Email').fill(email)
  await page.getByLabel('Password').fill('openoctopus-e2e-password')
  await page.getByLabel('Admin Token (optional)').fill('dev-admin-token')
  await page.getByRole('button', { name: 'Create account' }).click()

  await expect(page).toHaveURL(/\/chat$/)
  await expect(page.getByRole('link', { name: 'Admin settings' })).toBeVisible()

  await page.goto('/account')
  await expectSinglePaneToFillWorkspace(page)
  await expect(page.locator('.page-scroll').getByLabel('Language')).toBeVisible()
  await expect(page.locator('.page-scroll').getByRole('button', { name: 'Theme: System' })).toBeVisible()
  await expect(page.getByRole('link', { name: 'Edit SOUL.md' })).toHaveAttribute('href', '/workspace?path=SOUL.md')
  await expect(page.getByRole('link', { name: 'Edit MEMORY.md' })).toHaveAttribute('href', '/workspace?path=MEMORY.md')
  await page.setViewportSize({ width: 390, height: 844 })
  await expect(page.getByRole('link', { name: 'Account' })).toBeVisible()
  await page.setViewportSize({ width: 1280, height: 720 })
  await page.getByRole('button', { name: 'Sign out' }).click()
  await expect(page).toHaveURL(/\/login$/)
  await page.getByLabel('Email').fill(email)
  await page.getByLabel('Password').fill('openoctopus-e2e-password')
  await page.getByRole('button', { name: 'Sign in' }).click()
  await expect(page).toHaveURL(/\/chat$/)

  await page.getByRole('link', { name: 'Admin settings' }).click()
  await expect(page.getByRole('heading', { name: 'System configuration' })).toBeVisible()
  await expectSinglePaneToFillWorkspace(page)
  await expect(page.getByLabel('Default SOUL')).toHaveValue("You are OpenOctopus, the user's personal AI partner.")
  await expectFieldsToAlign(page, 'Maximum concurrent requests', 'Maximum output tokens')
  await page.getByLabel('API Base URL').fill('http://127.0.0.1:18080')
  await page.getByLabel('API Key').fill('frontend-e2e-key')
  await page.getByLabel('Model').fill('openoctopus-e2e-model')
  await page.getByLabel('Context window').fill('131072')
  await page.getByLabel('Compaction headroom').fill('16000')
  await page.getByLabel('Maximum concurrent requests').fill('2')
  await page.getByLabel('Maximum output tokens').fill('1024')
  const providerSaved = page.waitForResponse((response) => (
    response.url().endsWith('/api/admin/config')
      && response.request().method() === 'PATCH'
  ))
  await page.getByRole('button', { name: 'Validate and save Provider' }).click()
  const providerResponse = await providerSaved
  expect(providerResponse.status()).toBe(200)
  await expect(providerResponse.json()).resolves.toMatchObject({
    llm_endpoint: 'http://127.0.0.1:18080',
    llm_api_key: '<redacted>',
    llm_model: 'openoctopus-e2e-model',
  })
  await expect(page.getByLabel('API Base URL')).toHaveValue('http://127.0.0.1:18080')

  await page.getByRole('link', { name: 'Devices' }).click()
  await expect(page.getByRole('heading', { name: 'Devices' })).toBeVisible()
  await expectSinglePaneToFillWorkspace(page)
  await page.getByRole('button', { name: 'Add device' }).click()
  await page.getByLabel('Device name').fill(deviceName)
  const deviceCreated = page.waitForResponse((response) => (
    response.url().endsWith('/api/devices')
      && response.request().method() === 'POST'
  ))
  await page.getByRole('button', { name: 'Create device' }).click()
  expect((await deviceCreated).status()).toBe(201)
  await expect(page.getByRole('heading', { name: `${deviceName} Device Token` })).toBeVisible()
  await expect(page.locator('.secret-once code')).toHaveText(/\S{20,}/)
  await page.getByRole('button', { name: 'I saved it' }).click()

  await page.getByRole('link', { name: 'Workspace', exact: true }).click()
  await expect(page).toHaveURL(/\/workspace$/)
  await page.getByLabel('Choose file').setInputFiles({
    name: filename,
    mimeType: 'text/plain',
    buffer: Buffer.from(initialContents),
  })
  await page.getByRole('button', { name: 'Upload file' }).click()
  await expect(page.getByRole('status')).toHaveText('File uploaded.')
  await page.getByRole('button', { name: new RegExp(filename) }).click()
  const editor = page.getByLabel('File content')
  await expect(editor).toHaveValue(initialContents)
  await editor.fill(editedContents)
  await page.getByRole('button', { name: 'Save file' }).click()
  await expect(page.getByRole('status')).toHaveText('File saved.')

  await page.reload()
  await page.getByRole('button', { name: new RegExp(filename) }).click()
  await expect(page.getByLabel('File content')).toHaveValue(editedContents)

  await page.getByRole('link', { name: 'Chat', exact: true }).click()
  await expect(page.getByRole('heading', { name: 'What would you like your agent to do?' })).toBeVisible()
  await expect(page.getByRole('textbox', { name: 'Message', exact: true })).toHaveCSS('border-radius', '8px')
  await page.locator('input.chat-file-input').setInputFiles({
    name: attachmentName,
    mimeType: 'text/plain',
    buffer: Buffer.from('Context supplied through a browser chat attachment.\n'),
  })
  await expect(page.getByText(attachmentName)).toBeVisible()
  await page.getByRole('textbox', { name: 'Message', exact: true }).fill('Reply using the configured test Provider.')
  await page.getByRole('button', { name: 'Send message' }).click()
  await expect(page.getByText('Smoke reply from test provider.')).toBeVisible({ timeout: 30_000 })
})

async function expectSinglePaneToFillWorkspace(page: Page): Promise<void> {
  const dimensions = await page.locator('.workspace').evaluate((workspace) => {
    const pane = workspace.querySelector(':scope > .page-scroll')
    if (!(pane instanceof HTMLElement)) throw new Error('single-pane route is missing page-scroll')
    return {
      paneHeight: pane.getBoundingClientRect().height,
      workspaceHeight: workspace.getBoundingClientRect().height,
    }
  })
  expect(dimensions.paneHeight).toBeGreaterThanOrEqual(dimensions.workspaceHeight - 1)
}

async function expectFieldsToAlign(page: Page, firstLabel: string, secondLabel: string): Promise<void> {
  const [first, second] = await Promise.all([
    page.getByLabel(firstLabel).boundingBox(),
    page.getByLabel(secondLabel).boundingBox(),
  ])
  expect(first).not.toBeNull()
  expect(second).not.toBeNull()
  expect(Math.abs(first!.y - second!.y)).toBeLessThanOrEqual(1)
  expect(Math.abs(first!.height - second!.height)).toBeLessThanOrEqual(1)
}
