import { defineConfig, devices } from '@playwright/test'

const inheritedEnvironment = Object.fromEntries(
  Object.entries(process.env).filter(
    (entry): entry is [string, string] => (
      entry[1] !== undefined && !entry[0].startsWith('OPENOCTOPUS_')
    ),
  ),
)

const serverEnvironment = {
  ...inheritedEnvironment,
  OPENOCTOPUS_DATABASE_URL: process.env.OO_E2E_DATABASE_URL
    ?? 'postgresql+asyncpg://openoctopus:octopus@127.0.0.1:5432/openoctopus',
  OPENOCTOPUS_DATABASE_POOL_SIZE: '5',
  OPENOCTOPUS_DATABASE_MAX_OVERFLOW: '10',
  OPENOCTOPUS_DATABASE_POOL_TIMEOUT: '30',
  OPENOCTOPUS_DATABASE_POOL_PRE_PING: 'true',
  OPENOCTOPUS_HOST: '127.0.0.1',
  OPENOCTOPUS_PORT: '8080',
  OPENOCTOPUS_JWT_SECRET: 'openoctopus-frontend-e2e-secret-for-tests',
  OPENOCTOPUS_COOKIE_SECURE: 'false',
  OPENOCTOPUS_ADMIN_TOKEN: 'dev-admin-token',
  OPENOCTOPUS_OBJECT_STORAGE_ENDPOINT: process.env.OO_E2E_OBJECT_STORAGE_ENDPOINT
    ?? 'http://127.0.0.1:9000',
  OPENOCTOPUS_OBJECT_STORAGE_BUCKET: 'openoctopus',
  OPENOCTOPUS_OBJECT_STORAGE_REGION: 'us-east-1',
  OPENOCTOPUS_OBJECT_STORAGE_ACCESS_KEY: process.env.OO_E2E_OBJECT_STORAGE_ACCESS_KEY
    ?? 'openoctopus',
  OPENOCTOPUS_OBJECT_STORAGE_SECRET_KEY: process.env.OO_E2E_OBJECT_STORAGE_SECRET_KEY
    ?? 'change-me-object-storage-secret',
  OPENOCTOPUS_OBJECT_STORAGE_MAX_CONNECTIONS: '32',
  OPENOCTOPUS_REST_UPLOAD_MAX_CONCURRENCY: '8',
  OPENOCTOPUS_REST_DOWNLOAD_MAX_CONCURRENCY: '16',
  OPENOCTOPUS_REST_TRANSFER_MAX_CONCURRENCY_PER_USER: '2',
  OPENOCTOPUS_REST_TRANSFER_QUEUE_TIMEOUT_SECONDS: '5',
  OPENOCTOPUS_REST_TRANSFER_IDLE_TIMEOUT_SECONDS: '30',
  OPENOCTOPUS_CONTENT_CONVERSION_MEMORY_MB: '1024',
  OPENOCTOPUS_CONTENT_CONVERSION_TIMEOUT_SECONDS: '20',
  OPENOCTOPUS_CONTENT_CONVERSION_MAX_CONCURRENCY: '2',
  OPENOCTOPUS_CONTENT_CONVERSION_QUEUE_TIMEOUT_SECONDS: '5',
  OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY: '16',
  OPENOCTOPUS_WEB_FETCH_MAX_CONCURRENCY_PER_USER: '2',
  OPENOCTOPUS_WEB_FETCH_QUEUE_TIMEOUT_SECONDS: '5',
  OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY: '32',
  OPENOCTOPUS_CHAT_CONTEXT_MAX_CONCURRENCY_PER_USER: '2',
  OPENOCTOPUS_CHAT_CONTEXT_QUEUE_TIMEOUT_SECONDS: '30',
  OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX: '4096',
  OPENOCTOPUS_DEVICE_PENDING_CALLS_MAX_PER_USER: '64',
  OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX: '268435456',
  OPENOCTOPUS_DEVICE_PENDING_BYTES_MAX_PER_USER: '33554432',
  OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY: '32',
  OPENOCTOPUS_DEVICE_TRANSFER_MAX_CONCURRENCY_PER_USER: '2',
  OPENOCTOPUS_DEVICE_TRANSFER_QUEUE_TIMEOUT_SECONDS: '5',
  OPENOCTOPUS_DEVICE_TRANSFER_IDLE_TIMEOUT_SECONDS: '30',
  OPENOCTOPUS_WORKSPACE_DELETION_PURGE_TIMEOUT_SECONDS: '300',
  OPENOCTOPUS_WORKSPACE_DELETION_SHUTDOWN_GRACE_SECONDS: '5',
}

export default defineConfig({
  testDir: './e2e',
  outputDir: './test-results',
  fullyParallel: false,
  workers: 1,
  timeout: 60_000,
  retries: process.env.CI ? 1 : 0,
  reporter: process.env.CI ? [['html', { open: 'never' }], ['list']] : 'list',
  use: {
    baseURL: 'http://127.0.0.1:8080',
    trace: 'retain-on-failure',
    screenshot: 'only-on-failure',
    video: process.env.CI ? 'retain-on-failure' : 'off',
    ...devices['Desktop Chrome'],
    launchOptions: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE
      ? { executablePath: process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE }
      : undefined,
  },
  webServer: [
    {
      command: 'python e2e/provider_stub.py',
      url: 'http://127.0.0.1:18080/v1/models',
      reuseExistingServer: !process.env.CI,
      timeout: 30_000,
    },
    {
      command: 'python e2e/server_runner.py',
      url: 'http://127.0.0.1:8080/health',
      env: serverEnvironment,
      reuseExistingServer: !process.env.CI,
      timeout: 90_000,
    },
  ],
})
