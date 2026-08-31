import type { ReactNode } from 'react'
import { Navigate, Route, Routes } from 'react-router-dom'

import { AccountPage } from '../account/Account'
import { AdminMcpPage, AdminSettingsPage, AdminUsersPage } from '../admin/Admin'
import { AutomationsPage } from '../automations/Automations'
import { AuthPage, RequireAdmin, RequireAuth } from '../auth/auth'
import { useAuthenticatedUser } from '../auth/context'
import { DeviceDetailPage, DeviceListPage, DeviceMcpPage } from '../devices/Devices'
import { ChatPage } from '../chat'
import { AppShell } from '../layout/AppShell'
import { ThemeProvider } from '../theme/ThemeToggle'
import { WorkspacePage } from '../workspace/WorkspacePage'

export function AppRoutes(): ReactNode {
  return (
    <ThemeProvider>
      <Routes>
        <Route path="/login" element={<AuthPage mode="login" />} />
        <Route path="/register" element={<AuthPage mode="register" />} />
        <Route element={<RequireAuth />}>
          <Route element={<AppShell />}>
            <Route index element={<Navigate to="/chat" replace />} />
            <Route path="/chat" element={<ChatPage />} />
            <Route path="/chat/:sessionId" element={<ChatPage />} />
            <Route path="/workspace" element={<WorkspacePage />} />
            <Route path="/workspace/:workspaceRef" element={<WorkspacePage />} />
            <Route path="/devices" element={<DeviceListPage />} />
            <Route path="/devices/:name" element={<DeviceDetailPage />} />
            <Route path="/devices/:name/mcp" element={<DeviceMcpPage />} />
            <Route path="/automations" element={<AutomationsRoute />} />
            <Route path="/account" element={<AccountRoute />} />
            <Route element={<RequireAdmin />}>
              <Route path="/admin/settings" element={<AdminSettingsPage />} />
              <Route path="/admin/users" element={<AdminUsersPage />} />
              <Route path="/admin/mcp" element={<AdminMcpPage />} />
            </Route>
          </Route>
        </Route>
        <Route path="*" element={<Navigate to="/chat" replace />} />
      </Routes>
    </ThemeProvider>
  )
}

function AccountRoute(): ReactNode {
  return <AccountPage user={useAuthenticatedUser()} />
}

function AutomationsRoute(): ReactNode {
  return <AutomationsPage user={useAuthenticatedUser()} />
}
