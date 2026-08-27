import type { components, paths } from './openapi'

export type User = components['schemas']['User']
export type Session = components['schemas']['Session']
export type Effort = components['schemas']['Effort']
export type AuthResponse =
  paths['/api/auth/login']['post']['responses'][200]['content']['application/json']
export type RegisterRequest =
  paths['/api/auth/register']['post']['requestBody']['content']['application/json']
export type LoginRequest =
  paths['/api/auth/login']['post']['requestBody']['content']['application/json']
export type Device = components['schemas']['Device']
export type DeviceConfig = components['schemas']['DeviceConfig']
export type DeviceConfigPatch = components['schemas']['DeviceConfigPatch']
export type McpServerConfig = components['schemas']['McpServerConfig']
export type DeviceMcpServerConfigView = components['schemas']['DeviceMcpServerConfigView']
export type AdminConfig = components['schemas']['AdminConfig']
export type AdminUser = components['schemas']['AdminUser']
export type ServerMcpResponse = components['schemas']['ServerMcpResponse']
export type ServerMcpServerConfig = components['schemas']['ServerMcpServerConfig']
export type ServerMcpServerConfigView = components['schemas']['ServerMcpServerConfigView']
