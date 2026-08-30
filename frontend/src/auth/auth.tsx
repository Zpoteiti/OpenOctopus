import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { type FormEvent, type ReactNode, useState } from 'react'
import { Link, Navigate, Outlet, useLocation, useNavigate } from 'react-router-dom'
import { useTranslation } from 'react-i18next'

import { ApiError, apiJson } from '../api/client'
import type { AuthResponse, LoginRequest, RegisterRequest, User } from '../api/types'
import { ErrorNotice } from '../components/Page'
import { LanguageSelector } from '../i18n/LanguageSelector'
import { ThemeToggle } from '../theme/ThemeToggle'
import { AuthenticatedUserContext, useAuthenticatedUser } from './context'

const CURRENT_USER_KEY = ['current-user'] as const

function currentUserQuery() {
  return {
    queryKey: CURRENT_USER_KEY,
    queryFn: () => apiJson<User>('/api/me'),
    retry: false,
    staleTime: 30_000,
  }
}

export function RequireAuth(): ReactNode {
  const { t } = useTranslation()
  const location = useLocation()
  const query = useQuery(currentUserQuery())

  if (query.isPending) return <FullPageStatus label={t('auth.connecting')} />
  if (query.error instanceof ApiError && query.error.status === 401) {
    return <Navigate to="/login" replace state={{ from: location.pathname }} />
  }
  if (query.isError) {
    return (
      <FullPageStatus
        label={t('auth.unavailable')}
        action={<button onClick={() => void query.refetch()}>{t('common.retry')}</button>}
      />
    )
  }
  return (
    <AuthenticatedUserContext.Provider value={query.data}>
      <Outlet />
    </AuthenticatedUserContext.Provider>
  )
}

export function RequireAdmin(): ReactNode {
  const user = useAuthenticatedUser()
  return user.is_admin ? <Outlet /> : <Navigate to="/chat" replace />
}

export function AuthPage({ mode }: { mode: 'login' | 'register' }): ReactNode {
  const { t } = useTranslation()
  const navigate = useNavigate()
  const location = useLocation()
  const queryClient = useQueryClient()
  const currentUser = useQuery(currentUserQuery())
  const [adminNotice, setAdminNotice] = useState<string | null>(null)
  const isRegister = mode === 'register'
  const finishAuthentication = (result: AuthResponse): void => {
    queryClient.setQueryData(CURRENT_USER_KEY, result.user)
    const requested = (location.state as { from?: string } | null)?.from
    navigate(requested && requested.startsWith('/') ? requested : '/chat', { replace: true })
  }

  const auth = useMutation({
    mutationFn: async (body: LoginRequest | RegisterRequest) => {
      const endpoint = isRegister ? '/api/auth/register' : '/api/auth/login'
      return apiJson<AuthResponse>(endpoint, { method: 'POST', body: JSON.stringify(body) })
    },
    onSuccess: (result, variables) => {
      if (isRegister && 'admin_token' in variables && variables.admin_token && !result.user.is_admin) {
        setAdminNotice(t('auth.adminMismatch'))
        return
      }
      finishAuthentication(result)
    },
  })

  if (currentUser.isSuccess) return <Navigate to="/chat" replace />

  const submit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    setAdminNotice(null)
    const data = new FormData(event.currentTarget)
    const email = String(data.get('email') ?? '')
    const password = String(data.get('password') ?? '')
    if (isRegister) {
      const adminToken = String(data.get('admin_token') ?? '').trim()
      auth.mutate({
        email,
        password,
        name: String(data.get('name') ?? ''),
        ...(adminToken ? { admin_token: adminToken } : {}),
      })
    } else {
      auth.mutate({ email, password })
    }
  }

  return (
    <main className="auth-page">
      <div className="auth-theme"><LanguageSelector /><ThemeToggle compact /></div>
      <section className="auth-brand" aria-label={t('auth.introLabel')}>
        <Brand />
        <div className="auth-promise">
          <span className="eyebrow">{t('auth.eyebrow')}</span>
          <h1>{t('auth.hero')}</h1>
          <p>{t('auth.heroDescription')}</p>
        </div>
      </section>
      <section className="auth-panel">
        <div className="auth-card">
          <span className="auth-kicker">OPENOCTOPUS</span>
          <h2>{isRegister ? t('auth.createAccount') : t('auth.welcomeBack')}</h2>
          <p className="auth-subtitle">
            {isRegister ? t('auth.registerSubtitle') : t('auth.loginSubtitle')}
          </p>
          <form onSubmit={submit} className="auth-form">
            {isRegister ? (
              <label>
                {t('auth.name')}
                <input name="name" autoComplete="name" required />
              </label>
            ) : null}
            <label>
              {t('auth.email')}
              <input name="email" type="email" autoComplete="email" required />
            </label>
            <label>
              {t('auth.password')}
              <input
                name="password"
                type="password"
                minLength={isRegister ? 8 : undefined}
                autoComplete={isRegister ? 'new-password' : 'current-password'}
                required
              />
            </label>
            {isRegister ? (
              <label>
                {t('auth.adminToken')}
                <input
                  aria-label={t('auth.adminToken')}
                  name="admin_token"
                  type="password"
                  autoComplete="off"
                />
                <small>{t('auth.adminTokenHelp')}</small>
              </label>
            ) : null}
            {auth.error ? <ErrorNotice error={auth.error} /> : null}
            {adminNotice ? (
              <div className="form-notice" role="status">
                <p>{adminNotice}</p>
                <button
                  type="button"
                  className="secondary-button"
                  onClick={() => auth.data && finishAuthentication(auth.data)}
                >
                  {t('auth.continueAsMember')}
                </button>
              </div>
            ) : null}
            <button className="primary-button auth-submit" disabled={auth.isPending || Boolean(adminNotice)}>
              {auth.isPending ? t('auth.submitting') : isRegister ? t('auth.createAccount') : t('auth.login')}
            </button>
          </form>
          <p className="auth-switch">
            {isRegister ? t('auth.hasAccount') : t('auth.noAccount')}{' '}
            <Link to={isRegister ? '/login' : '/register'}>
              {isRegister ? t('auth.backToLogin') : t('auth.registerNow')}
            </Link>
          </p>
        </div>
      </section>
    </main>
  )
}

export function Brand(): ReactNode {
  const { t } = useTranslation()
  return (
    <div className="brand">
      <span className="brand-symbol" aria-hidden="true">oo</span>
      <span>
        <strong className="brand-name">OpenOctopus</strong>
        <small className="brand-caption">{t('brand.caption')}</small>
      </span>
    </div>
  )
}

function FullPageStatus({ label, action }: { label: string; action?: ReactNode }): ReactNode {
  return (
    <main className="full-page-status">
      <Brand />
      <p>{label}</p>
      {action}
    </main>
  )
}
