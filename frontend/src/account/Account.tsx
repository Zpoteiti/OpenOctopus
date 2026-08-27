import { useMutation, useQueryClient } from '@tanstack/react-query'
import { type FormEvent, type ReactNode, useRef, useState } from 'react'
import { useTranslation } from 'react-i18next'
import { Link, useNavigate } from 'react-router-dom'

import { apiJson } from '../api/client'
import type { User } from '../api/types'
import { Card, ErrorNotice, PageHeader } from '../components/Page'
import { LanguageSelector } from '../i18n/LanguageSelector'
import { ThemeToggle } from '../theme/ThemeToggle'

export function AccountPage({ user }: { user: User }): ReactNode {
  const { t } = useTranslation()
  const client = useQueryClient()
  const navigate = useNavigate()
  const passwordRef = useRef<HTMLInputElement>(null)
  const [confirmDelete, setConfirmDelete] = useState(false)
  const update = useMutation({
    mutationFn: (body: Record<string, string>) => apiJson<User>('/api/me', { method: 'PATCH', body: JSON.stringify(body) }),
    onSuccess: (result) => {
      client.setQueryData(['current-user'], result)
      if (passwordRef.current) passwordRef.current.value = ''
    },
  })
  const logout = useMutation({
    mutationFn: () => apiJson('/api/auth/logout', { method: 'POST' }),
    onSuccess: () => {
      client.clear()
      navigate('/login', { replace: true })
    },
  })
  const remove = useMutation({
    mutationFn: () => apiJson('/api/me', { method: 'DELETE' }),
    onSuccess: () => {
      client.clear()
      navigate('/register', { replace: true })
    },
  })
  const submit = (event: FormEvent<HTMLFormElement>): void => {
    event.preventDefault()
    const data = new FormData(event.currentTarget)
    const password = String(data.get('password') ?? '')
    update.mutate({
      name: String(data.get('name') ?? ''),
      email: String(data.get('email') ?? ''),
      ...(password ? { password } : {}),
    })
  }

  return (
    <div className="page-scroll">
      <PageHeader eyebrow={t('account.eyebrow')} title={t('account.title')} description={t('account.description')} />
      <ErrorNotice error={update.error ?? logout.error ?? remove.error} />
      <Card title={t('account.profile')}>
        <form className="form-grid" onSubmit={submit}>
          <label>{t('auth.name')}<input name="name" defaultValue={user.name} required /></label>
          <label>{t('auth.email')}<input name="email" type="email" defaultValue={user.email} required /></label>
          <label className="full-row">{t('account.newPassword')}<input ref={passwordRef} name="password" type="password" minLength={8} autoComplete="new-password" placeholder={t('account.passwordPlaceholder')} /></label>
          <div className="form-actions full-row"><button className="primary-button" disabled={update.isPending}>{t('account.saveProfile')}</button></div>
        </form>
      </Card>
      <Card title={t('account.preferences')} description={t('account.preferencesDescription')}>
        <div className="account-preferences">
          <div className="preference-setting">
            <div className="preference-setting-copy">
              <strong>{t('language.label')}</strong>
              <small>{t('account.languageDescription')}</small>
            </div>
            <LanguageSelector />
          </div>
          <div className="preference-setting">
            <div className="preference-setting-copy">
              <strong>{t('account.appearance')}</strong>
              <small>{t('account.appearanceDescription')}</small>
            </div>
            <ThemeToggle />
          </div>
        </div>
      </Card>
      <Card title={t('account.agentFiles')} description={t('account.agentFilesDescription')}>
        <div className="form-actions">
          <Link className="secondary-button" to="/workspace?path=SOUL.md">{t('account.editSoul')}</Link>
          <Link className="secondary-button" to="/workspace?path=MEMORY.md">{t('account.editMemory')}</Link>
        </div>
      </Card>
      <Card title={t('account.session')}><button className="secondary-button" onClick={() => logout.mutate()} disabled={logout.isPending}>{t('account.logout')}</button></Card>
      <Card title={t('account.deleteTitle')} description={t('account.deleteDescription')} tone="danger">
        {confirmDelete ? <div className="danger-actions"><span>{t('account.irreversible')}</span><button className="danger-button" onClick={() => remove.mutate()} disabled={remove.isPending}>{t('account.confirmDelete')}</button></div> : <button className="danger-button" onClick={() => setConfirmDelete(true)}>{t('account.deleteMine')}</button>}
      </Card>
    </div>
  )
}
