import { render, screen, within } from '@testing-library/react'
import { describe, expect, it } from 'vitest'

import { ApiError } from '../api/client'
import { ErrorNotice } from './Page'

describe('ErrorNotice', () => {
  it('shows a stable API error code after the user-facing message', () => {
    render(<ErrorNotice error={new ApiError(409, 'config_conflict', 'Configuration changed')} />)

    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('Configuration changed config_conflict')
    expect(within(alert).getByText('config_conflict')).toHaveProperty('tagName', 'CODE')
  })

  it('does not repeat a code already present in the message', () => {
    render(<ErrorNotice error={new ApiError(409, 'config_conflict', 'Configuration changed (config_conflict)')} />)

    const alert = screen.getByRole('alert')
    expect(alert).toHaveTextContent('Configuration changed (config_conflict)')
    expect(alert.textContent?.match(/config_conflict/g)).toHaveLength(1)
  })
})
