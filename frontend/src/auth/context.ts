import { createContext, useContext } from 'react'

import type { User } from '../api/types'

export const AuthenticatedUserContext = createContext<User | null>(null)

export function useAuthenticatedUser(): User {
  const user = useContext(AuthenticatedUserContext)
  if (!user) throw new Error('Authenticated user is unavailable')
  return user
}
