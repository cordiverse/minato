import { Database } from './database'

export * from './database'
export * from './driver'
export * from './error'
export * from './eval'
export * from './model'
export * from './query'
export * from './selection'
export * from './utils'

declare module 'cordis' {
  interface Events {
    'minato/model'(name: string): void
  }

  interface Context {
    database: Database<Tables>
    model: Database<Tables>
  }
}

export interface Tables {}

export default Database
