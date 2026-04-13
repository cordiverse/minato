import { Database } from './database.ts'

export * from './database.ts'
export * from './driver.ts'
export * from './error.ts'
export * from './eval.ts'
export * from './model.ts'
export * from './query.ts'
export * from './selection.ts'
export * from './type.ts'
export * from './utils.ts'

declare module 'cordis' {
  interface Events {
    'minato/model'(name: string): void
  }

  interface Context {
    model: Database
    database: Database
  }
}

export interface Types {}

export interface Tables {}

export default Database
