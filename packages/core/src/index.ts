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
    'model'(name: string): void
  }

  interface Context {
    [Database.Tables]: Database.Tables
    [Database.Types]: Database.Types
    database: Database<this>
    model: Database<this>
  }
}

export { Logger, Schema, Schema as z } from 'cordis'

export default Database
