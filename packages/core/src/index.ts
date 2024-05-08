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
    [Types]: Types
    [Tables]: Tables
    [Context.Minato]: Context.Minato<this>
    [Context.Database]: Context.Database<this>
    model: Database<this[typeof Tables], this[typeof Types], this> & this[typeof Context.Minato]
    database: Database<this[typeof Tables], this[typeof Types], this> & this[typeof Context.Database]
  }

  namespace Context {
    const Minato: unique symbol
    const Database: unique symbol
    // https://github.com/typescript-eslint/typescript-eslint/issues/6720
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    interface Minato<C extends Context = Context> {}
    // https://github.com/typescript-eslint/typescript-eslint/issues/6720
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    interface Database<C extends Context = Context> {}
  }
}

export { Logger, Schema, Schema as z } from 'cordis'

export const Types = Symbol('minato.types')
export interface Types {}

export const Tables = Symbol('minato.tables')
export interface Tables {}

export default Database
