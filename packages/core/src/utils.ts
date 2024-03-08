import { Intersect, is, mapValues } from 'cosmokit'
import { Eval } from './eval.ts'

export type Values<S> = S[keyof S]

export type Keys<O, T = any> = Values<{
  [K in keyof O]: O[K] extends T | undefined ? K : never
}> & string

export type Atomic = number | string | boolean | bigint | symbol | Date
export type Indexable = string | number
export type Comparable = string | number | boolean | Date

type FlatWrap<S, T, P extends string> = { [K in P]?: S }
  // rule out atomic / recursive types
  | (S extends Atomic | T ? never
  // rule out array types
  : S extends any[] ? never
  // rule out dict / infinite types
  : string extends keyof S ? never
  : FlatMap<S, T, `${P}.`>)

type FlatMap<S, T = never, P extends string = ''> = Values<{
  [K in keyof S & string as `${P}${K}`]: FlatWrap<S[K], S | T, `${P}${K}`>
}>

export type Flatten<S> = Intersect<FlatMap<S>>

export type Row<S> = {
  [K in keyof S]-?: Row.Cell<NonNullable<S[K]>>
}

export namespace Row {
  export type Cell<T> = Eval.Expr<T, false> & (T extends Comparable ? {} : Row<T>)
  export type Computed<S, T> = T | ((row: Row<S>) => T)
}

export function isComparable(value: any): value is Comparable {
  return typeof value === 'string'
    || typeof value === 'number'
    || typeof value === 'boolean'
    || value instanceof Date
}

const letters = 'abcdefghijklmnopqrstuvwxyz'

export function randomId() {
  return Array(8).fill(0).map(() => letters[Math.floor(Math.random() * letters.length)]).join('')
}

export function makeRegExp(source: string | RegExp) {
  return source instanceof RegExp ? source : new RegExp(source)
}

export function clone<T>(source: T): T
export function clone(source: any) {
  if (!source || typeof source !== 'object') return source
  if (Buffer.isBuffer(source)) return Buffer.copyBytesFrom(source)
  if (Array.isArray(source)) return source.map(clone)
  if (is('Date', source)) return new Date(source.valueOf())
  if (is('RegExp', source)) return new RegExp(source.source, source.flags)
  return mapValues(source, clone)
}
