import { Binary, Intersect, isNullable } from 'cosmokit'
import { Eval, isEvalExpr } from './eval.ts'

export type Values<S> = S[keyof S]

export type Keys<O, T = any> = Values<{
  [P in keyof O]: O[P] extends T | undefined ? P : never
}> & string

export type FlatKeys<O, T = any> = Keys<Flatten<O>, T>

export type FlatPick<O, K extends FlatKeys<O>> = {
  [P in string & keyof O as K extends P | `${P}.${any}` ? P : never]:
    | P extends K
    ? O[P]
    : FlatPick<O[P], Extract<K extends `${any}.${infer R}` ? R : never, FlatKeys<O[P]>>>
}

export type DeepPartial<T> =
  | T extends Values<AtomicTypes> ? T
  : T extends (infer U)[] ? DeepPartial<U>[]
  : T extends object ? { [K in keyof T]?: DeepPartial<T[K]> }
  : T

export interface AtomicTypes {
  Number: number
  String: string
  Boolean: boolean
  BigInt: bigint
  Symbol: symbol
  Date: Date
  RegExp: RegExp
  Function: Function
  ArrayBuffer: ArrayBuffer
  SharedArrayBuffer: SharedArrayBuffer
}

export type Indexable = string | number | bigint
export type Comparable = string | number | boolean | bigint | Date

type FlatWrap<S, A extends 0[], P extends string> = { [K in P]: S }
  // rule out atomic types
  | (S extends Values<AtomicTypes> ? never
  // rule out array types
  : S extends any[] ? never
  // check recursion depth
  // rule out dict / infinite types
  : string extends keyof S ? never
  : A extends [0, ...infer R extends 0[]] ? FlatMap<S, R, `${P}.`>
  : never)

type FlatMap<S, T extends 0[], P extends string = ''> = Values<{
  [K in keyof S & string as `${P}${K}`]: FlatWrap<S[K], T, `${P}${K}`>
}>

type Sequence<N extends number, A extends 0[] = []> = A['length'] extends N ? A : Sequence<N, [0, ...A]>

export type Flatten<S, D extends number = 5> = Intersect<FlatMap<S, Sequence<D>>>

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
    || typeof value === 'bigint'
    || value instanceof Date
}

export function isFlat(value: any): value is Values<AtomicTypes> {
  return !value
    || typeof value !== 'object'
    || isEvalExpr(value)
    || Object.keys(value).length === 0
    || Array.isArray(value)
    || value instanceof Date
    || value instanceof RegExp
    || Binary.isSource(value)
}

const letters = 'abcdefghijklmnopqrstuvwxyz'

export function randomId() {
  return Array(8).fill(0).map(() => letters[Math.floor(Math.random() * letters.length)]).join('')
}

export interface RegExpLike {
  source: string
  flags?: string
}

export function makeRegExp(source: string | RegExpLike, flags?: string) {
  return (source instanceof RegExp && !flags) ? source : new RegExp((source as any).source ?? source, flags ?? (source as any).flags)
}

export function unravel(source: object, init?: (value) => any) {
  const result = {}
  for (const key in source) {
    let node = result
    const segments = key.split('.').reverse()
    for (let index = segments.length - 1; index > 0; index--) {
      const segment = segments[index]
      node = node[segment] ??= {}
      if (init) node = init(node)
    }
    node[segments[0]] = source[key]
  }
  return result
}

export function flatten(source: object, prefix = '', ignore: (value: any) => boolean = isFlat) {
  const result = {}
  for (const key in source) {
    const value = source[key]
    if (ignore(value)) {
      result[`${prefix}${key}`] = value
    } else {
      Object.assign(result, flatten(value, `${prefix}${key}.`, ignore))
    }
  }
  return result
}

export function getCell(row: any, path: any): any {
  if (path in row) return row[path]
  if (path.includes('.')) {
    const index = path.indexOf('.')
    return getCell(row[path.slice(0, index)] ?? {}, path.slice(index + 1))
  } else {
    return row[path]
  }
}

export function isEmpty(value: any) {
  if (isNullable(value)) return true
  if (typeof value !== 'object') return false
  for (const key in value) {
    if (!isEmpty(value[key])) return false
  }
  return true
}
