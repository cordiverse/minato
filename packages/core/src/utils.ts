import { Intersect, isNullable } from 'cosmokit'
import { Eval } from './eval.ts'

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

type FlatWrap<S, A extends 0[], P extends string> = { [K in P]?: S }
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
    || value instanceof Date
}

const letters = 'abcdefghijklmnopqrstuvwxyz'

export function randomId() {
  return Array(8).fill(0).map(() => letters[Math.floor(Math.random() * letters.length)]).join('')
}

export function makeRegExp(source: string | RegExp) {
  return source instanceof RegExp ? source : new RegExp(source)
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

export function isEmpty(value: any) {
  if (isNullable(value)) return true
  if (typeof value !== 'object') return false
  for (const key in value) {
    if (!isEmpty(value[key])) return false
  }
  return true
}
