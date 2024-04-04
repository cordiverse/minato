import { Intersect, is, mapValues } from 'cosmokit'
import { Eval } from './eval.ts'

export type Values<S> = S[keyof S]

export type Keys<O, T = any> = Values<{
  [K in keyof O]: O[K] extends T | undefined ? K : never
}> & string

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

export type Indexable = string | number
export type Comparable = string | number | boolean | Date

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

export function clone<T>(source: T): T
export function clone(source: any) {
  if (!source || typeof source !== 'object') return source
  if (is('ArrayBuffer', source)) return source.slice(0)
  if (Array.isArray(source)) return source.map(clone)
  if (is('Date', source)) return new Date(source.valueOf())
  if (is('RegExp', source)) return new RegExp(source.source, source.flags)
  return mapValues(source, clone)
}

export function toArrayBuffer(source: ArrayBuffer | ArrayBufferView): ArrayBuffer {
  return ArrayBuffer.isView(source) ? source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength) : source
}

export function hexToArrayBuffer(source: string): ArrayBuffer {
  const buffer: number[] = []
  for (let i = 0; i < source.length; i += 2) {
    buffer.push(Number.parseInt(source.substring(i, i + 2), 16))
  }
  return Uint8Array.from(buffer).buffer
}

export function arrayBufferToHex(source: ArrayBuffer): string {
  return Array.from(new Uint8Array(source), byte => byte.toString(16).padStart(2, '0')).join('')
}

export function base64ToArrayBuffer(source: string): ArrayBuffer {
  return Uint8Array.from(atob(source), c => c.charCodeAt(0)).buffer
}

export function arrayBufferToBase64(source: ArrayBuffer): string {
  return btoa(Array.from(new Uint8Array(source), b => String.fromCharCode(b)).join(''))
}
