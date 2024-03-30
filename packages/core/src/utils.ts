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

export function unravel(source: object) {
  const result = {}
  for (const key in source) {
    let node = result
    const segments = key.split('.').reverse()
    for (let index = segments.length - 1; index > 0; index--) {
      const segment = segments[index]
      node = node[segment] ??= {}
    }
    node[segments[0]] = source[key]
  }
  return result
}

export function clone<T>(source: T): T
export function clone(source: any) {
  if (!source || typeof source !== 'object') return source
  if (isUint8Array(source)) return (hasGlobalBuffer && Buffer.isBuffer(source)) ? Buffer.copyBytesFrom(source) : source.slice()
  if (Array.isArray(source)) return source.map(clone)
  if (is('Date', source)) return new Date(source.valueOf())
  if (is('RegExp', source)) return new RegExp(source.source, source.flags)
  return mapValues(source, clone)
}

const hasGlobalBuffer = typeof Buffer === 'function' && Buffer.prototype?._isBuffer !== true

export function isUint8Array(value: any): value is Uint8Array {
  const stringTag = value?.[Symbol.toStringTag] ?? Object.prototype.toString.call(value)
  return (hasGlobalBuffer && Buffer.isBuffer(value))
    || ArrayBuffer.isView(value)
    || ['ArrayBuffer', 'SharedArrayBuffer', '[object ArrayBuffer]', '[object SharedArrayBuffer]'].includes(stringTag)
}

export function Uint8ArrayFromHex(source: string) {
  if (hasGlobalBuffer) return Buffer.from(source, 'hex')
  const hex = source.length % 2 === 0 ? source : source.slice(0, source.length - 1)
  const buffer: number[] = []
  for (let i = 0; i < hex.length; i += 2) {
    buffer.push(Number.parseInt(`${hex[i]}${hex[i + 1]}`, 16))
  }
  return Uint8Array.from(buffer)
}

export function Uint8ArrayToHex(source: Uint8Array) {
  return (hasGlobalBuffer) ? toLocalUint8Array(source).toString('hex')
    : Array.from(toLocalUint8Array(source), byte => byte.toString(16).padStart(2, '0')).join('')
}

export function Uint8ArrayFromBase64(source: string) {
  return (hasGlobalBuffer) ? Buffer.from(source, 'base64') : Uint8Array.from(atob(source), c => c.charCodeAt(0))
}

export function Uint8ArrayToBase64(source: Uint8Array) {
  return (hasGlobalBuffer) ? (source as Buffer).toString('base64') : btoa(Array.from(Uint16Array.from(source), b => String.fromCharCode(b)).join(''))
}

export function toLocalUint8Array(source: Uint8Array) {
  if (hasGlobalBuffer) {
    return Buffer.isBuffer(source) ? Buffer.from(source)
      : ArrayBuffer.isView(source) ? Buffer.from(source.buffer, source.byteOffset, source.byteLength)
        : Buffer.from(source)
  } else {
    const stringTag = source?.[Symbol.toStringTag] ?? Object.prototype.toString.call(source)
    return stringTag === 'Uint8Array' ? source
      : ArrayBuffer.isView(source) ? new Uint8Array(source.buffer.slice(source.byteOffset, source.byteOffset + source.byteLength))
        : new Uint8Array(source)
  }
}
