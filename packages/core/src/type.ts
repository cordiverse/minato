import { Binary, defineProperty, isNullable, mapValues } from 'cosmokit'
import { Field } from './model.ts'
import { Eval, isEvalExpr } from './eval.ts'
import { isEmpty } from './utils.ts'
// import { Keys } from './utils.ts'

export interface Type<T = any, N = any> {
  [Type.kType]?: true
  // FIXME
  type: Field.Type<T> // | Keys<N, T> | Field.NewType<T>
  inner?: T extends (infer I)[] ? Type<I, N> : Field.Type<T> extends 'json' ? { [key in keyof T]: Type<T[key], N> } : never
  array?: boolean
  // For left joined unmatched result only
  ignoreNull?: boolean
}

export namespace Type {
  export const kType = Symbol.for('minato.type')

  export const Any: Type = fromField('expr')
  export const Boolean: Type<boolean> = fromField('boolean')
  export const Number: Type<number> = fromField('double')
  export const String: Type<string> = fromField('string')

  type Extract<T> =
    | T extends Type<infer I> ? I
    : T extends Field<infer I> ? I
    : T extends Field.Type<infer I> ? I
    : T extends Eval.Term<infer I> ? I
    : never

  export type Object<T = any> = Type<T>
  export const Object = <T extends any>(obj?: T): Object<{ [K in keyof T]: Extract<T> }> => defineProperty({
    type: 'json' as any,
    inner: globalThis.Object.keys(obj ?? {}).length ? mapValues(obj!, (value) => isType(value) ? value : fromField(value)) as any : undefined,
  }, kType, true)

  export type Array<T = any> = Type<T[]>
  export const Array = <T>(type?: Type<T>): Type.Array<T> => defineProperty({
    type: 'json',
    inner: type,
    array: true,
  }, kType, true)

  export function fromPrimitive<T>(value: T): Type<T> {
    if (isNullable(value)) return fromField('expr' as any)
    else if (typeof value === 'number') return Number as any
    else if (typeof value === 'string') return String as any
    else if (typeof value === 'boolean') return Boolean as any
    else if (typeof value === 'bigint') return fromField('bigint' as any)
    else if (value instanceof Date) return fromField('timestamp' as any)
    else if (Binary.is(value)) return fromField('binary' as any)
    else if (globalThis.Array.isArray(value)) return Array(value.length ? fromPrimitive(value[0]) : undefined) as any
    else if (typeof value === 'object') return fromField('json' as any)
    throw new TypeError(`invalid primitive: ${value}`)
  }

  // FIXME: Type | Field<T> | Field.Type<T> | Keys<N, T> | Field.NewType<T>
  export function fromField<T, N>(field: any): Type<T, N> {
    if (isType(field)) return field
    else if (field === 'array') return Array() as never
    else if (field === 'object') return Object() as never
    else if (typeof field === 'string') return defineProperty({ type: field }, kType, true) as never
    else if (field.type) return field.type
    else if (field.expr?.[kType]) return field.expr[kType]
    throw new TypeError(`invalid field: ${field}`)
  }

  export function fromTerm<T>(value: Eval.Term<T>, initial?: Type): Type<T> {
    if (isEvalExpr(value)) return value[kType] ?? initial ?? fromField('expr' as any)
    else return fromPrimitive(value as T)
  }

  export function fromTerms(values: Eval.Term<any>[], initial?: Type): Type {
    return values.map((x) => fromTerm(x)).find((type) => type.type !== 'expr') ?? initial ?? fromField('expr')
  }

  export function isType(value: any): value is Type {
    return value?.[kType] === true
  }

  export function isArray(type?: Type) {
    return (type?.type === 'json') && type?.array
  }

  export function getInner(type?: Type, key?: string): Type | undefined {
    if (!type?.inner) return
    if (isArray(type)) return type.inner
    if (isNullable(key)) return
    if (type.inner[key]) return type.inner[key]
    if (key.includes('.')) return key.split('.').reduce((t, k) => getInner(t, k), type)
    const fields = globalThis.Object.entries(type.inner)
      .filter(([k]) => k.startsWith(`${key}.`))
      .map(([k, v]) => [k.slice(key.length + 1), v])
    return fields.length ? Object(globalThis.Object.fromEntries(fields)) : undefined
  }

  export function transform(value: any, type: Type, callback: (value: any, type?: Type) => any) {
    if (!isNullable(value) && type?.inner) {
      if (Type.isArray(type)) {
        return (value as any[]).map(x => callback(x, Type.getInner(type))).filter(x => !type.ignoreNull || !isEmpty(x))
      } else {
        if (type.ignoreNull && isEmpty(value)) return null
        return mapValues(value, (x, k) => callback(x, Type.getInner(type, k)))
      }
    }
    return value
  }
}
