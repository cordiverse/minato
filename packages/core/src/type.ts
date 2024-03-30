import { defineProperty, Dict, isNullable, mapValues } from 'cosmokit'
import { Field } from './model.ts'
import { Eval, isEvalExpr } from './eval.ts'

export interface Type<T = any> {
  [Type.kType]?: true
  type: Field.Type<T>
  inner?: T extends (infer I)[] ? [Type<I>] : Field.Type<T> extends 'json' ? { [key in keyof T]: Type<T[key]> } : never
}

export namespace Type {
  export const kType = Symbol.for('minato.type')

  export const Boolean: Type<boolean> = defineProperty({ type: 'boolean' }, kType, true) as any
  export const Number: Type<number> = defineProperty({ type: 'double' }, kType, true)
  export const String: Type<string> = defineProperty({ type: 'string' }, kType, true)

  type TypeMap<T extends Dict<any>> = {
    [K in keyof T]: Type<T[K]>
  }

  export type Object<T = any> = Type<T>
  export const Object = <T extends Dict<any>>(obj?: T): Object<TypeMap<T>> => defineProperty({
    type: 'json' as any,
    inner: globalThis.Object.keys(obj ?? {}).length ? mapValues(obj!, (value) => isType(value) ? value : fromField(value)) as any : undefined,
  }, kType, true)

  export type Array<T = any> = Type<T[]>
  export const Array = <T>(type?: Type<T>): Type.Array<T> => defineProperty({
    type: 'json',
    inner: type ? [type] : undefined,
  }, kType, true)

  export function fromPrimitive<T>(value: T): Type<T> {
    if (isNullable(value)) return fromField('expr' as any)
    else if (typeof value === 'number') return Number as any
    else if (typeof value === 'string') return String as any
    else if (typeof value === 'boolean') return Boolean as any
    else if (value instanceof Date) return fromField('timestamp' as any)
    else if (ArrayBuffer.isView(value)) return fromField('binary' as any)
    else if (globalThis.Array.isArray(value)) return Array(value.length ? fromPrimitive(value[0]) : undefined) as any
    else if (typeof value === 'object') return fromField('json' as any)
    throw new TypeError(`invalid primitive: ${value}`)
  }

  export function fromField<T>(field: Field<T> | Field.Type<T>): Type<T> {
    if (isType(field)) throw new TypeError(`invalid field: ${field}`)
    if (typeof field === 'string') return defineProperty({ type: field }, kType, true)
    else if (field.type) return field.type
    else if (field.expr?.[kType]) return field.expr[kType]
    throw new TypeError(`invalid field: ${field}`)
  }

  export function fromTerm<T>(value: Eval.Term<T>): Type<T> {
    if (isEvalExpr(value)) return value[kType] ?? fromField('expr' as any)
    else return fromPrimitive(value)
  }

  export function isType(value: any): value is Type {
    return value?.[kType] === true
  }

  export function getInner(type: Type<any>, key?: string): Type | undefined {
    if (!type?.inner) return
    if (globalThis.Array.isArray(type.inner) && isNullable(key)) return type.inner[0]
    if (isNullable(key)) return
    if (type.inner[key]) return type.inner[key]
    return Object(globalThis.Object.fromEntries(globalThis.Object.entries(type.inner)
      .filter(([k]) => k.startsWith(`${key}.`))
      .map(([k, v]) => [k.slice(key.length + 1), v]),
    ))
  }
}
