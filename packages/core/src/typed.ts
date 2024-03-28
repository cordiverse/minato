import { defineProperty, isNullable, mapValues } from 'cosmokit'
import { Field } from './model.ts'
import { Eval, isEvalExpr } from './eval.ts'

export interface Typed<T = any> {
  [Typed.kTyped]?: true
  type: Field.Type<T>
  inner?: T extends (infer I)[] ? Typed<I> : Field.Type<T> extends 'json' ? { [key in keyof T]: Typed<T[key]> } : never
  list?: T extends any[] ? true : false
}

export namespace Typed {
  export const kTyped = Symbol.for('minato.typed')

  export const Boolean: Typed<boolean> = defineProperty({ type: 'boolean' }, kTyped, true) as any
  export const Number: Typed<number> = defineProperty({ type: 'double' }, kTyped, true)
  export const String: Typed<string> = defineProperty({ type: 'string' }, kTyped, true)

  export type Object<T = any> = Typed<T>
  export const Object = <T extends object>(obj?: T): Object<T> => defineProperty({
    type: 'json' as any,
    inner: globalThis.Object.keys(obj ?? {}).length ? mapValues(obj!, (value) => isTyped(value) ? value : fromField(value)) as any : undefined,
  }, kTyped, true)

  export type Array<T = any> = Typed<T[]>
  export const Array = <T>(type?: Typed<T>): Typed.Array<T> => defineProperty({
    type: 'json',
    inner: type,
    list: true,
  }, kTyped, true)

  export function fromPrimitive<T>(value: T): Typed<T> {
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

  export function fromField<T>(field: Field<T> | Field.Type<T>): Typed<T> {
    if (typeof field === 'string') return defineProperty({ type: field }, kTyped, true)
    else if (field.typed) return field.typed
    else if (field.expr?.[kTyped]) return field.expr[kTyped]
    else return defineProperty({ type: field.type }, kTyped, true)
  }

  export function fromTerm<T>(value: Eval.Term<T>): Typed<T> {
    if (isEvalExpr(value)) return value[kTyped] ?? fromField('expr' as any)
    else return fromPrimitive(value)
  }

  export function isTyped(value: any): value is Typed {
    return value?.[kTyped] === true
  }
}
