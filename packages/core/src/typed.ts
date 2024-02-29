import { mapValues } from 'cosmokit'
import { Field, Primary } from './model.ts'
import { Eval, isEvalExpr } from './eval.ts'

export interface Typed<T = any> {
  type: Typed.Type<T>
  field?: Field.Type<T>
  inner?: Typed.Type<T> extends 'object' ? { [key in keyof T]: Typed<T[key]> } : T extends (infer I)[] ? Typed<I> : never
}

export namespace Typed {

  export const expr = Symbol('typed.expr')

  export type Type<T = any> =
    | T extends Primary ? 'primary'
    : T extends number ? 'number'
    : T extends string ? 'string'
    : T extends boolean ? 'boolean'
    : T extends Date ? 'date'
    : T extends unknown[] ? 'list'
    : T extends object ? 'object'
    : never

  // export type Type = 'string' | 'number' | 'boolean' | 'object' | 'date'

  export type Object<T = any> = Typed<T>
  export type List<T = any> = Typed<T[]>

  export const Boolean: Typed<boolean> = { type: 'boolean' }
  export const Number: Typed<number> = { type: 'number' }
  export const String: Typed<string> = { type: 'string' }
  export const $Date: Typed<Date> = { type: 'date' }
  export const $Object: Typed<object> = { type: 'object' }

  export const Object = <T extends object>(obj: T): Object<T> => ({
    type: 'object' as any,
    inner: mapValues(obj, (value) => transform(value)) as any,
  })
  export const List = <T>(type: Typed<T>): List<T> => ({
    type: 'list' as any,
    inner: type as any,
  })

  export function fromPrimitive<T>(value: T): Typed<T> {
    if (typeof value === 'number') return { type: 'number' } as any
    else if (typeof value === 'string') return { type: 'string' } as any
    else if (typeof value === 'boolean') return { type: 'boolean' } as any
    else if (value instanceof Date) return { type: 'date' } as any
    else if (Array.isArray(value)) return { type: 'object' } as any
    else if (typeof value === 'object') return { type: 'object' } as any
    throw new TypeError(`invalid primitive: ${value}`)
  }

  export function fromField<T>(field: Field<T>): Typed {
    if (field.typed) return field.typed
    else if (field.expr?.[expr]) return field.expr[expr]
    // else if (field.type === 'primary') return { type: 'string' }
    else if (Field.number.includes(field.type)) return { ...Number, field: field.type }
    else if (Field.string.includes(field.type)) return { ...String, field: field.type }
    else if (Field.boolean.includes(field.type)) return { ...Boolean, field: field.type }
    else if (Field.date.includes(field.type)) return { ...$Date, field: field.type }
    else if (Field.object.includes(field.type)) return { ...$Object, field: field.type, inner: undefined }
    throw new TypeError(`invalid field: ${field}`)
  }

  export function transform<T>(value: Eval.Expr<T> | T): Typed<T> {
    if (isEvalExpr(value)) return value[expr]
    else return fromPrimitive(value)
  }
}
