import { defineProperty, mapValues } from 'cosmokit'
import { Field, Primary } from './model.ts'
import { Eval, isEvalExpr } from './eval.ts'

export interface Typed<T = any> {
  [Typed.symbol]?: true
  type: Typed.Type<T>
  field?: Field.Type<T>
  inner?: Typed.Type<T> extends 'object' ? { [key in keyof T]: Typed<T[key]> } : T extends (infer I)[] ? Typed<I> : never
}

export namespace Typed {

  export const symbol = Symbol.for('minato.typed')

  export type Type<T = any> =
    | T extends Primary ? 'primary'
    : T extends number ? 'number'
    : T extends string ? 'string'
    : T extends boolean ? 'boolean'
    : T extends Date ? 'date'
    : T extends unknown[] ? 'list'
    : T extends object ? 'object'
    : 'expr'

  // export type Type = 'string' | 'number' | 'boolean' | 'object' | 'date'

  export type Object<T = any> = Typed<T>
  export type List<T = any> = Typed<T[]>

  export const Boolean: Typed<boolean> = defineProperty({ type: 'boolean' }, symbol, true) as any
  export const Number: Typed<number> = defineProperty({ type: 'number' }, symbol, true)
  export const String: Typed<string> = defineProperty({ type: 'string' }, symbol, true)
  export const $Date: Typed<Date> = defineProperty({ type: 'date' }, symbol, true)
  export const $Object: Typed<object> = defineProperty({ type: 'object' }, symbol, true)
  export const $List: Typed<any[]> = defineProperty({ type: 'list' }, symbol, true)
  export const $Expr: Typed<any> = defineProperty({ type: 'expr' }, symbol, true)

  export const Object = <T extends object>(obj: T): Object<T> => defineProperty({
    type: 'object' as any,
    inner: mapValues(obj, (value) => transform(value)) as any,
  }, symbol, true)
  export const List = <T>(type: Typed<T>): List<T> => defineProperty({
    [symbol]: true,
    type: 'list' as any,
    inner: type as any,
  }, symbol, true)

  export function fromPrimitive<T>(value: T): Typed<T> {
    if (typeof value === 'number') return Number as any
    else if (typeof value === 'string') return String as any
    else if (typeof value === 'boolean') return Boolean as any
    else if (value instanceof Date) return $Date as any
    else if (Array.isArray(value)) return $List as any
    else if (typeof value === 'object') return $Object as any
    throw new TypeError(`invalid primitive: ${value}`)
  }

  export function fromField<T>(field: Field<T> | Field.Type<T>): Typed {
    if (typeof field === 'string') return defineProperty({ ...$Expr, field }, symbol, true)
    if (field.typed) return field.typed
    else if (field.expr?.[symbol]) return field.expr[symbol]
    else if (field.type === 'primary') return defineProperty({ type: 'primary', field: field.type }, symbol, true)
    else if (Field.number.includes(field.type)) return defineProperty({ ...Number, field: field.type }, symbol, true)
    else if (Field.string.includes(field.type)) return defineProperty({ ...String, field: field.type }, symbol, true)
    else if (Field.boolean.includes(field.type)) return defineProperty({ ...Boolean, field: field.type }, symbol, true)
    else if (Field.date.includes(field.type)) return defineProperty({ ...$Date, field: field.type }, symbol, true)
    else if (Field.object.includes(field.type)) return defineProperty({ ...$Object, field: field.type }, symbol, true) as any
    else return defineProperty({ ...$Expr, field: field.type }, symbol, true)
  }

  export function transform<T>(value: Eval.Expr<T> | T | Typed<T>): Typed<T> {
    if (isTyped(value)) return value
    else if (isEvalExpr(value)) return value[symbol]
    else return fromPrimitive(value)
  }

  export function isTyped(value: any): value is Typed {
    return value?.[symbol] === true
  }
}
