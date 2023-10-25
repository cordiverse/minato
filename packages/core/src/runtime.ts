import { getExprRuntimeType } from './eval'

const kRuntime = Symbol('Runtime')

export interface RuntimeType<T = any> {
  [kRuntime]: true
  primitive: RuntimeType.Primitive<T>
  list?: boolean
  json?: boolean
}

export namespace RuntimeType {
  export type Primitive<T = any> = 'any' | 'number' | 'string' | 'boolean' | 'date' | 'regexp' | RuntimeType<T> | { [key in keyof T]: RuntimeType<any> }

  export function create<T, P extends RuntimeType.Primitive<T> = RuntimeType.Primitive<T>>(primitive: P, extra: Partial<RuntimeType<T>> = {}): RuntimeType<T> {
    if (Object.keys(extra).length === 0 && test(primitive)) return primitive
    return { [kRuntime]: true, primitive, ...extra }
  }

  export function merge(...types: any[]): RuntimeType {
    return types.map(x => getExprRuntimeType(x)).find(x => x.primitive !== 'any') ?? RuntimeType.any
  }

  export function list(type: any): RuntimeType {
    const primitive = getExprRuntimeType(type)
    if (!primitive.list) return { ...primitive, list: true }
    else return create(primitive, { list: true })
  }

  export function json(type: any): RuntimeType {
    const primitive = getExprRuntimeType(type)
    if (!primitive.json) return { ...primitive, json: true }
    else return create(primitive, { json: true })
  }

  export function test(type: any): type is RuntimeType {
    return type && type[kRuntime]
  }

  export const any = RuntimeType.create('any')
  export const number = RuntimeType.create('number')
  export const string = RuntimeType.create('string')
  export const boolean = RuntimeType.create('boolean')
  export const date = RuntimeType.create('date')
  export const regexp = RuntimeType.create('regexp')
}
