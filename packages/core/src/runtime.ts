import { getExprRuntimeType } from './eval'

const kRuntime = Symbol('Runtime')

export interface RuntimeType<T = any, A extends boolean = boolean> {
  [kRuntime]: true
  primitive: RuntimeType.Primitive<T>
  list: A
}

export namespace RuntimeType {
  export type Primitive<T = any> = 'any' | 'number' | 'string' | 'boolean' | 'date' | 'regexp' | RuntimeType
  | (T extends object ? { [key in keyof T]: RuntimeType } : never)

  export function create<T extends any, P extends RuntimeType.Primitive<T>>(primitive: P): RuntimeType<T, false>
  export function create<T extends any, P extends RuntimeType.Primitive<T>, A extends boolean>(primitive: P, list: A): RuntimeType<T, A>
  export function create(primitive: any, list: boolean = false) {
    return { [kRuntime]: true, primitive, list }
  }

  export function merge(...types: any[]): RuntimeType {
    return types.map(x => getExprRuntimeType(x)).find(x => x.primitive !== 'any') ?? RuntimeType.any
  }

  export function test(type: any): type is RuntimeType {
    return type && type[kRuntime]
  }

  export const any = RuntimeType.create('any')
}
