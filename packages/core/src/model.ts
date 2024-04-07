import { clone, is, isNullable, makeArray, MaybeArray, valueMap } from 'cosmokit'
import { Database } from './database.ts'
import { Eval, isEvalExpr } from './eval.ts'
import { Flatten, Keys, unravel } from './utils.ts'
import { Type } from './type.ts'
import { Driver } from './driver.ts'

export const Primary = Symbol('Primary')
export type Primary = (string | number) & { [Primary]: true }

export interface Field<T = any> {
  type: Type<T>
  deftype?: Field.Type<T>
  length?: number
  nullable?: boolean
  initial?: T
  precision?: number
  scale?: number
  expr?: Eval.Expr
  legacy?: string[]
  deprecated?: boolean
  transformers?: Driver.Transformer[]
}

export namespace Field {
  export const number: Type[] = ['integer', 'unsigned', 'float', 'double', 'decimal']
  export const string: Type[] = ['char', 'string', 'text']
  export const boolean: Type[] = ['boolean']
  export const date: Type[] = ['timestamp', 'date', 'time']
  export const object: Type[] = ['list', 'json']

  export type Type<T = any> =
    | T extends Primary ? 'primary'
    : T extends number ? 'integer' | 'unsigned' | 'float' | 'double' | 'decimal'
    : T extends string ? 'char' | 'string' | 'text'
    : T extends boolean ? 'boolean'
    : T extends Date ? 'timestamp' | 'date' | 'time'
    : T extends ArrayBuffer ? 'binary'
    : T extends unknown[] ? 'list' | 'json'
    : T extends object ? 'json'
    : 'expr'

  type Shorthand<S extends string> = S | `${S}(${any})`

  export type Object<T = any, N = any> = {
    type: 'object'
    inner?: Extension<T, N>
  } & Omit<Field<T>, 'type'>

  export type Array<T = any, N = any> = {
    type: 'array'
    inner?: Literal<T, N> | Definition<T, N> | Transform<T, any, N>
  } & Omit<Field<T[]>, 'type'>

  export type Transform<S = any, T = S, N = any> = {
    type: Type<T> | Keys<N, T> | NewType<T> | 'object' | 'array'
    dump: (value: S | null) => T | null | void
    load: (value: T | null) => S | null | void
    initial?: S
  } & Omit<Definition<T, N>, 'type' | 'initial'>

  export type Definition<T, N> =
    | (Omit<Field<T>, 'type'> & { type: Type<T> })
    | Object<T, N>
    | (T extends (infer I)[] ? Array<I, N> : never)

  export type Literal<T, N> =
    | Shorthand<Type<T>>
    | Keys<N, T>
    | NewType<T>

  export type Parsable<T = any> = {
    type: Type<T> | Field<T>['type']
  } & Omit<Field<T>, 'type'>

  type MapField<O = any, N = any> = {
    [K in keyof O]?: Literal<O[K], N> | Definition<O[K], N> | Transform<O[K], any, N>
  }

  export type Extension<O = any, N = any> = MapField<Flatten<O>, N>

  const NewType = Symbol('newtype')
  export type NewType<T> = string & { [NewType]: T }

  export type Config<O = any> = {
    [K in keyof O]?: Field<O[K]>
  }

  const regexp = /^(\w+)(?:\((.+)\))?$/

  export function parse(source: string | Parsable): Field {
    if (typeof source === 'function') throw new TypeError('view field is not supported')
    if (typeof source !== 'string') {
      return {
        initial: null,
        deftype: source.type as any,
        ...source,
        type: Type.fromField(source.type),
      }
    }

    // parse string definition
    const capture = regexp.exec(source)
    if (!capture) throw new TypeError('invalid field definition')
    const type = capture[1] as Type
    const args = (capture[2] || '').split(',')
    const field: Field = { deftype: type, type: Type.fromField(type) }

    // set default initial value
    if (field.initial === undefined) field.initial = getInitial(type)

    // set length information
    if (type === 'decimal') {
      field.precision = +args[0]
      field.scale = +args[1]
    } else if (args[0]) {
      field.length = +args[0]
    }

    return field
  }

  export function getInitial(type: Field.Type, initial?: any) {
    if (initial === undefined) {
      if (Field.number.includes(type)) return 0
      if (Field.string.includes(type)) return ''
      if (type === 'list') return []
      if (type === 'json') return {}
    }
    return initial
  }
}

export namespace Model {
  export type Migration = (database: Database) => Promise<void>

  export interface Config<O = {}> {
    callback?: Migration
    // driver?: keyof any
    autoInc: boolean
    primary: MaybeArray<Keys<O>>
    unique: MaybeArray<Keys<O>>[]
    foreign: {
      [K in keyof O]?: [string, string]
    }
  }
}

export interface Model<S> extends Model.Config<S> {}

export class Model<S = any> {
  fields: Field.Config<S> = {}
  migrations = new Map<Model.Migration, string[]>()

  private type: Type<S> | undefined

  constructor(public name: string) {
    this.autoInc = false
    this.primary = 'id' as never
    this.unique = []
    this.foreign = {}
  }

  extend(fields: Field.Extension<S>, config?: Partial<Model.Config<S>>): void
  extend(fields = {}, config: Partial<Model.Config> = {}) {
    const { primary, autoInc, unique = [] as [], foreign, callback } = config

    this.primary = primary || this.primary
    this.autoInc = autoInc || this.autoInc
    unique.forEach(key => this.unique.includes(key) || this.unique.push(key))
    Object.assign(this.foreign, foreign)

    if (callback) this.migrations.set(callback, Object.keys(fields))

    for (const key in fields) {
      this.fields[key] = Field.parse(fields[key])
      this.fields[key].deprecated = !!callback
    }

    if (typeof this.primary === 'string' && this.fields[this.primary]?.deftype === 'primary') {
      this.autoInc = true
    }

    // check index
    this.checkIndex(this.primary)
    this.unique.forEach(index => this.checkIndex(index))
  }

  private checkIndex(index: MaybeArray<string>) {
    for (const key of makeArray(index)) {
      if (!this.fields[key]) {
        throw new TypeError(`missing field definition for index key "${key}"`)
      }
    }
  }

  resolveValue(field: string | Field | Type, value: any) {
    if (isNullable(value)) return value
    if (typeof field === 'string') field = this.fields[field] as Field
    if (field) field = Type.fromField(field)
    if (field?.type === 'time') {
      const date = new Date(0)
      date.setHours(value.getHours(), value.getMinutes(), value.getSeconds(), value.getMilliseconds())
      return date
    } else if (field?.type === 'date') {
      const date = new Date(value)
      date.setHours(0, 0, 0, 0)
      return date
    }
    return value
  }

  resolveModel(obj: any, model?: Type) {
    if (!model) model = this.getType()
    if (isNullable(obj) || !model.inner) return obj
    if (Type.isArray(model) && Array.isArray(obj)) {
      return obj.map(x => this.resolveModel(x, Type.getInner(model)!))
    }

    const result = {}
    for (const key in obj) {
      const type = Type.getInner(model, key)
      if (!type || isNullable(obj[key])) {
        result[key] = obj[key]
      } else if (type.type !== 'json') {
        result[key] = this.resolveValue(type, obj[key])
      } else if (type.inner && Type.isArray(type) && Array.isArray(obj[key])) {
        result[key] = obj[key].map(x => this.resolveModel(x, Type.getInner(type)))
      } else if (type.inner) {
        result[key] = this.resolveModel(obj[key], type)
      } else {
        result[key] = obj[key]
      }
    }
    return result
  }

  format(source: object, strict = true, prefix = '', result = {} as S) {
    const fields = Object.keys(this.fields)
    Object.entries(source).map(([key, value]) => {
      key = prefix + key
      if (value === undefined) return
      if (fields.includes(key)) {
        result[key] = value
        return
      }
      const field = fields.find(field => key.startsWith(field + '.'))
      if (field) {
        result[key] = value
      } else if (!value || typeof value !== 'object' || isEvalExpr(value) || Object.keys(value).length === 0) {
        if (strict) {
          throw new TypeError(`unknown field "${key}" in model ${this.name}`)
        }
      } else {
        this.format(value, strict, key + '.', result)
      }
    })
    return prefix === '' ? this.resolveModel(result) : result
  }

  parse(source: object, strict = true, prefix = '', result = {} as S) {
    const fields = Object.keys(this.fields)
    if (strict && prefix === '') {
      // initialize object layout
      Object.assign(result as any, unravel(Object.fromEntries(fields
        .filter(key => key.includes('.'))
        .map(key => [key.slice(0, key.lastIndexOf('.')), {}])),
      ))
    }
    for (const key in source) {
      let node = result
      const segments = key.split('.').reverse()
      for (let index = segments.length - 1; index > 0; index--) {
        const segment = segments[index]
        node = node[segment] ??= {}
      }
      if (key in source) {
        const fullKey = prefix + key, value = source[key]
        const field = fields.find(field => fullKey === field || fullKey.startsWith(field + '.'))
        if (field) {
          node[segments[0]] = value
        } else if (!value || typeof value !== 'object' || isEvalExpr(value) || Array.isArray(value) || is('ArrayBuffer', value)
           || Object.keys(value).length === 0) {
          if (strict) {
            throw new TypeError(`unknown field "${fullKey}" in model ${this.name}`)
          } else {
            node[segments[0]] = value
          }
        } else {
          this.parse(value, strict, fullKey + '.', node[segments[0]] ??= {})
        }
      }
    }
    return prefix === '' ? this.resolveModel(result) : result
  }

  create(data?: {}) {
    const result = {} as S
    const keys = makeArray(this.primary)
    for (const key in this.fields) {
      const { initial, deprecated } = this.fields[key]!
      if (deprecated) continue
      if (!keys.includes(key) && !isNullable(initial)) {
        result[key] = clone(initial)
      }
    }
    return this.parse({ ...result, ...data })
  }

  getType(): Type<S>
  getType(key: string): Type | undefined
  getType(key?: string): Type | undefined {
    this.type ??= Type.Object(valueMap(this.fields!, field => Type.fromField(field!))) as any
    return key ? Type.getInner(this.type, key) : this.type
  }
}
