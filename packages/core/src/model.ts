import { isNullable, makeArray, MaybeArray } from 'cosmokit'
import { Database } from './database.ts'
import { Eval, isEvalExpr } from './eval.ts'
import { Selection } from './selection.ts'
import { clone, Flatten, isUint8Array, Keys } from './utils.ts'
import { Typed } from './typed.ts'

export const Primary = Symbol('Primary')
export type Primary = (string | number) & { [Primary]: true }

export interface Field<T = any> {
  type: Field.Type<T>
  length?: number
  nullable?: boolean
  initial?: T
  precision?: number
  scale?: number
  expr?: Eval.Expr
  legacy?: string[]
  deprecated?: boolean
  typed?: Typed<T>
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
    : T extends Uint8Array ? 'binary'
    : T extends unknown[] ? 'list' | 'json'
    : T extends object ? 'json'
    : 'expr'

  type Shorthand<S extends string> = S | `${S}(${any})`

  export type Transform<S = any, T = any> = {
    type: Type<T>
    dump: (value: S) => T | null
    load: (value: T, initial?: S) => S | null
    initial?: S
  } & Omit<Field<T>, 'type' | 'initial'>

  const NewType = Symbol('NewType')
  export type NewType<S = any> = string & { [NewType]: S }

  type MapField<O = any> = {
    [K in keyof O]?: Field<O[K]> | Shorthand<Type<O[K]>> | Selection.Callback<O, O[K]> | Transform<O[K]> | NewType<O[K]>
  }

  export type Extension<O = any> = MapField<Flatten<O>>

  export type Config<O = any> = {
    [K in keyof O]?: Field<O[K]>
  }

  const regexp = /^(\w+)(?:\((.+)\))?$/

  export function parse(source: string | Field): Field {
    if (typeof source === 'function') return { type: 'expr', expr: source }
    if (typeof source !== 'string') return { initial: null, typed: Typed.fromField(source), ...source }

    // parse string definition
    const capture = regexp.exec(source)
    if (!capture) throw new TypeError('invalid field definition')
    const type = capture[1] as Type
    const args = (capture[2] || '').split(',')
    const field: Field = { type }

    // set default initial value
    if (field.initial === undefined) {
      if (number.includes(field.type)) field.initial = 0
      if (string.includes(field.type)) field.initial = ''
      if (field.type === 'list') field.initial = []
      if (field.type === 'json') field.initial = {}
    }

    // set length information
    if (type === 'decimal') {
      field.precision = +args[0]
      field.scale = +args[1]
    } else if (args[0]) {
      field.length = +args[0]
    }

    field.typed = Typed.fromField(field)

    return field
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

    if (typeof this.primary === 'string' && this.fields[this.primary]?.type === 'primary') {
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

  resolveValue(key: string, value: any) {
    if (isNullable(value)) return value
    if (this.fields[key]?.type === 'time') {
      const date = new Date(0)
      date.setHours(value.getHours(), value.getMinutes(), value.getSeconds(), value.getMilliseconds())
      return date
    } else if (this.fields[key]?.type === 'date') {
      const date = new Date(value)
      date.setHours(0, 0, 0, 0)
      return date
    }
    return value
  }

  format(source: object, strict = true, prefix = '', result = {} as S) {
    const fields = Object.keys(this.fields)
    Object.entries(source).map(([key, value]) => {
      key = prefix + key
      if (value === undefined) return
      if (fields.includes(key)) {
        result[key] = this.resolveValue(key, value)
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
    return result
  }

  parse(source: object, strict = true, prefix = '', result = {} as S) {
    const fields = Object.keys(this.fields)
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
          node[segments[0]] = this.resolveValue(key, value)
        } else if (!value || typeof value !== 'object' || isEvalExpr(value) || Array.isArray(value) || isUint8Array(value) || Object.keys(value).length === 0) {
          if (strict) {
            throw new TypeError(`unknown field "${fullKey}" in model ${this.name}`)
          } else {
            node[segments[0]] = this.resolveValue(key, value)
          }
        } else {
          this.parse(value, strict, fullKey + '.', node[segments[0]] ??= {})
        }
      }
    }
    return result
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
}
