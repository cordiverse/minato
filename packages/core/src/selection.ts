import { defineProperty, Dict, valueMap } from 'cosmokit'
import { Driver } from './driver'
import { Eval, executeEval } from './eval'
import { Model } from './model'
import { Query } from './query'
import { Keys, Row, randomId } from './utils'

export type Direction = 'asc' | 'desc'

export interface Modifier {
  limit: number
  offset: number
  sort: [Eval.Expr, Direction][]
  group: string[]
  having: Eval.Expr<boolean>
  fields?: Dict<Eval.Expr>
  optional: Dict<boolean>
}

namespace Executable {
  export type Action = 'get' | 'set' | 'remove' | 'create' | 'upsert' | 'eval'

  export interface Payload {
    type: Action
    table: string | Selection | Dict<string | Selection.Immutable>
    ref: string
    query: Query.Expr
    args: any[]
  }
}

const createRow = (ref: string, expr = {}, prefix = '') => new Proxy(expr, {
  get(target, key) {
    if (typeof key === 'symbol' || key in target) return Reflect.get(target, key)
    return createRow(ref, Eval('', [ref, `${prefix}${key}`]), `${prefix}${key}.`)
  },
})

interface Executable extends Executable.Payload {}

class Executable<S = any, T = any> {
  public readonly row!: Row<S>
  public readonly model!: Model
  public readonly driver!: Driver

  constructor(driver: Driver, payload: Executable.Payload) {
    Object.assign(this, payload)
    const expr = {}
    if (typeof payload.table !== 'string' && !(payload.table instanceof Selection)) {
      for (const key in payload.table) {
        expr[key] = createRow(key)
      }
    }
    defineProperty(this, 'driver', driver)
    defineProperty(this, 'row', createRow(this.ref, expr))
    defineProperty(this, 'model', driver.model(this.table))
  }

  protected resolveQuery(query?: Query<S>): Query.Expr<S>
  protected resolveQuery(query: Query<S> = {}): any {
    if (typeof query === 'function') return { $expr: query(this.row) }
    if (Array.isArray(query) || query instanceof RegExp || ['string', 'number'].includes(typeof query)) {
      const { primary } = this.model
      if (Array.isArray(primary)) {
        throw new TypeError('invalid shorthand for composite primary key')
      }
      return { [primary]: query }
    }
    return query
  }

  protected resolveField(field: FieldLike<S>): Eval.Expr {
    if (typeof field === 'string') {
      return this.row[field]
    } else if (typeof field === 'function') {
      return field(this.row)
    } else {
      throw new TypeError('invalid field definition')
    }
  }

  protected resolveFields(fields: string | string[] | Dict<FieldLike<S>>) {
    if (typeof fields === 'string') fields = [fields]
    if (Array.isArray(fields)) {
      const modelFields = Object.keys(this.model.fields)
      const keys = fields.flatMap((key) => {
        if (this.model.fields[key]) return key
        return modelFields.filter(path => path.startsWith(key + '.'))
      })
      return Object.fromEntries(keys.map(key => [key, this.row[key]]))
    } else {
      return valueMap(fields, field => this.resolveField(field))
    }
  }

  execute(): Promise<T> {
    return this.driver[this.type as any](this, ...this.args)
  }
}

type FieldLike<S = any> = Keys<S> | Selection.Callback<S>

type FieldType<S, T extends FieldLike<S>> =
  | T extends Keys<S> ? S[T]
  : T extends Selection.Callback<S> ? Eval<ReturnType<T>>
  : never

type FieldMap<S, M extends Dict<FieldLike<S>>> = {
  [K in keyof M]: FieldType<S, M[K]>
}

export namespace Selection {
  export type Callback<S = any, T = any> = (row: Row<S>) => Eval.Expr<T>

  export interface Immutable extends Executable, Executable.Payload {
    tables: Dict<Model>
  }

  export interface Mutable extends Executable, Executable.Payload {
    table: string
  }
}

export interface Selection extends Executable.Payload {
  args: [Modifier]
}

export class Selection<S = any> extends Executable<S, S[]> {
  public tables: Dict<Model> = {}

  constructor(driver: Driver, table: string | Selection | Dict<string | Selection.Immutable>, query?: Query) {
    super(driver, {
      type: 'get',
      ref: randomId(),
      table,
      query: null as never,
      args: [{ sort: [], limit: Infinity, offset: 0, group: [], having: Eval.and(), optional: {} }],
    })
    this.tables[this.ref] = this.model
    this.query = this.resolveQuery(query)
    if (typeof table !== 'string') {
      Object.assign(this.tables, table.tables)
    }
  }

  where(query: Query<S>) {
    this.query.$and ||= []
    this.query.$and.push(this.resolveQuery(query))
    return this
  }

  limit(limit: number): this
  limit(offset: number, limit: number): this
  limit(...args: [number] | [number, number]) {
    if (args.length > 1) this.offset(args.shift()!)
    this.args[0].limit = args[0]
    return this
  }

  offset(offset: number) {
    this.args[0].offset = offset
    return this
  }

  orderBy(field: FieldLike<S>, direction: Direction = 'asc') {
    this.args[0].sort.push([this.resolveField(field), direction])
    return this
  }

  groupBy<K extends Keys<S>>(fields: K | K[], query?: Selection.Callback<S, boolean>): Selection<Pick<S, K>>
  groupBy<K extends Keys<S>, U extends Dict<FieldLike<S>>>(
    fields: K | K[],
    extra?: U,
    query?: Selection.Callback<S, boolean>,
  ): Selection<Pick<S, K> & FieldMap<S, U>>
  groupBy<K extends Dict<FieldLike<S>>>(fields: K, query?: Selection.Callback<S, boolean>): Selection<FieldMap<S, K>>
  groupBy<K extends Dict<FieldLike<S>>, U extends Dict<FieldLike<S>>>(
    fields: K,
    extra?: U,
    query?: Selection.Callback<S, boolean>,
  ): Selection<FieldMap<S, K & U>>
  groupBy(fields: any, ...args: any[]) {
    this.args[0].fields = this.resolveFields(fields)
    this.args[0].group = Object.keys(this.args[0].fields!)
    const extra = typeof args[0] === 'function' ? undefined : args.shift()
    Object.assign(this.args[0].fields!, this.resolveFields(extra || {}))
    if (args[0]) this.having(args[0])
    return new Selection(this.driver, this)
  }

  having(query: Selection.Callback<S, boolean>) {
    this.args[0].having['$and'].push(this.resolveField(query))
    return this
  }

  project<K extends Keys<S>>(fields: K | K[]): Selection<Pick<S, K>>
  project<U extends Dict<FieldLike<S>>>(fields: U): Selection<FieldMap<S, U>>
  project(fields: Keys<S>[] | Dict<FieldLike<S>>) {
    this.args[0].fields = this.resolveFields(fields)
    return new Selection(this.driver, this)
  }

  _action(type: Executable.Action, ...args: any[]) {
    return new Executable(this.driver, { ...this, type, args })
  }

  /** @deprecated use `selection.execute()` instead */
  evaluate<T>(callback: Selection.Callback<S, T>): Executable {
    const selection = new Selection(this.driver, this)
    return selection._action('eval', this.resolveField(callback))
  }

  execute(): Promise<S[]>
  execute<T>(callback: Selection.Callback<S, T>): Promise<T>
  execute(callback?: any) {
    if (!callback) return super.execute()
    return this.evaluate(callback).execute()
  }
}

export function executeSort(data: any[], modifier: Modifier, name: string) {
  const { limit, offset, sort } = modifier

  // step 1: sort data
  data.sort((a, b) => {
    for (const [field, direction] of sort) {
      const sign = direction === 'asc' ? 1 : -1
      const x = executeEval({ [name]: a, _: a }, field)
      const y = executeEval({ [name]: b, _: b }, field)
      if (x < y) return -sign
      if (x > y) return sign
    }
    return 0
  })

  // step 2: truncate data
  return data.slice(offset, offset + limit)
}
