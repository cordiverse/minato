import { defineProperty, Dict, pick, valueMap } from 'cosmokit'
import { Driver } from './driver'
import { Eval, executeEval } from './eval'
import { Model } from './model'
import { Query } from './query'
import { Comparable, Keys, randomId } from './utils'

export type Direction = 'asc' | 'desc'

export interface Modifier {
  limit: number
  offset: number
  sort: [Eval.Expr, Direction][]
  group: string[]
  having: Eval.Expr<boolean>
}

export namespace Executable {
  export type Action = 'get' | 'set' | 'remove' | 'create' | 'upsert' | 'eval'

  export interface Payload {
    type: Action
    table?: string
    ref: string
    query: Query.Expr
    fields?: Dict<Eval.Expr>
    args?: any[]
  }
}

const createRow = (ref: string, prefix = '', expr = {}) => new Proxy(expr, {
  get(target, key) {
    if (typeof key === 'symbol' || key.startsWith('$')) return Reflect.get(target, key)
    return createRow(ref, `${prefix}${key}.`, Eval('', [ref, `${prefix}${key}`]))
  },
})

export interface Executable extends Executable.Payload {}

export class Executable<S = any, T = any> {
  #row: Selection.Row<S>
  #model: Model

  public driver: Driver

  constructor(driver: Driver, payload?: Executable.Payload) {
    defineProperty(this, 'driver', driver)
    Object.assign(this, payload)
  }

  get row() {
    return this.#row ||= createRow(this.ref)
  }

  get model() {
    return this.#model ||= this.driver.model(this.table)
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

  resolveData(data: any, fields: Dict<Eval.Expr<any>>) {
    data = this.model.format(data, false)
    for (const key in this.model.fields) {
      data[key] ??= null
    }
    if (!fields) return this.model.parse(data)
    return this.model.parse(pick(data, Object.keys(fields)))
  }

  protected resolveField(field: Selection.Field<S>): Eval.Expr {
    if (typeof field === 'string') {
      return this.row[field]
    } else if (typeof field === 'function') {
      return field(this.row)
    }
  }

  protected resolveFields(fields: string | string[] | Dict) {
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

export namespace Selection {
  export type Callback<S, T = any> = (row: Row<S>) => Eval.Expr<T>
  export type Field<S = any> = Keys<S> | Callback<S>
  export type Take<S, F extends Field<S>> =
    | F extends Keys<S> ? S[F]
    : F extends Callback<S> ? Eval<ReturnType<F>>
    : never

  export type Row<S> = {
    [K in keyof S]: Eval.Expr<S[K]> & (S[K] extends Comparable ? {} : Row<S[K]>)
  }

  export type Yield<S, T> = T | ((row: Row<S>) => T)

  export type Project<S, T extends Dict<Field<S>>> = {
    [K in keyof T]: Take<S, T[K]>
  }

  export type Selector<S> = Keys<S>// | Selection

  export type Resolve<S, T> =
    | T extends Keys<S> ? S[T]
    // : T extends Selection<infer U> ? U
    : never
}

export class Selection<S = any> extends Executable<S, S[]> {
  args: [Modifier]

  constructor(driver: Driver, table: string, query?: Query) {
    super(driver)
    this.type = 'get'
    this.ref = randomId()
    this.table = table
    this.query = this.resolveQuery(query)
    this.args = [{ sort: [], limit: Infinity, offset: 0, group: [], having: Eval.and() }]
  }

  where(query: Query) {
    this.query.$and ||= []
    this.query.$and.push(this.resolveQuery(query))
    return this
  }

  limit(limit: number): this
  limit(offset: number, limit: number): this
  limit(...args: [number] | [number, number]) {
    if (args.length > 1) this.offset(args.shift())
    this.args[0].limit = args[0]
    return this
  }

  offset(offset: number) {
    this.args[0].offset = offset
    return this
  }

  orderBy(field: Selection.Field<S>, direction: Direction = 'asc') {
    this.args[0].sort.push([this.resolveField(field), direction])
    return this
  }

  groupBy<T extends Keys<S>>(fields: T | T[], cond?: Selection.Callback<S, boolean>): Selection<Pick<S, T>>
  groupBy<T extends Keys<S>, U extends Dict<Selection.Field<S>>>(
    fields: T | T[],
    extra?: U,
    cond?: Selection.Callback<S, boolean>,
  ): Selection<Pick<S, T> & Selection.Project<S, U>>
  groupBy<T extends Dict<Selection.Field<S>>>(fields: T, cond?: Selection.Callback<S, boolean>): Selection<Selection.Project<S, T>>
  groupBy<T extends Dict<Selection.Field<S>>, U extends Dict<Selection.Field<S>>>(
    fields: T,
    extra?: U,
    cond?: Selection.Callback<S, boolean>,
  ): Selection<Selection.Project<S, T & U>>
  groupBy(fields: any, ...args: any[]) {
    this.fields = this.resolveFields(fields)
    this.args[0].group = Object.keys(this.fields)
    const extra = typeof args[0] === 'function' ? undefined : args.shift()
    Object.assign(this.fields, this.resolveFields(extra || {}))
    if (args[0]) this.having(args[0])
    return this as any
  }

  having(cond: Selection.Callback<S, boolean>) {
    this.args[0].having['$and'].push(this.resolveField(cond))
    return this
  }

  project<T extends Keys<S>>(fields: T[]): Selection<Pick<S, T>>
  project<T extends Dict<Selection.Field<S>>>(fields: T): Selection<Selection.Project<S, T>>
  project(fields: Keys<S>[] | Dict<Selection.Field<S>>) {
    this.fields = this.resolveFields(fields)
    return this as any
  }

  _action(type: Executable.Action, ...args: any[]) {
    return new Executable(this.driver, { ...this, type, args })
  }

  /** @deprecated use `selection.execute()` instead */
  evaluate<T>(callback: Selection.Callback<S, T>): Executable<S, T> {
    return this._action('eval', this.resolveField(callback))
  }

  execute(): Promise<S[]>
  execute<T>(callback: Selection.Callback<S, T>): Promise<T>
  execute(callback?: any) {
    if (!callback) return super.execute()
    return this._action('eval', this.resolveField(callback)).execute()
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
