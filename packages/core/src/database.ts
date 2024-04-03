import { Dict, Intersect, makeArray, mapValues, MaybeArray, valueMap } from 'cosmokit'
import { Context, Service, Spread } from 'cordis'
import { Flatten, Indexable, Keys, randomId, Row, unravel } from './utils.ts'
import { Selection } from './selection.ts'
import { Field, Model } from './model.ts'
import { Driver } from './driver.ts'
import { Eval, Update } from './eval.ts'
import { Query } from './query.ts'
import { Type } from './type.ts'

type TableLike<S> = Keys<S> | Selection

type TableType<S, T extends TableLike<S>> =
  | T extends Keys<S> ? S[T]
  : T extends Selection<infer U> ? U
  : never

export namespace Join1 {
  export type Input<S> = readonly Keys<S>[]

  export type Output<S, U extends Input<S>> = Intersect<
    | U extends readonly (infer K extends Keys<S>)[]
    ? { [P in K]: TableType<S, P> }
    : never
  >

  type Parameters<S, U extends Input<S>> =
    | U extends readonly [infer K extends Keys<S>, ...infer R]
    ? [Row<S[K]>, ...Parameters<S, Extract<R, Input<S>>>]
    : []

  export type Predicate<S, U extends Input<S>> = (...args: Parameters<S, U>) => Eval.Expr<boolean>
}

export namespace Join2 {
  export type Input<S> = Dict<TableLike<S>>

  export type Output<S, U extends Input<S>> = {
    [K in keyof U]: TableType<S, U[K]>
  }

  type Parameters<S, U extends Input<S>> = {
    [K in keyof U]: Row<TableType<S, U[K]>>
  }

  export type Predicate<S, U extends Input<S>> = (args: Parameters<S, U>) => Eval.Expr<boolean>
}

const kTransaction = Symbol('transaction')

export class Database<S = any, N = any, C extends Context = Context> extends Service<undefined, C> {
  static [Service.provide] = 'model'
  static [Service.immediate] = true

  public tables: { [K in Keys<S>]: Model<S[K]> } = Object.create(null)
  public drivers: Record<keyof any, Driver> = Object.create(null)
  public types: Dict<Field.Transform> = Object.create(null)
  public migrating = false
  private prepareTasks: Dict<Promise<void>> = Object.create(null)
  private migrateTasks: Dict<Promise<void>> = Object.create(null)

  private stashed = new Set<string>()

  async connect<T = undefined>(driver: Driver.Constructor<T>, ...args: Spread<T>) {
    this.ctx.plugin(driver, args[0] as any)
    await this.ctx.start()
  }

  refresh() {
    for (const name in this.tables) {
      this.prepareTasks[name] = this.prepare(name)
    }
  }

  async prepared() {
    await Promise.all(Object.values(this.prepareTasks))
    if (!this.migrating) {
      await Promise.all(Object.values(this.migrateTasks))
    }
  }

  private getDriver(table: any) {
    // const model: Model = this.tables[name]
    // if (model.driver) return this.drivers[model.driver]
    const driver = Object.values(this.drivers)[0]
    if (driver) driver.database = this
    return driver
  }

  private async prepare(name: string) {
    this.stashed.add(name)
    await this.prepareTasks[name]
    await Promise.resolve()
    if (!this.stashed.delete(name)) return

    const driver = this.getDriver(name)
    if (!driver) return

    const { fields } = driver.model(name)
    Object.values(fields).forEach(field => field?.transformers?.forEach(x => driver.define(x)))

    await driver.prepare(name)
  }

  extend<K extends Keys<S>>(name: K, fields: Field.Extension<S[K], N>, config: Partial<Model.Config<S[K]>> = {}) {
    let model = this.tables[name]
    if (!model) {
      model = this.tables[name] = new Model(name)
      // model.driver = config.driver
    }
    Object.entries(fields).forEach(([key, field]: [string, any]) => {
      const transformer = []
      this.parseField(field, transformer, undefined, value => field = fields[key] = value)
      if (typeof field === 'object') field.transformers = transformer
    })
    model.extend(fields, config)
    this.prepareTasks[name] = this.prepare(name)
    ;(this.ctx as Context).emit('model', name)
  }

  private parseField(field: any, transformers: Driver.Transformer[] = [], setInitial?: (value) => void, setField?: (value) => void): Type {
    if (field === 'array') {
      setInitial?.([])
      setField?.({ type: 'json', initial: [] })
      return Type.Array()
    } else if (field === 'object') {
      setInitial?.({})
      setField?.({ type: 'json', initial: {} })
      return Type.Object()
    } else if (typeof field === 'string' && this.types[field]) {
      transformers.push({
        types: [this.types[field].type],
        load: this.types[field].load,
        dump: this.types[field].dump,
      })
      setInitial?.(this.types[field].initial)
      setField?.(this.types[field])
      return Type.fromField(field as any)
    } else if (typeof field === 'object' && field.load && field.dump) {
      const name = this.define(field)
      transformers.push({
        types: [name as any],
        load: field.load,
        dump: field.dump,
      })
      // for transform type, intentionally assign a null initial on default
      // setInitial?.(Field.getInitial(field.type, field.initial))
      setInitial?.(field.initial)
      setField?.({ ...field, deftype: field.type, type: name })
      return Type.fromField(name as any)
    } else if (typeof field === 'object' && field.type === 'object') {
      const inner = unravel(field.inner, value => (value.type = 'object', value.inner ??= {}))
      const initial = Object.create(null)
      const res = Type.Object(mapValues(inner, (x, k) => this.parseField(x, transformers, value => initial[k] = value)))
      setInitial?.(Field.getInitial('json', initial))
      setField?.({ initial: Field.getInitial('json', initial), ...field, deftype: 'json', type: res })
      return res
    } else if (typeof field === 'object' && field.type === 'array') {
      const res = field.inner ? Type.Array(this.parseField(field.inner, transformers)) : Type.Array()
      setInitial?.([])
      setField?.({ initial: [], ...field, deftype: 'json', type: res })
      return res
    } else if (typeof field === 'object') {
      setInitial?.(Field.getInitial(field.type.split('(')[0], field.initial))
      setField?.(field)
      return Type.fromField(field.type.split('(')[0])
    } else {
      setInitial?.(Field.getInitial(field.split('(')[0]))
      setField?.(field)
      return Type.fromField(field.split('(')[0])
    }
  }

  define<K extends Exclude<Keys<N>, Field.Type>>(name: K, field: Field.Transform<N[K]>): K
  define<S>(field: Field.Transform<S>): Field.NewType<S>
  define(name: any, field?: any) {
    if (typeof name === 'object') {
      field = name
      name = undefined
    }

    if (name && this.types[name]) throw new Error(`type "${name}" already defined`)
    if (!name) while (this.types[name = '_define_' + randomId()]);
    this[Context.current].effect(() => {
      this.types[name] = { deftype: field.type, ...field, type: name }
      return () => delete this.types[name]
    })
    return name as any
  }

  migrate<K extends Keys<S>>(name: K, fields: Field.Extension<S[K], N>, callback: Model.Migration) {
    this.extend(name, fields, { callback })
  }

  select<T>(table: Selection<T>, query?: Query<T>): Selection<T>
  select<T extends Keys<S>>(table: T, query?: Query<S[T]>): Selection<S[T]>
  select(table: any, query?: any) {
    return new Selection(this.getDriver(table), table, query)
  }

  join<const U extends Join1.Input<S>>(tables: U, callback?: Join1.Predicate<S, U>, optional?: boolean[]): Selection<Join1.Output<S, U>>
  join<const U extends Join2.Input<S>>(tables: U, callback?: Join2.Predicate<S, U>, optional?: Dict<boolean, Keys<U>>): Selection<Join2.Output<S, U>>
  join(tables: any, query?: any, optional?: any) {
    if (Array.isArray(tables)) {
      const sel = new Selection(this.getDriver(tables[0]), Object.fromEntries(tables.map((name) => [name, this.select(name)])))
      if (typeof query === 'function') {
        sel.args[0].having = Eval.and(query(...tables.map(name => sel.row[name])))
      }
      sel.args[0].optional = Object.fromEntries(tables.map((name, index) => [name, optional?.[index]]))
      return this.select(sel)
    } else {
      const sel = new Selection(this.getDriver(Object.values(tables)[0]), valueMap(tables, (t: TableLike<S>) => typeof t === 'string' ? this.select(t) : t))
      if (typeof query === 'function') {
        sel.args[0].having = Eval.and(query(sel.row))
      }
      sel.args[0].optional = optional
      return this.select(sel)
    }
  }

  async get<T extends Keys<S>, K extends Keys<S[T]>>(table: T, query: Query<S[T]>, cursor?: Driver.Cursor<K>): Promise<Pick<S[T], K>[]> {
    return this.select(table, query).execute(cursor)
  }

  async eval<T extends Keys<S>, U>(table: T, expr: Selection.Callback<S[T], U, true>, query?: Query<S[T]>): Promise<U> {
    return this.select(table, query).execute(typeof expr === 'function' ? expr : () => expr)
  }

  async set<T extends Keys<S>>(table: T, query: Query<S[T]>, update: Row.Computed<S[T], Update<S[T]>>): Promise<Driver.WriteResult> {
    const sel = this.select(table, query)
    if (typeof update === 'function') update = update(sel.row)
    const primary = makeArray(sel.model.primary)
    if (primary.some(key => key in update)) {
      throw new TypeError(`cannot modify primary key`)
    }
    update = sel.model.format(update)
    if (Object.keys(update).length === 0) return {}
    return await sel._action('set', update).execute()
  }

  async remove<T extends Keys<S>>(table: T, query: Query<S[T]>): Promise<Driver.WriteResult> {
    const sel = this.select(table, query)
    return await sel._action('remove').execute()
  }

  async create<T extends Keys<S>>(table: T, data: Partial<S[T]>): Promise<S[T]> {
    const sel = this.select(table)
    const { primary, autoInc } = sel.model
    if (!autoInc) {
      const keys = makeArray(primary)
      if (keys.some(key => !(key in data))) {
        throw new Error('missing primary key')
      }
    }
    return sel._action('create', sel.model.create(data)).execute()
  }

  async upsert<T extends Keys<S>>(
    table: T,
    upsert: Row.Computed<S[T], Update<S[T]>[]>,
    keys?: MaybeArray<Keys<Flatten<S[T]>, Indexable>>,
  ): Promise<Driver.WriteResult> {
    const sel = this.select(table)
    if (typeof upsert === 'function') upsert = upsert(sel.row)
    upsert = upsert.map(item => sel.model.format(item))
    keys = makeArray(keys || sel.model.primary) as any
    return await sel._action('upsert', upsert, keys).execute()
  }

  async withTransaction(callback: (database: this) => Promise<void>): Promise<void>
  async withTransaction<T extends Keys<S>>(table: T, callback: (database: this) => Promise<void>): Promise<void>
  async withTransaction(arg: any, ...args: any[]) {
    if (this[kTransaction]) throw new Error('nested transactions are not supported')
    const [table, callback] = typeof arg === 'string' ? [arg, ...args] : [null, arg, ...args]
    const driver = this.getDriver(table)
    return await driver.withTransaction(async (session) => {
      const database = new Proxy(this, {
        get(target, p, receiver) {
          if (p === kTransaction) return true
          else if (p === 'getDriver') return () => session
          else return Reflect.get(target, p, receiver)
        },
      })
      await callback(database)
    })
  }

  async stopAll() {
    const drivers = Object.values(this.drivers)
    this.drivers = Object.create(null)
    await Promise.all(drivers.map(driver => driver.stop()))
  }

  async drop<T extends Keys<S>>(table: T) {
    await this.getDriver(table).drop(table)
  }

  async dropAll() {
    await Promise.all(Object.values(this.drivers).map(driver => driver.dropAll()))
  }

  async stats() {
    await this.prepared()
    const stats: Driver.Stats = { size: 0, tables: {} }
    await Promise.all(Object.values(this.drivers).map(async (driver) => {
      const { size = 0, tables } = await driver.stats()
      stats.size += size
      Object.assign(stats.tables, tables)
    }))
    return stats
  }
}
