import { defineProperty, Dict, makeArray, mapValues, MaybeArray, omit } from 'cosmokit'
import { Context, Service, Spread } from 'cordis'
import { FlatKeys, FlatPick, Indexable, Keys, randomId, Row, unravel } from './utils.ts'
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

  export type Output<S, U extends Input<S>> = {
    [P in U[number]]: TableType<S, P>
  }

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

export namespace Database {
  export interface Tables {}

  export interface Types {}
}

export class Database<S = {}, N = {}, C extends Context = Context> extends Service<undefined, C> {
  static [Service.provide] = 'model'
  static [Service.immediate] = true
  static readonly Tables = Symbol('minato.tables')
  static readonly Types = Symbol('minato.types')
  static readonly transact = Symbol('minato.transact')
  static readonly migrate = Symbol('minato.migrate')

  public tables: Dict<Model> = Object.create(null)
  public drivers: Driver<any, C>[] = []
  public types: Dict<Field.Transform> = Object.create(null)

  private _driver: Driver<any, C> | undefined
  private stashed = new Set<string>()
  private prepareTasks: Dict<Promise<void>> = Object.create(null)
  public migrateTasks: Dict<Promise<void>> = Object.create(null)

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
    if (this[Database.migrate]) return
    await Promise.all(Object.values(this.prepareTasks))
  }

  private getDriver(table: string | Selection): Driver<any, C> {
    if (table instanceof Selection) return table.driver as any
    const model: Model = this.tables[table]
    if (!model) throw new Error(`cannot resolve table "${table}"`)
    return model.ctx?.get('database')?._driver as any
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

  extend<K extends Keys<S>, T extends Field.Extension<S[K], N>>(name: K, fields: T, config: Partial<Model.Config<Keys<T>>> = {}) {
    let model = this.tables[name]
    if (!model) {
      model = this.tables[name] = new Model(name)
    }
    Object.entries(fields).forEach(([key, field]: [string, any]) => {
      const transformer = []
      this.parseField(field, transformer, undefined, value => field = fields[key] = value)
      if (typeof field === 'object') field.transformers = transformer
    })
    model.extend(fields, config)
    if (makeArray(model.primary).every(key => key in fields)) {
      defineProperty(model, 'ctx', this[Context.origin])
    }
    this.prepareTasks[name] = this.prepare(name)
    ;(this.ctx as Context).emit('model', name)
  }

  private _parseField(field: any, transformers: Driver.Transformer[] = [], setInitial?: (value) => void, setField?: (value) => void): Type {
    if (field === 'object') {
      setInitial?.({})
      setField?.({ type: 'json', initial: {} })
      return Type.Object()
    } else if (field === 'array') {
      setInitial?.([])
      setField?.({ type: 'json', initial: [] })
      return Type.Array()
    } else if (typeof field === 'string' && this.types[field]) {
      transformers.push({
        types: [field as any],
        load: this.types[field].load,
        dump: this.types[field].dump,
      }, ...(this.types[field].transformers ?? []))
      setInitial?.(this.types[field].initial)
      setField?.({ ...this.types[field], type: field })
      return Type.fromField(field)
    } else if (typeof field === 'string') {
      setInitial?.(Field.getInitial((field as any).split('(')[0]))
      setField?.(field)
      return Type.fromField(field.split('(')[0])
    } else if (typeof field === 'object' && field.type === 'object') {
      const inner = field.inner ? unravel(field.inner, value => (value.type = 'object', value.inner ??= {})) : Object.create(null)
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
    } else if (typeof field === 'object' && this.types[field.type]) {
      transformers.push({
        types: [field.type as any],
        load: this.types[field.type].load,
        dump: this.types[field.type].dump,
      }, ...(this.types[field.type].transformers ?? []))
      setInitial?.(field.initial === undefined ? this.types[field.type].initial : field.initial)
      setField?.({ initial: this.types[field.type].initial, ...field })
      return Type.fromField(field.type)
    } else {
      setInitial?.(Field.getInitial(field.type, field.initial))
      setField?.(field)
      return Type.fromField(field.type)
    }
  }

  private parseField(field: any, transformers: Driver.Transformer[] = [], setInitial?: (value) => void, setField?: (value: Field.Parsable) => void): Type {
    let midfield
    let type = this._parseField(field, transformers, setInitial, (value) => (midfield = value, setField?.(value)))
    if (typeof field === 'object' && field.load && field.dump) {
      if (type.inner) type = Type.fromField(this.define({ ...omit(midfield, ['load', 'dump']), type } as any))

      const name = this.define({ ...field, deftype: midfield.deftype, type: type.type })
      transformers.push({
        types: [name as any],
        load: field.load,
        dump: field.dump,
      })
      // for transform type, intentionally assign a null initial on default
      setInitial?.(field.initial)
      setField?.({ ...field, deftype: midfield.deftype ?? this.types[type.type]?.deftype ?? type.type, initial: midfield.initial, type: name })
      return Type.fromField(name as any)
    }
    if (typeof midfield === 'object') setField?.({ ...midfield, deftype: midfield.deftype ?? this.types[type.type]?.deftype ?? type?.type })
    return type
  }

  define<K extends Exclude<Keys<N>, Field.Type | 'object' | 'array'>>(
    name: K,
    field: Field.Definition<N[K], N> | Field.Transform<N[K], any, N>,
  ): K

  define<T>(field: Field.Definition<T, N> | Field.Transform<T, any, N>): Field.NewType<T>
  define(name: any, field?: any) {
    if (typeof name === 'object') {
      field = name
      name = undefined
    }

    if (name && this.types[name]) throw new Error(`type "${name}" already defined`)
    if (!name) while (this.types[name = '_define_' + randomId()]);

    const transformers = []
    const type = this._parseField(field, transformers, undefined, value => field = value)
    field.transformers = transformers

    this[Context.current].effect(() => {
      this.types[name] = { ...field }
      this.types[name].deftype ??= this.types[field.type]?.deftype ?? type.type as any
      return () => delete this.types[name]
    })
    return name as any
  }

  migrate<K extends Keys<S>>(
    name: K,
    fields: Field.Extension<S[K], N>,
    callback: Model.Migration<this>,
  ) {
    this.extend(name, fields, { callback })
  }

  select<T>(table: Selection<T>, query?: Query<T>): Selection<T>
  select<K extends Keys<S>>(table: K, query?: Query<S[K]>): Selection<S[K]>
  select(table: any, query?: any) {
    return new Selection(this.getDriver(table), table, query)
  }

  join<const X extends Join1.Input<S>>(
    tables: X,
    callback?: Join1.Predicate<S, X>,
    optional?: boolean[],
  ): Selection<Join1.Output<S, X>>

  join<X extends Join2.Input<S>>(
    tables: X,
    callback?: Join2.Predicate<S, X>,
    optional?: Dict<boolean, Keys<X>>,
  ): Selection<Join2.Output<S, X>>

  join(tables: any, query = (...args: any[]) => Eval.and(), optional?: any) {
    const oldTables = tables
    if (Array.isArray(oldTables)) {
      tables = Object.fromEntries(oldTables.map((name) => [name, this.select(name)]))
    }
    let sels = mapValues(tables, (t: TableLike<S>) => {
      return typeof t === 'string' ? this.select(t) : t
    })
    if (Object.keys(sels).length === 0) throw new Error('no tables to join')
    const drivers = new Set(Object.values(sels).map(sel => sel.driver))
    if (drivers.size !== 1) throw new Error('cannot join tables from different drivers')
    if (Object.keys(sels).length === 2 && (optional?.[0] || optional?.[Object.keys(sels)[0]])) {
      if (optional[1] || optional[Object.keys(sels)[1]]) throw new Error('full join is not supported')
      sels = Object.fromEntries(Object.entries(sels).reverse())
    }
    const sel = new Selection([...drivers][0], sels)
    if (Array.isArray(oldTables)) {
      sel.args[0].having = Eval.and(query(...oldTables.map(name => sel.row[name])))
      sel.args[0].optional = Object.fromEntries(oldTables.map((name, index) => [name, optional?.[index]]))
    } else {
      sel.args[0].having = Eval.and(query(sel.row))
      sel.args[0].optional = optional
    }
    return this.select(sel)
  }

  async get<K extends Keys<S>, P extends FlatKeys<S[K]> = any>(
    table: K,
    query: Query<S[K]>,
    cursor?: Driver.Cursor<P>,
  ): Promise<FlatPick<S[K], P>[]> {
    return this.select(table, query).execute(cursor)
  }

  async eval<K extends Keys<S>, T>(table: K, expr: Selection.Callback<S[K], T, true>, query?: Query<S[K]>): Promise<T> {
    return this.select(table, query).execute(typeof expr === 'function' ? expr : () => expr)
  }

  async set<K extends Keys<S>>(
    table: K,
    query: Query<S[K]>,
    update: Row.Computed<S[K], Update<S[K]>>,
  ): Promise<Driver.WriteResult> {
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

  async remove<K extends Keys<S>>(table: K, query: Query<S[K]>): Promise<Driver.WriteResult> {
    const sel = this.select(table, query)
    return await sel._action('remove').execute()
  }

  async create<K extends Keys<S>>(table: K, data: Partial<S[K]>): Promise<S[K]> {
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

  async upsert<K extends Keys<S>>(
    table: K,
    upsert: Row.Computed<S[K], Update<S[K]>[]>,
    keys?: MaybeArray<FlatKeys<S[K], Indexable>>,
  ): Promise<Driver.WriteResult> {
    const sel = this.select(table)
    if (typeof upsert === 'function') upsert = upsert(sel.row)
    upsert = upsert.map(item => sel.model.format(item))
    keys = makeArray(keys || sel.model.primary) as any
    return await sel._action('upsert', upsert, keys).execute()
  }

  makeProxy(marker: any, getDriver?: (driver: Driver<any, C>, database: this) => Driver<any, C>) {
    const drivers = new Map<Driver<any, C>, Driver<any, C>>()
    const database = new Proxy(this, {
      get: (target, p, receiver) => {
        if (p === marker) return true
        if (p !== 'getDriver') return Reflect.get(target, p, receiver)
        return (name: any) => {
          const original = this.getDriver(name)
          let driver = drivers.get(original)
          if (!driver) {
            driver = getDriver?.(original, database) ?? new Proxy(original, {
              get: (target, p, receiver) => {
                if (p === 'database') return database
                return Reflect.get(target, p, receiver)
              },
            })
            drivers.set(original, driver)
          }
          return driver
        }
      },
    })
    return database
  }

  withTransaction(callback: (database: this) => Promise<void>) {
    return this.transact(callback)
  }

  async transact(callback: (database: this) => Promise<void>) {
    if (this[Database.transact]) throw new Error('nested transactions are not supported')
    const finalTasks: Promise<void>[] = []
    const database = this.makeProxy(Database.transact, (driver) => {
      let session: any
      let _resolve: (value: any) => void
      const sessionTask = new Promise((resolve) => _resolve = resolve)
      driver = new Proxy(driver, {
        get: (target, p, receiver) => {
          if (p === 'database') return database
          if (p === 'session') return session
          if (p === '_ensureSession') return () => sessionTask
          return Reflect.get(target, p, receiver)
        },
      })
      finalTasks.push(driver.withTransaction((_session) => {
        _resolve(session = _session)
        return initialTask
      }))
      return driver
    })
    const initialTask = (async () => {
      await Promise.resolve()
      await callback(database)
    })()
    await initialTask.finally(() => Promise.all(finalTasks))
  }

  async stopAll() {
    await Promise.all(this.drivers.splice(0, Infinity).map(driver => driver.stop()))
  }

  async drop<K extends Keys<S>>(table: K) {
    if (this[Database.transact]) throw new Error('cannot drop table in transaction')
    await this.getDriver(table).drop(table)
  }

  async dropAll() {
    if (this[Database.transact]) throw new Error('cannot drop table in transaction')
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
