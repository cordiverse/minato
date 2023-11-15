import { Awaitable, Dict, Intersect, makeArray, MaybeArray, valueMap } from 'cosmokit'
import { Eval, Update } from './eval'
import { Field, Model } from './model'
import { Query } from './query'
import { Flatten, Indexable, Keys, Row } from './utils'
import { Direction, Modifier, Selection } from './selection'

export namespace Driver {
  export interface Stats {
    size: number
    tables: Dict<TableStats>
  }

  export interface TableStats {
    count: number
    size: number
  }

  export type Cursor<K extends string = never> = K[] | CursorOptions<K>

  export interface CursorOptions<K> {
    limit?: number
    offset?: number
    fields?: K[]
    sort?: Dict<Direction>
  }

  export interface WriteResult {
    inserted?: number
    modified?: number
    removed?: number
  }
}

type TableLike<S> = Keys<S> | Selection

type TableType<S, T extends TableLike<S>> =
  | T extends Keys<S> ? S[T]
  : T extends Selection<infer U> ? U
  : never

type TableMap1<S, M extends readonly Keys<S>[]> = Intersect<
  | M extends readonly (infer K extends Keys<S>)[]
  ? { [P in K]: TableType<S, P> }
  : never
>

type TableMap2<S, U extends Dict<TableLike<S>>> = {
  [K in keyof U]: TableType<S, U[K]>
}

type JoinParameters<S, U extends readonly Keys<S>[]> =
  | U extends readonly [infer K extends Keys<S>, ...infer R]
  ? [Row<S[K]>, ...JoinParameters<S, Extract<R, readonly Keys<S>[]>>]
  : []

type JoinCallback1<S, U extends readonly Keys<S>[]> = (...args: JoinParameters<S, U>) => Eval.Expr<boolean>

type JoinCallback2<S, U extends Dict<TableLike<S>>> = (args: {
  [K in keyof U]: Row<TableType<S, U[K]>>
}) => Eval.Expr<boolean>

export class Database<S = any> {
  public tables: { [K in Keys<S>]: Model<S[K]> } = Object.create(null)
  public drivers: Record<keyof any, Driver> = Object.create(null)
  public migrating = false
  private prepareTasks: Dict<Promise<void>> = Object.create(null)
  private migrateTasks: Dict<Promise<void>> = Object.create(null)

  private stashed = new Set<string>()

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
    await this.getDriver(name)?.prepare(name)
  }

  extend<K extends Keys<S>>(name: K, fields: Field.Extension<S[K]>, config: Partial<Model.Config<S[K]>> = {}) {
    let model = this.tables[name]
    if (!model) {
      model = this.tables[name] = new Model(name)
      // model.driver = config.driver
    }
    model.extend(fields, config)
    this.prepareTasks[name] = this.prepare(name)
  }

  migrate<K extends Keys<S>>(name: K, fields: Field.Extension<S[K]>, callback: Model.Migration) {
    this.extend(name, fields, { callback })
  }

  select<T extends Keys<S>>(table: T, query?: Query<S[T]>): Selection<S[T]> {
    return new Selection(this.getDriver(table), table, query)
  }

  join<U extends readonly Keys<S>[]>(tables: U, callback?: JoinCallback1<S, U>, optional?: boolean[]): Selection<TableMap1<S, U>>
  join<U extends Dict<TableLike<S>>>(tables: U, callback?: JoinCallback2<S, U>, optional?: Dict<boolean, Keys<U>>): Selection<TableMap2<S, U>>
  join(tables: any, query?: any, optional?: any) {
    if (Array.isArray(tables)) {
      const sel = new Selection(this.getDriver(tables[0]), Object.fromEntries(tables.map((name) => [name, this.select(name)])))
      if (typeof query === 'function') {
        sel.args[0].having = Eval.and(query(...tables.map(name => sel.row[name])))
      }
      sel.args[0].optional = Object.fromEntries(tables.map((name, index) => [name, optional?.[index]]))
      return new Selection(this.getDriver(sel), sel)
    } else {
      const sel = new Selection(this.getDriver(Object.values(tables)[0]), valueMap(tables, (t: TableLike<S>) => typeof t === 'string' ? this.select(t) : t))
      if (typeof query === 'function') {
        sel.args[0].having = Eval.and(query(sel.row))
      }
      sel.args[0].optional = optional
      return new Selection(this.getDriver(sel), sel)
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
    return await sel._action('set', sel.model.format(update)).execute()
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

  async stopAll() {
    const drivers = Object.values(this.drivers)
    this.drivers = Object.create(null)
    await Promise.all(drivers.map(driver => driver.stop()))
  }

  async drop<T extends Keys<S>>(table: T) {
    await this.getDriver(table).drop(table)
  }

  async dropAll() {
    await Promise.all(Object.values(this.drivers).map(driver => driver.drop()))
  }

  async stats() {
    const stats: Driver.Stats = { size: 0, tables: {} }
    await Promise.all(Object.values(this.drivers).map(async (driver) => {
      const { size = 0, tables } = await driver.stats()
      stats.size += size
      Object.assign(stats.tables, tables)
    }))
    return stats
  }
}

export namespace Driver {
  export type Constructor<T = any> = new (database: Database, config?: T) => Driver
}

export abstract class Driver {
  abstract start(): Promise<void>
  abstract stop(): Promise<void>
  abstract drop(table?: string): Promise<void>
  abstract stats(): Promise<Partial<Driver.Stats>>
  abstract prepare(name: string): Promise<void>
  abstract get(sel: Selection.Immutable, modifier: Modifier): Promise<any>
  abstract eval(sel: Selection.Immutable, expr: Eval.Expr): Promise<any>
  abstract set(sel: Selection.Mutable, data: Update): Promise<Driver.WriteResult>
  abstract remove(sel: Selection.Mutable): Promise<Driver.WriteResult>
  abstract create(sel: Selection.Mutable, data: any): Promise<any>
  abstract upsert(sel: Selection.Mutable, data: any[], keys: string[]): Promise<Driver.WriteResult>

  constructor(public database: Database) {}

  model<S = any>(table: string | Selection.Immutable | Dict<string | Selection.Immutable>): Model<S> {
    if (typeof table === 'string') {
      const model = this.database.tables[table]
      if (model) return model
      throw new TypeError(`unknown table name "${table}"`)
    }

    if (table instanceof Selection) {
      if (!table.args[0].fields) return table.model
      const model = new Model('temp')
      model.fields = valueMap(table.args[0].fields, (_, key) => ({
        type: 'expr',
      }))
      return model
    }

    const model = new Model('temp')
    for (const key in table) {
      const submodel = this.model(table[key])
      for (const field in submodel.fields) {
        if (submodel.fields[field]!.deprecated) continue
        model.fields[`${key}.${field}`] = {
          type: 'expr',
          expr: { $: [key, field] } as any,
        }
      }
    }
    return model
  }

  async migrate(name: string, hooks: MigrationHooks) {
    const database = Object.create(this.database)
    const model = this.model(name)
    database.migrating = true
    if (this.database.migrating) await database.migrateTasks[name]
    database.migrateTasks[name] = Promise.resolve(database.migrateTasks[name]).then(() => {
      return Promise.all([...model.migrations].map(async ([migrate, keys]) => {
        try {
          if (!hooks.before(keys)) return
          await migrate(database)
          hooks.after(keys)
        } catch (reason) {
          hooks.error(reason)
        }
      }))
    }).then(hooks.finalize).catch(hooks.error)
  }
}

export interface MigrationHooks {
  before: (keys: string[]) => boolean
  after: (keys: string[]) => void
  finalize: () => Awaitable<void>
  error: (reason: any) => void
}
