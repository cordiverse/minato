import { Dict, Intersect, makeArray, MaybeArray, valueMap } from 'cosmokit'
import { Eval, Update } from './eval'
import { Field, Model } from './model'
import { Query } from './query'
import { Computed, Flatten, Indexable, Keys } from './utils'
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

type TableMap2<S, M extends Dict<TableLike<S>>> = {
  [K in keyof M]: TableType<S, M[K]>
}

export class Database<S = any> {
  public tables: { [K in Keys<S>]: Model<S[K]> } = Object.create(null)
  public drivers: Record<keyof any, Driver> = Object.create(null)
  private tasks: Dict<Promise<void>> = Object.create(null)
  private stashed = new Set<string>()

  refresh() {
    for (const name in this.tables) {
      this.tasks[name] = this.prepare(name)
    }
  }

  private getDriver(table: any) {
    // const model: Model = this.tables[name]
    // if (model.driver) return this.drivers[model.driver]
    return Object.values(this.drivers)[0]
  }

  private async prepare(name: string) {
    this.stashed.add(name)
    await this.tasks[name]
    return new Promise<void>(async (resolve) => {
      Promise.resolve().then(async () => {
        if (this.stashed.delete(name)) {
          await this.getDriver(name)?.prepare(name)
        }
        resolve()
      })
    })
  }

  extend<K extends Keys<S>>(name: K, fields: Field.Extension<S[K]>, config: Partial<Model.Config<S[K]>> = {}) {
    let model = this.tables[name]
    if (!model) {
      model = this.tables[name] = new Model(name)
      // model.driver = config.driver
    }
    model.extend(fields, config)
    this.tasks[name] = this.prepare(name)
  }

  select<T extends Keys<S>>(table: T, query?: Query<S[T]>): Selection<S[T]> {
    return new Selection(this.getDriver(table), table, query)
  }

  join<M extends readonly Keys<S>[]>(tables: M): Selection<TableMap1<S, M>>
  join<M extends Dict<TableLike<S>>>(tables: M): Selection<TableMap2<S, M>>
  join(tables: any) {
    const selections: Dict<Selection<S>> = Array.isArray(tables)
      ? Object.fromEntries(tables.map((name) => [name, name]))
      : tables
    return new Selection(this.getDriver(tables[0]), selections)
  }

  async get<T extends Keys<S>, K extends Keys<S[T]>>(table: T, query: Query<S[T]>, cursor?: Driver.Cursor<K>): Promise<Pick<S[T], K>[]> {
    await this.tasks[table]
    if (Array.isArray(cursor)) {
      cursor = { fields: cursor }
    } else if (!cursor) {
      cursor = {}
    }

    const selection = this.select(table, query)
    if (cursor.fields) selection.project(cursor.fields)
    if (cursor.limit !== undefined) selection.limit(cursor.limit)
    if (cursor.offset !== undefined) selection.offset(cursor.offset)
    if (cursor.sort) {
      for (const field in cursor.sort) {
        selection.orderBy(field as any, cursor.sort[field])
      }
    }
    return selection.execute()
  }

  async eval<T extends Keys<S>, U>(table: T, expr: Selection.Callback<S[T], U>, query?: Query<S[T]>): Promise<U> {
    await this.tasks[table]
    return this.select(table, query).execute(typeof expr === 'function' ? expr : () => expr)
  }

  async set<T extends Keys<S>>(table: T, query: Query<S[T]>, update: Computed<S[T], Update<S[T]>>) {
    await this.tasks[table]
    const sel = this.select(table, query)
    if (typeof update === 'function') update = update(sel.row)
    const primary = makeArray(sel.model.primary)
    if (primary.some(key => key in update)) {
      throw new TypeError(`cannot modify primary key`)
    }
    await sel._action('set', sel.model.format(update)).execute()
  }

  async remove<T extends Keys<S>>(table: T, query: Query<S[T]>) {
    await this.tasks[table]
    const sel = this.select(table, query)
    await sel._action('remove').execute()
  }

  async create<T extends Keys<S>>(table: T, data: Partial<S[T]>): Promise<S[T]> {
    await this.tasks[table]
    const sel = this.select(table)
    return sel._action('create', sel.model.create(data)).execute()
  }

  async upsert<T extends Keys<S>>(table: T, upsert: Computed<S[T], Update<S[T]>[]>, keys?: MaybeArray<Keys<Flatten<S[T]>, Indexable>>) {
    await this.tasks[table]
    const sel = this.select(table)
    if (typeof upsert === 'function') upsert = upsert(sel.row)
    upsert = upsert.map(item => sel.model.format(item))
    keys = makeArray(keys || sel.model.primary) as any
    await sel._action('upsert', upsert, keys).execute()
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
  abstract set(sel: Selection.Mutable, data: Update): Promise<void>
  abstract remove(sel: Selection.Mutable): Promise<void>
  abstract create(sel: Selection.Mutable, data: any): Promise<any>
  abstract upsert(sel: Selection.Mutable, data: any[], keys: string[]): Promise<void>

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
        model.fields[`${key}.${field}`] = {
          type: 'expr',
          expr: { $: [key, field] } as any,
        }
      }
    }
    return model
  }
}
