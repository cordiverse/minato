import { Dict, makeArray, MaybeArray, valueMap } from 'cosmokit'
import { Eval, Update } from './eval'
import { Field, Model } from './model'
import { Query } from './query'
import { Flatten, Indexable, Keys } from './utils'
import { Direction, Modifier, Selection } from './selection'

export type Result<S, K, T = (...args: any) => any> = {
  [P in keyof S as S[P] extends T ? P : P extends K ? P : never]: S[P]
}

export namespace Driver {
  export interface Stats {
    size?: number
    tables?: Dict<TableStats>
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

export class Database<S = any> {
  public tables: { [K in Keys<S>]?: Model<S[K]> } = Object.create(null)
  public drivers: Record<keyof any, Driver> = Object.create(null)
  private tasks: Dict<Promise<void>> = Object.create(null)
  private stashed = new Set<string>()

  refresh() {
    for (const name in this.tables) {
      this.tasks[name] = this.prepare(name)
    }
  }

  private getDriver(name: string) {
    const model: Model = this.tables[name]
    if (model.driver) return this.drivers[model.driver]
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

  extend<K extends Keys<S>>(name: K, fields: Field.Extension<S[K]>, config: Model.Config<S[K]> = {}) {
    let model = this.tables[name]
    if (!model) {
      model = this.tables[name] = new Model(name)
      model.driver = config.driver
    }
    model.extend(fields, config)
    this.tasks[name] = this.prepare(name)
  }

  select<T extends Selection.Selector<S>>(table: T, query?: Query<Selection.Resolve<S, T>>): Selection<Selection.Resolve<S, T>> {
    return new Selection(this.getDriver(table), table, query)
  }

  async get<T extends Keys<S>, K extends Keys<S[T]>>(table: T, query: Query<Selection.Resolve<S, T>>, cursor?: Driver.Cursor<K>): Promise<Result<S[T], K>[]> {
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

  async eval<K extends Keys<S>, T>(table: K, expr: Selection.Callback<S[K], T>, query?: Query<Selection.Resolve<S, K>>): Promise<T> {
    await this.tasks[table]
    return this.select(table, query).execute(typeof expr === 'function' ? expr : () => expr)
  }

  async set<T extends Keys<S>>(table: T, query: Query<Selection.Resolve<S, T>>, update: Selection.Yield<S[T], Update<S[T]>>) {
    await this.tasks[table]
    const sel = this.select(table, query)
    if (typeof update === 'function') update = update(sel.row)
    const primary = makeArray(sel.model.primary)
    if (primary.some(key => key in update)) {
      throw new TypeError(`cannot modify primary key`)
    }
    await sel._action('set', sel.model.format(update)).execute()
  }

  async remove<T extends Keys<S>>(table: T, query: Query<Selection.Resolve<S, T>>) {
    await this.tasks[table]
    const sel = this.select(table, query)
    await sel._action('remove').execute()
  }

  async create<T extends Keys<S>>(table: T, data: Partial<S[T]>): Promise<S[T]> {
    await this.tasks[table]
    const sel = this.select(table)
    return sel._action('create', sel.model.create(data)).execute()
  }

  async upsert<T extends Keys<S>>(table: T, upsert: Selection.Yield<S[T], Update<S[T]>[]>, keys?: MaybeArray<Keys<Flatten<S[T]>, Indexable>>) {
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

  async dropAll() {
    await Promise.all(Object.values(this.drivers).map(driver => driver.drop()))
  }

  async stats() {
    const stats: Driver.Stats = { size: 0, tables: {} }
    await Promise.all(Object.values(this.drivers).map(async (driver) => {
      const { size, tables } = await driver.stats()
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
  abstract drop(): Promise<void>
  abstract stats(): Promise<Driver.Stats>
  abstract prepare(name: string): Promise<void>
  abstract get(sel: Selection.Immutable, modifier: Modifier): Promise<any>
  abstract eval(sel: Selection.Immutable, expr: Eval.Expr): Promise<any>
  abstract set(sel: Selection.Mutable, data: Update): Promise<void>
  abstract remove(sel: Selection.Mutable): Promise<void>
  abstract create(sel: Selection.Mutable, data: any): Promise<any>
  abstract upsert(sel: Selection.Mutable, data: any[], keys: string[]): Promise<void>

  constructor(public database: Database) {}

  model<S = any>(table: string | Selection<S>): Model<S> {
    if (typeof table === 'string') {
      const model = this.database.tables[table]
      if (model) return model
      throw new TypeError(`unknown table name "${table}"`)
    }

    if (!table.args[0].fields) return table.model
    const model = new Model('temp')
    model.fields = valueMap(table.args[0].fields, () => ({
      type: 'expr',
    }))
    return model
  }
}
