import { Awaitable, Dict, valueMap } from 'cosmokit'
import { Context, Logger } from 'cordis'
import { Eval, Update } from './eval.ts'
import { Direction, Modifier, Selection } from './selection.ts'
import { Model } from './model.ts'
import { Database } from './database.ts'

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
    matched?: number
    modified?: number
    removed?: number
  }
}

export namespace Driver {
  export type Constructor<T = any> = new (database: Database, config?: T) => Driver
}

export abstract class Driver<C = any> {
  static inject = ['model']

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
  abstract withTransaction(callback: (driver: Driver) => Promise<void>): Promise<void>

  public database: Database
  public logger: Logger

  constructor(public ctx: Context, public config: C) {
    this.database = ctx.model
    this.logger = ctx.logger(this.constructor.name)

    ctx.on('ready', async () => {
      await this.start()
      ctx.model.drivers.default = this
      ctx.model.refresh()
      const database = Object.create(ctx.model)
      ctx.database = database
    })

    ctx.on('dispose', async () => {
      ctx.database = null as never
      delete ctx.model.drivers.default
      await this.stop()
    })
  }

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
