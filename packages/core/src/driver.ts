import { Awaitable, deepEqual, defineProperty, Dict, mapValues, remove } from 'cosmokit'
import { Context, Service } from 'cordis'
import { Eval, Update } from './eval.ts'
import { Direction, Modifier, Selection } from './selection.ts'
import { Field, Model, Relation } from './model.ts'
import { Database } from './database.ts'
import { Type } from './type.ts'
import { FlatKeys, Keys, Values } from './utils.ts'

export namespace Driver {
  export interface Stats {
    size: number
    tables: Dict<TableStats>
  }

  export interface TableStats {
    count: number
    size: number
  }

  export type Cursor<K extends string = string, S = any, T extends Keys<S> = any> = K[] | CursorOptions<K, S, T>

  export interface CursorOptions<K extends string = string, S = any, T extends Keys<S> = any> {
    limit?: number
    offset?: number
    fields?: K[]
    sort?: Partial<Dict<Direction, FlatKeys<S[T]>>>
    include?: Relation.Include<S[T], Values<S>>
  }

  export interface WriteResult {
    inserted?: number
    matched?: number
    modified?: number
    removed?: number
  }

  export interface IndexDef<K extends string = string> {
    name?: string
    keys: { [P in K]?: 'asc' | 'desc' }
  }

  export interface Index<K extends string = string> extends IndexDef<K> {
    unique?: boolean
  }

  export interface Transformer<S = any, T = any> {
    types: Field.Type<S>[]
    dump: (value: S | null) => T | null | void
    load: (value: T | null) => S | null | void
  }
}

export namespace Driver {
  export type Constructor<T> = new (ctx: Context, config: T) => Driver<T>
}

export abstract class Driver<T = any, C extends Context = Context> {
  static inject = ['minato']

  abstract start(): Promise<void>
  abstract stop(): Promise<void>
  abstract drop(table: string): Promise<void>
  abstract dropAll(): Promise<void>
  abstract stats(): Promise<Partial<Driver.Stats>>
  abstract prepare(name: string): Promise<void>
  abstract get(sel: Selection.Immutable, modifier: Modifier): Promise<any>
  abstract eval(sel: Selection.Immutable, expr: Eval.Expr): Promise<any>
  abstract set(sel: Selection.Mutable, data: Update): Promise<Driver.WriteResult>
  abstract remove(sel: Selection.Mutable): Promise<Driver.WriteResult>
  abstract create(sel: Selection.Mutable, data: any): Promise<any>
  abstract upsert(sel: Selection.Mutable, data: any[], keys: string[]): Promise<Driver.WriteResult>
  abstract withTransaction(callback: (session?: any) => Promise<void>): Promise<void>
  abstract getIndexes(table: string): Promise<Driver.Index[]>
  abstract createIndex(table: string, index: Driver.Index): Promise<void>
  abstract dropIndex(table: string, name: string): Promise<void>

  public types: Dict<Driver.Transformer> = Object.create(null)

  constructor(public ctx: C, public config: T) {}

  async* [Context.init]() {
    this.ctx.minato.drivers.push(this)
    yield () => remove(this.ctx.minato.drivers, this)
    this.ctx.minato.refresh()
    const database: Database = Object.create(this.ctx.minato) // FIXME use original model
    defineProperty(database, 'ctx', this.ctx)
    defineProperty(database, Service.tracker, {
      associate: 'database',
      property: 'ctx',
    })
    defineProperty(database, '_driver', this)
    this.ctx.set('database', database)
  }

  model<S = any>(table: string | Selection.Immutable | Dict<string | Selection.Immutable>): Model<S> {
    if (typeof table === 'string') {
      const model = this.ctx.minato.tables[table]
      if (model) return model
      throw new TypeError(`unknown table name "${table}"`)
    }

    if (Selection.is(table)) {
      if (!table.args[0].fields && (typeof table.table === 'string' || Selection.is(table.table))) {
        return table.model
      }
      const model = new Model('temp')
      if (table.args[0].fields) {
        model.fields = mapValues(table.args[0].fields, (expr) => ({
          type: Type.fromTerm(expr),
        }))
      } else {
        model.fields = mapValues(table.model.fields, (field) => ({
          type: Type.fromField(field),
        }))
      }
      return model
    }

    const model = new Model('temp')
    for (const key in table) {
      const submodel = this.model(table[key])
      for (const field in submodel.fields) {
        if (!Field.available(submodel.fields[field])) continue
        model.fields[`${key}.${field}`] = {
          expr: Eval('', [table[key].ref, field], Type.fromField(submodel.fields[field]!)),
          type: Type.fromField(submodel.fields[field]!),
        }
      }
    }
    return model
  }

  protected async migrate(name: string, hooks: MigrationHooks) {
    const database = this.ctx.minato.makeProxy(Database.migrate)
    const model = this.model(name)
    await (database.migrateTasks[name] = Promise.resolve(database.migrateTasks[name]).then(() => {
      return Promise.all([...model.migrations].map(async ([migrate, keys]) => {
        try {
          if (!hooks.before(keys)) return
          await migrate(database)
          hooks.after(keys)
        } catch (reason) {
          hooks.error(reason)
        }
      }))
    }).then(hooks.finalize).catch(hooks.error))
  }

  define<S, T>(converter: Driver.Transformer<S, T>) {
    converter.types.forEach(type => this.types[type] = converter)
  }

  async _ensureSession() {}

  async prepareIndexes(table: string) {
    const oldIndexes = await this.getIndexes(table)
    const { indexes } = this.model(table)
    for (const index of indexes) {
      const oldIndex = oldIndexes.find(info => info.name === index.name)
      if (!oldIndex) {
        await this.createIndex(table, index)
      } else if (!deepEqual(oldIndex, index)) {
        await this.dropIndex(table, index.name!)
        await this.createIndex(table, index)
      }
    }
  }
}

export interface MigrationHooks {
  before: (keys: string[]) => boolean
  after: (keys: string[]) => void
  finalize: () => Awaitable<void>
  error: (reason: any) => void
}
