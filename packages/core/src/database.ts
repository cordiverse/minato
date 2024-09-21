import { deduplicate, defineProperty, Dict, filterKeys, isNullable, makeArray, mapValues, MaybeArray, noop, omit, pick, remove } from 'cosmokit'
import { Context, Service, Spread } from 'cordis'
import { AtomicTypes, DeepPartial, FlatKeys, FlatPick, Flatten, getCell, Indexable, Keys, randomId, Row, unravel, Values } from './utils.ts'
import { Selection } from './selection.ts'
import { Field, Model, Relation } from './model.ts'
import { Driver } from './driver.ts'
import { Eval, isUpdateExpr, Update } from './eval.ts'
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

type CreateUnit<T, S> =
  | T extends Values<AtomicTypes> ? T
  : T extends (infer I extends Values<S>)[] ? Create<I, S>[] |
    {
      $literal?: DeepPartial<I>
      $create?: MaybeArray<Create<I, S>>
      $upsert?: MaybeArray<Create<I, S>>
      $connect?: Query.Expr<Flatten<I>>
    }
  : T extends Values<S> ? Create<T, S> |
    {
      $literal?: DeepPartial<T>
      $create?: Create<T, S>
      $upsert?: Create<T, S>
      $connect?: Query.Expr<Flatten<T>>
    }
  : T extends (infer U)[] ? DeepPartial<U>[]
  : T extends object ? Create<T, S>
  : T

export type Create<T, S> = { [K in keyof T]?: CreateUnit<T[K], S> }

function mergeQuery<T>(base: Query.FieldExpr<T>, query: Query.Expr<Flatten<T>> | ((row: Row<T>) => Query.Expr<Flatten<T>>)): Selection.Callback<T, boolean> {
  if (typeof query === 'function') {
    return (row: any) => {
      const q = query(row)
      return { $expr: true, ...base, ...(q.$expr ? q : { $expr: q }) } as any
    }
  } else {
    return (_: any) => ({ $expr: true, ...base, ...query }) as any
  }
}

export class Database<S = {}, N = {}, C extends Context = Context> extends Service<undefined, C> {
  static [Service.provide] = 'model'
  static [Service.immediate] = true

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
    if (Selection.is(table)) return table.driver as any
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
    await driver.prepareIndexes(name)
  }

  extend<K extends Keys<S>>(name: K, fields: Field.Extension<S[K], N>, config: Partial<Model.Config<FlatKeys<S[K]>>> = {}) {
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
      defineProperty(model, 'ctx', this.ctx)
    }
    Object.entries(fields).forEach(([key, def]: [string, Relation.Definition]) => {
      if (!Relation.Type.includes(def.type)) return
      const subprimary = !def.fields && makeArray(model.primary).includes(key)
      const [relation, inverse] = Relation.parse(def, key, model, this.tables[def.table ?? key], subprimary)
      const relmodel = this.tables[relation.table]
      if (!relmodel) throw new Error(`relation table ${relation.table} does not exist`)
      ;(model.fields[key] = Field.parse('expr')).relation = relation
      if (def.target) {
        (relmodel.fields[def.target] ??= Field.parse('expr')).relation = inverse
      }

      if (relation.type === 'oneToOne' || relation.type === 'manyToOne') {
        relation.fields.forEach((x, i) => {
          model.fields[x] ??= { ...relmodel.fields[relation.references[i]] } as any
          if (!relation.required) {
            model.fields[x]!.nullable = true
            model.fields[x]!.initial = null
          }
        })
      } else if (relation.type === 'manyToMany') {
        const assocTable = Relation.buildAssociationTable(relation.table, name)
        if (this.tables[assocTable]) return
        const shared = Object.entries(relation.shared).map(([x, y]) => [Relation.buildSharedKey(x, y), model.fields[x]!.deftype] as const)
        const fields = relation.fields.map(x => [Relation.buildAssociationKey(x, name), model.fields[x]!.deftype] as const)
        const references = relation.references.map(x => [Relation.buildAssociationKey(x, relation.table), relmodel.fields[x]?.deftype] as const)
        this.extend(assocTable as any, {
          ...Object.fromEntries([...shared, ...fields, ...references]),
          [name]: {
            type: 'manyToOne',
            table: name,
            fields: [...shared, ...fields].map(x => x[0]),
            references: [...Object.keys(relation.shared), ...relation.fields],
          },
          [relation.table]: {
            type: 'manyToOne',
            table: relation.table,
            fields: [...shared, ...references].map(x => x[0]),
            references: [...Object.values(relation.shared), ...relation.references],
          },
        } as any, {
          primary: [...shared, ...fields, ...references].map(x => x[0]) as any,
        })
      }
    })
    // use relation field as primary
    if (Array.isArray(model.primary) || model.fields[model.primary]!.relation) {
      model.primary = deduplicate(makeArray(model.primary).map(key => model.fields[key]!.relation?.fields || key).flat())
    }
    model.unique = model.unique.map(keys => typeof keys === 'string' ? model.fields[keys]!.relation?.fields || keys
      : keys.map(key => model.fields[key]!.relation?.fields || key).flat())

    this.prepareTasks[name] = this.prepare(name)
    ;(this.ctx as Context).emit('model', name)
  }

  private _parseField(field: any, transformers: Driver.Transformer[] = [], setInitial?: (value) => void, setField?: (value) => void): Type {
    if (field === 'object') {
      setInitial?.({})
      setField?.({ initial: {}, deftype: 'json', type: Type.Object() })
      return Type.Object()
    } else if (field === 'array') {
      setInitial?.([])
      setField?.({ initial: [], deftype: 'json', type: Type.Array() })
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

    this.ctx.effect(() => {
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
  select<K extends Keys<S>>(
    table: K,
    query?: Query<S[K]>,
    include?: Relation.Include<S[K], Values<S>> | null,
  ): Selection<S[K]>

  select(table: any, query?: any, include?: any) {
    let sel = new Selection(this.getDriver(table), table, query)
    if (typeof table !== 'string') return sel
    const whereOnly = include === null, isAssoc = !!include?.$assoc
    const rawquery = typeof query === 'function' ? query : () => query
    const modelFields = this.tables[table].fields
    if (include) include = filterKeys(include, (key) => !!modelFields[key]?.relation)
    for (const key in { ...sel.query, ...sel.query.$not }) {
      if (modelFields[key]?.relation) {
        if (sel.query[key] === null && !modelFields[key].relation.required) {
          sel.query[key] = Object.fromEntries(modelFields[key]!.relation!.references.map(k => [k, null]))
        }
        if (sel.query[key] && typeof sel.query[key] !== 'function' && typeof sel.query[key] === 'object'
          && Object.keys(sel.query[key]).every(x => modelFields[key]!.relation!.fields.includes(`${key}.${x}`))) {
          Object.entries(sel.query[key]).forEach(([k, v]) => sel.query[`${key}.${k}`] = v)
          delete sel.query[key]
        }
        if (sel.query.$not?.[key] === null && !modelFields[key].relation.required) {
          sel.query.$not[key] = Object.fromEntries(modelFields[key]!.relation!.references.map(k => [k, null]))
        }
        if (sel.query.$not?.[key] && typeof sel.query.$not[key] !== 'function' && typeof sel.query.$not[key] === 'object'
          && Object.keys(sel.query.$not[key]).every(x => modelFields[key]!.relation!.fields.includes(`${key}.${x}`))) {
          Object.entries(sel.query.$not[key]).forEach(([k, v]) => sel.query.$not![`${key}.${k}`] = v)
          delete sel.query.$not[key]
        }
        if (!include || !Object.getOwnPropertyNames(include).includes(key)) {
          (include ??= {})[key] = true
        }
      }
    }

    sel.query = omit(sel.query, Object.keys(include ?? {}))
    if (Object.keys(sel.query.$not ?? {}).length) {
      sel.query.$not = omit(sel.query.$not!, Object.keys(include ?? {}))
      if (Object.keys(sel.query.$not).length === 0) Reflect.deleteProperty(sel.query, '$not')
    }

    if (include && typeof include === 'object') {
      if (typeof table !== 'string') throw new Error('cannot include relations on derived selection')
      const extraFields: string[] = []
      const applyQuery = (sel: Selection, key: string) => {
        const query2 = rawquery(sel.row)
        const relquery = query2[key] !== undefined ? query2[key]
          : query2.$not?.[key] !== undefined ? { $not: query2.$not?.[key] }
            : undefined
        return relquery === undefined ? sel : sel.where(this.transformRelationQuery(table, sel.row, key, relquery))
      }
      for (const key in include) {
        if (!include[key] || !modelFields[key]?.relation) continue
        const relation: Relation.Config<S> = modelFields[key]!.relation as any
        const relmodel = this.tables[relation.table]
        if (relation.type === 'oneToOne' || relation.type === 'manyToOne') {
          sel = whereOnly ? sel : sel.join(key, this.select(relation.table,
            typeof include[key] === 'object' ? filterKeys(include[key], (k) => !relmodel.fields[k]?.relation) : {} as any,
            typeof include[key] === 'object' ? filterKeys(include[key], (k) => !!relmodel.fields[k]?.relation) : include[key],
          ), (self, other) => Eval.and(
            ...relation.fields.map((k, i) => Eval.eq(self[k], other[relation.references[i]])),
          ), !isAssoc)
          sel = applyQuery(sel, key)
        } else if (relation.type === 'oneToMany') {
          sel = whereOnly ? sel : sel.join(key, this.select(relation.table,
            typeof include[key] === 'object' ? filterKeys(include[key], (k) => !relmodel.fields[k]?.relation) : {} as any,
            typeof include[key] === 'object' ? filterKeys(include[key], (k) => !!relmodel.fields[k]?.relation) : include[key],
          ), (self, other) => Eval.and(
            ...relation.fields.map((k, i) => Eval.eq(self[k], other[relation.references[i]])),
          ), true)
          sel = applyQuery(sel, key)
          sel = whereOnly ? sel : sel.groupBy([
            ...Object.entries(modelFields).filter(([k, field]) => !extraFields.some(x => k.startsWith(`${x}.`)) && Field.available(field)).map(([k]) => k),
            ...extraFields,
          ], {
            [key]: row => Eval.ignoreNull(Eval.array(row[key])),
          })
        } else if (relation.type === 'manyToMany') {
          const assocTable: any = Relation.buildAssociationTable(relation.table, table)
          const references = relation.fields.map(x => Relation.buildAssociationKey(x, table))
          const shared = Object.entries(relation.shared).map(([x, y]) => [Relation.buildSharedKey(x, y), {
            field: x,
            reference: y,
          }] as const)
          sel = whereOnly ? sel : sel.join(key, this.select(assocTable, {}, { $assoc: true, [relation.table]: include[key] } as any),
            (self, other) => Eval.and(
              ...shared.map(([k, v]) => Eval.eq(self[v.field], other[k])),
              ...relation.fields.map((k, i) => Eval.eq(self[k], other[references[i]])),
            ), true)
          sel = applyQuery(sel, key)
          sel = whereOnly ? sel : sel.groupBy([
            ...Object.entries(modelFields).filter(([k, field]) => !extraFields.some(x => k.startsWith(`${x}.`)) && Field.available(field)).map(([k]) => k),
            ...extraFields,
          ], {
            [key]: row => Eval.ignoreNull(Eval.array(row[key][relation.table as any])),
          })
        }
        extraFields.push(key)
      }
    }
    return sel
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
    const drivers = new Set(Object.values(sels).map(sel => sel.driver[Database.transact] ?? sel.driver))
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

  async get<K extends Keys<S>>(table: K, query: Query<S[K]>): Promise<S[K][]>

  async get<K extends Keys<S>, P extends FlatKeys<S[K]> = any>(
    table: K,
    query: Query<S[K]>,
    cursor?: Driver.Cursor<P, S, K>,
  ): Promise<FlatPick<S[K], P>[]>

  async get<K extends Keys<S>>(table: K, query: Query<S[K]>, cursor?: any) {
    let fields = Array.isArray(cursor) ? cursor : cursor?.fields
    fields = fields ? Object.fromEntries(fields.map(x => [x, true])) : cursor?.include
    return this.select(table, query, fields).execute(cursor) as any
  }

  async eval<K extends Keys<S>, T>(table: K, expr: Selection.Callback<S[K], T, true>, query?: Query<S[K]>): Promise<T> {
    return this.select(table, query).execute(typeof expr === 'function' ? expr : () => expr)
  }

  async set<K extends Keys<S>>(
    table: K,
    query: Query<S[K]>,
    update: Row.Computed<S[K], Update<S[K]>>,
  ): Promise<Driver.WriteResult> {
    const rawupdate = typeof update === 'function' ? update : () => update
    let sel = this.select(table, query, null)
    if (typeof update === 'function') update = update(sel.row)
    const primary = makeArray(sel.model.primary)
    if (primary.some(key => key in update)) {
      throw new TypeError(`cannot modify primary key`)
    }

    const relations: [string, Relation.Config<S>][] = Object.entries(sel.model.fields)
      .filter(([key, field]) => key in update && field!.relation)
      .map(([key, field]) => [key, field!.relation!] as const) as any
    if (relations.length) {
      return await this.ensureTransaction(async (database) => {
        const rows = await database.get(table, query)
        sel = database.select(table, query, null)
        let baseUpdate = omit(rawupdate(sel.row), relations.map(([key]) => key) as any)
        baseUpdate = sel.model.format(baseUpdate)
        for (const [key] of relations) {
          await Promise.all(rows.map(row => database.processRelationUpdate(table, row, key, rawupdate(row as any)[key])))
        }
        return Object.keys(baseUpdate).length === 0 ? {} : await sel._action('set', baseUpdate).execute()
      })
    }

    update = sel.model.format(update)
    if (Object.keys(update).length === 0) return {}
    return sel._action('set', update).execute()
  }

  async remove<K extends Keys<S>>(table: K, query: Query<S[K]>): Promise<Driver.WriteResult> {
    const sel = this.select(table, query, null)
    return sel._action('remove').execute()
  }

  async create<K extends Keys<S>>(table: K, data: Create<S[K], S>): Promise<S[K]>
  async create<K extends Keys<S>>(table: K, data: any): Promise<S[K]> {
    const sel = this.select(table)

    if (!this.hasRelation(table, data)) {
      const { primary, autoInc } = sel.model
      if (!autoInc) {
        const keys = makeArray(primary)
        if (keys.some(key => getCell(data, key) === undefined)) {
          throw new Error('missing primary key')
        }
      }
      return sel._action('create', sel.model.create(data)).execute()
    } else {
      return this.ensureTransaction(database => database.createOrUpdate(table, data, false))
    }
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
    return sel._action('upsert', upsert, keys).execute()
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

  async transact<T>(callback: (database: this) => Promise<T>) {
    if (this[Database.transact]) throw new Error('nested transactions are not supported')
    const finalTasks: Promise<void>[] = []
    const database = this.makeProxy(Database.transact, (driver) => {
      let initialized = false, session: any
      let _resolve: (value: any) => void
      const sessionTask = new Promise((resolve) => _resolve = resolve)
      driver = new Proxy(driver, {
        get: (target, p, receiver) => {
          if (p === Database.transact) return target
          if (p === 'database') return database
          if (p === 'session') return session
          if (p === '_ensureSession') return () => sessionTask
          return Reflect.get(target, p, receiver)
        },
      })
      finalTasks.push(driver.withTransaction((_session) => {
        if (initialized) initialTask = initialTaskFactory()
        initialized = true
        _resolve(session = _session)
        return initialTask as any
      }))
      return driver
    })
    const initialTaskFactory = () => Promise.resolve().then(() => callback(database))
    let initialTask = initialTaskFactory()
    return initialTask.catch(noop).finally(() => Promise.all(finalTasks))
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

  private ensureTransaction<T>(callback: (database: this) => Promise<T>) {
    if (this[Database.transact]) {
      return callback(this)
    } else {
      return this.transact(callback)
    }
  }

  private transformRelationQuery(table: any, row: any, key: any, query: Query.FieldExpr) {
    const relation: Relation.Config<S> = this.tables[table].fields[key]!.relation! as any
    const results: Eval.Expr<boolean>[] = []
    if (relation.type === 'oneToOne' || relation.type === 'manyToOne') {
      if (query === null) {
        results.push(Eval.nin(
          relation.fields.map(x => row[x]),
          this.select(relation.table).evaluate(relation.references),
        ))
      } else {
        results.push(Eval.in(
          relation.fields.map(x => row[x]),
          this.select(relation.table, query as any).evaluate(relation.references),
        ))
      }
    } else if (relation.type === 'oneToMany') {
      if (query.$or) results.push(Eval.or(...query.$or.map((q: any) => this.transformRelationQuery(table, row, key, q).$expr)))
      if (query.$and) results.push(...query.$and.map((q: any) => this.transformRelationQuery(table, row, key, q).$expr))
      if (query.$not) results.push(Eval.not(this.transformRelationQuery(table, row, key, query.$not).$expr))
      if (query.$some) {
        results.push(Eval.in(
          relation.fields.map(x => row[x]),
          this.select(relation.table, query.$some).evaluate(relation.references),
        ))
      }
      if (query.$none) {
        results.push(Eval.nin(
          relation.fields.map(x => row[x]),
          this.select(relation.table, query.$none).evaluate(relation.references),
        ))
      }
      if (query.$every) {
        results.push(Eval.nin(
          relation.fields.map(x => row[x]),
          this.select(relation.table, Eval.not(query.$every as any) as any).evaluate(relation.references),
        ))
      }
    } else if (relation.type === 'manyToMany') {
      const assocTable: any = Relation.buildAssociationTable(table, relation.table)
      const fields: any[] = relation.fields.map(x => Relation.buildAssociationKey(x, table))
      const references = relation.references.map(x => Relation.buildAssociationKey(x, relation.table))
      if (query.$or) results.push(Eval.or(...query.$or.map((q: any) => this.transformRelationQuery(table, row, key, q).$expr)))
      if (query.$and) results.push(...query.$and.map((q: any) => this.transformRelationQuery(table, row, key, q).$expr))
      if (query.$not) results.push(Eval.not(this.transformRelationQuery(table, row, key, query.$not).$expr))
      if (query.$some) {
        const innerTable = this.select(relation.table, query.$some).evaluate(relation.references)
        const relTable = this.select(assocTable, r => Eval.in(references.map(x => r[x]), innerTable)).evaluate(fields)
        results.push(Eval.in(relation.fields.map(x => row[x]), relTable))
      }
      if (query.$none) {
        const innerTable = this.select(relation.table, query.$none).evaluate(relation.references)
        const relTable = this.select(assocTable, r => Eval.in(references.map(x => r[x]), innerTable)).evaluate(fields)
        results.push(Eval.nin(relation.fields.map(x => row[x]), relTable))
      }
      if (query.$every) {
        const innerTable = this.select(relation.table, Eval.not(query.$every as any) as any).evaluate(relation.references)
        const relTable = this.select(assocTable, r => Eval.in(references.map(x => r[x]), innerTable)).evaluate(fields)
        results.push(Eval.nin(relation.fields.map(x => row[x]), relTable))
      }
    }
    return { $expr: Eval.and(...results) } as any
  }

  private async createOrUpdate<K extends Keys<S>>(table: K, data: any, upsert: boolean = true): Promise<S[K]> {
    const sel = this.select(table)
    data = { ...data }
    const tasks = ['']
    for (const key in data) {
      if (data[key] !== undefined && this.tables[table].fields[key]?.relation) {
        const relation = this.tables[table].fields[key].relation
        if (relation.type === 'oneToOne' && relation.required) tasks.push(key)
        else if (relation.type === 'oneToOne') tasks.unshift(key)
        else if (relation.type === 'oneToMany') tasks.push(key)
        else if (relation.type === 'manyToOne') tasks.unshift(key)
        else if (relation.type === 'manyToMany') tasks.push(key)
      }
    }

    for (const key of [...tasks]) {
      if (!key) {
        // create the plain data, with or without upsert
        const { primary, autoInc } = sel.model
        const keys = makeArray(primary)
        if (keys.some(key => isNullable(getCell(data, key)))) {
          if (!autoInc) {
            throw new Error('missing primary key')
          } else {
            // nullable relation may pass null here, remove it to enable autoInc
            delete data[primary as string]
            upsert = false
          }
        }
        if (upsert) {
          await sel._action('upsert', [sel.model.format(omit(data, tasks))], keys).execute()
        } else {
          Object.assign(data, await sel._action('create', sel.model.create(omit(data, tasks))).execute())
        }
        continue
      }
      const value = data[key]
      const relation: Relation.Config<S> = this.tables[table].fields[key]!.relation! as any
      if (relation.type === 'oneToOne') {
        if (value.$literal) {
          data[key] = value.$literal
          remove(tasks, key)
        } else if (value.$create || !isUpdateExpr(value)) {
          const result = await this.createOrUpdate(relation.table, {
            ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])),
            ...value.$create ?? value,
          } as any)
          if (!relation.required) {
            relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(result, k))
          }
        } else if (value.$upsert) {
          await this.upsert(relation.table, [{
            ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])),
            ...value.$upsert,
          }])
          if (!relation.required) {
            relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(value.$upsert, k))
          }
        } else if (value.$connect) {
          if (relation.required) {
            await this.set(relation.table,
              value.$connect,
              Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])) as any,
            )
          } else {
            const result = relation.references.every(k => value.$connect![k as any] !== undefined) ? [value.$connect]
              : await this.get(relation.table, value.$connect as any)
            if (result.length !== 1) throw new Error('related row not found or not unique')
            relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(result[0], k))
          }
        }
      } else if (relation.type === 'manyToOne') {
        if (value.$literal) {
          data[key] = value.$literal
          remove(tasks, key)
        } else if (value.$create || !isUpdateExpr(value)) {
          const result = await this.createOrUpdate(relation.table, value.$create ?? value)
          relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(result, k))
        } else if (value.$upsert) {
          await this.upsert(relation.table, [value.$upsert])
          relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(value.$upsert, k))
        } else if (value.$connect) {
          const result = relation.references.every(k => value.$connect![k as any] !== undefined) ? [value.$connect]
            : await this.get(relation.table, value.$connect as any)
          if (result.length !== 1) throw new Error('related row not found or not unique')
          relation.references.forEach((k, i) => data[relation.fields[i]] = getCell(result[0], k))
        }
      } else if (relation.type === 'oneToMany') {
        if (value.$create || Array.isArray(value)) {
          for (const item of makeArray(value.$create ?? value)) {
            await this.createOrUpdate(relation.table, {
              ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])),
              ...item,
            })
          }
        }
        if (value.$upsert) {
          await this.upsert(relation.table, makeArray(value.$upsert).map(r => ({
            ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])),
            ...r,
          })))
        }
        if (value.$connect) {
          await this.set(relation.table,
            value.$connect,
            Object.fromEntries(relation.references.map((k, i) => [k, getCell(data, relation.fields[i])])) as any,
          )
        }
      } else if (relation.type === 'manyToMany') {
        const assocTable = Relation.buildAssociationTable(relation.table, table)
        const fields = relation.fields.map(x => Relation.buildAssociationKey(x, table))
        const references = relation.references.map(x => Relation.buildAssociationKey(x, relation.table))
        const shared = Object.entries(relation.shared).map(([x, y]) => [Relation.buildSharedKey(x, y), {
          field: x,
          reference: y,
        }] as const)
        const result: any[] = []
        if (value.$create || Array.isArray(value)) {
          for (const item of makeArray(value.$create ?? value)) {
            result.push(await this.createOrUpdate(relation.table, {
              ...Object.fromEntries(shared.map(([, v]) => [v.reference, getCell(item, v.reference) ?? getCell(data, v.field)])),
              ...item,
            }))
          }
        }
        if (value.$upsert) {
          const upsert = makeArray(value.$upsert).map(r => ({
            ...Object.fromEntries(shared.map(([, v]) => [v.reference, getCell(r, v.reference) ?? getCell(data, v.field)])),
            ...r,
          }))
          await this.upsert(relation.table, upsert)
          result.push(...upsert)
        }
        if (value.$connect) {
          for (const item of makeArray(value.$connect)) {
            if (references.every(k => item[k] !== undefined)) result.push(item)
            else result.push(...await this.get(relation.table, item))
          }
        }
        await this.upsert(assocTable as any, result.map(r => ({
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(r, v.reference) ?? getCell(data, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, getCell(data, relation.fields[i])])),
          ...Object.fromEntries(references.map((k, i) => [k, getCell(r, relation.references[i])])),
        } as any)))
      }
    }
    return data
  }

  private async processRelationUpdate(table: any, row: any, key: any, value: Relation.Modifier) {
    const model = this.tables[table], update = Object.create(null)
    const relation: Relation.Config<S> = this.tables[table].fields[key]!.relation! as any
    if (relation.type === 'oneToOne') {
      if (value === null) {
        value = relation.required ? { $remove: {} } : { $disconnect: {} }
      }
      if (typeof value === 'object' && !isUpdateExpr(value)) {
        value = { $create: value }
      }
      if (value.$remove) {
        await this.remove(relation.table, Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])) as any)
      }
      if (value.$disconnect) {
        if (relation.required) {
          await this.set(relation.table,
            mergeQuery(Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])), value.$disconnect),
            Object.fromEntries(relation.references.map((k, i) => [k, null])) as any,
          )
        } else {
          Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, null])))
        }
      }
      if (value.$set || typeof value === 'function') {
        await this.set(
          relation.table,
          Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
          value.$set ?? value as any,
        )
      }
      if (value.$create) {
        const result = await this.createOrUpdate(relation.table, {
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...value.$create,
        })
        if (!relation.required) {
          Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(result, relation.references[i])])))
        }
      }
      if (value.$upsert) {
        await this.upsert(relation.table, makeArray(value.$upsert).map(r => ({
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...r,
        })))
        if (!relation.required) {
          Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(value.$upsert, relation.references[i])])))
        }
      }
      if (value.$connect) {
        if (relation.required) {
          await this.set(relation.table,
            value.$connect,
            Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
          )
        } else {
          const result = await this.get(relation.table, value.$connect as any)
          if (result.length !== 1) throw new Error('related row not found or not unique')
          Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(result[0], relation.references[i])])))
        }
      }
    } else if (relation.type === 'manyToOne') {
      if (value === null) {
        value = { $disconnect: {} }
      }
      if (typeof value === 'object' && !isUpdateExpr(value)) {
        value = { $create: value }
      }
      if (value.$remove) {
        await this.remove(relation.table, Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])) as any)
      }
      if (value.$disconnect) {
        Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, null])))
      }
      if (value.$set || typeof value === 'function') {
        await this.set(
          relation.table,
          Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
          value.$set ?? value as any,
        )
      }
      if (value.$create) {
        const result = await this.createOrUpdate(relation.table, {
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...value.$create,
        })
        Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(result, relation.references[i])])))
      }
      if (value.$upsert) {
        await this.upsert(relation.table, makeArray(value.$upsert).map(r => ({
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...r,
        })))
        Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(value.$upsert, relation.references[i])])))
      }
      if (value.$connect) {
        const result = await this.get(relation.table, value.$connect)
        if (result.length !== 1) throw new Error('related row not found or not unique')
        Object.assign(update, Object.fromEntries(relation.fields.map((k, i) => [k, getCell(result[0], relation.references[i])])))
      }
    } else if (relation.type === 'oneToMany') {
      if (Array.isArray(value)) {
        const $create: any[] = [], $upsert: any[] = []
        value.forEach(item => this.hasRelation(relation.table, item) ? $create.push(item) : $upsert.push(item))
        value = { $remove: {}, $create, $upsert }
      }
      if (value.$remove) {
        await this.remove(relation.table, mergeQuery(Object.fromEntries(relation.references.map((k, i) => [k, row[relation.fields[i]]])), value.$remove))
      }
      if (value.$disconnect) {
        await this.set(relation.table,
          mergeQuery(Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])), value.$disconnect),
          Object.fromEntries(relation.references.map((k, i) => [k, null])) as any,
        )
      }
      if (value.$set || typeof value === 'function') {
        for (const setexpr of makeArray(value.$set ?? value) as any[]) {
          const [query, update] = setexpr.update ? [setexpr.where, setexpr.update] : [{}, setexpr]
          await this.set(relation.table,
            mergeQuery(Object.fromEntries(relation.references.map((k, i) => [k, row[relation.fields[i]]])), query),
            update,
          )
        }
      }
      if (value.$create) {
        for (const item of makeArray(value.$create)) {
          await this.createOrUpdate(relation.table, {
            ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
            ...item,
          })
        }
      }
      if (value.$upsert) {
        await this.upsert(relation.table, makeArray(value.$upsert).map(r => ({
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...r,
        })))
      }
      if (value.$connect) {
        await this.set(relation.table,
          value.$connect,
          Object.fromEntries(relation.references.map((k, i) => [k, row[relation.fields[i]]])) as any,
        )
      }
    } else if (relation.type === 'manyToMany') {
      const assocTable = Relation.buildAssociationTable(table, relation.table) as Keys<S>
      const fields = relation.fields.map(x => Relation.buildAssociationKey(x, table))
      const references = relation.references.map(x => Relation.buildAssociationKey(x, relation.table))
      const shared = Object.entries(relation.shared).map(([x, y]) => [Relation.buildSharedKey(x, y), {
        field: x,
        reference: y,
      }] as const)
      if (Array.isArray(value)) {
        const $create: any[] = [], $upsert: any[] = []
        value.forEach(item => this.hasRelation(relation.table, item) ? $create.push(item) : $upsert.push(item))
        value = { $disconnect: {}, $create, $upsert }
      }
      if (value.$remove) {
        const rows = await this.select(assocTable, {
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
          [relation.table]: value.$remove,
        }, null).execute()
        await this.remove(assocTable, r => Eval.in(
          [...shared.map(([k, v]) => r[k]), ...fields.map(x => r[x]), ...references.map(x => r[x])],
          rows.map(r => [...shared.map(([k, v]) => getCell(r, k)), ...fields.map(x => getCell(r, x)), ...references.map(x => getCell(r, x))]),
        ))
        await this.remove(relation.table, (r) => Eval.in(
          [...shared.map(([k, v]) => r[v.reference]), ...relation.references.map(x => r[x])],
          rows.map(r => [...shared.map(([k, v]) => getCell(r, k)), ...references.map(x => getCell(r, x))]),
        ))
      }
      if (value.$disconnect) {
        const rows = await this.select(assocTable, {
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
          [relation.table]: value.$disconnect,
        }, null).execute()
        await this.remove(assocTable, r => Eval.in(
          [...shared.map(([k, v]) => r[k]), ...fields.map(x => r[x]), ...references.map(x => r[x])],
          rows.map(r => [...shared.map(([k, v]) => getCell(r, k)), ...fields.map(x => getCell(r, x)), ...references.map(x => getCell(r, x))]),
        ))
      }
      if (value.$set) {
        for (const setexpr of makeArray(value.$set) as any[]) {
          const [query, update] = setexpr.update ? [setexpr.where, setexpr.update] : [{}, setexpr]
          const rows = await this.select(assocTable, (r: any) => ({
            ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
            ...Object.fromEntries(fields.map((k, i) => [k, getCell(row, relation.fields[i])])) as any,
            [relation.table]: query,
          }), null).execute()
          await this.set(relation.table,
            (r) => Eval.in(
              [...shared.map(([k, v]) => r[v.reference]), ...relation.references.map(x => r[x])],
              rows.map(r => [...shared.map(([k, v]) => getCell(r, k)), ...references.map(x => getCell(r, x))]),
            ),
            update,
          )
        }
      }
      if (value.$create) {
        const result: any[] = []
        for (const item of makeArray(value.$create)) {
          result.push(await this.createOrUpdate(relation.table, {
            ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
            ...item,
          }))
        }
        await this.upsert(assocTable, result.map(r => ({
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, row[relation.fields[i]]])),
          ...Object.fromEntries(references.map((k, i) => [k, r[relation.references[i] as any]])),
        })) as any)
      }
      if (value.$upsert) {
        await this.upsert(relation.table, makeArray(value.$upsert).map(r => ({
          ...Object.fromEntries(relation.references.map((k, i) => [k, getCell(row, relation.fields[i])])),
          ...r,
        })))
        await this.upsert(assocTable, makeArray(value.$upsert).map(r => ({
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, row[relation.fields[i]]])),
          ...Object.fromEntries(references.map((k, i) => [k, r[relation.references[i] as any]])),
        })) as any)
      }
      if (value.$connect) {
        const rows = await this.get(relation.table,
          mergeQuery(Object.fromEntries(shared.map(([k, v]) => [v.reference, getCell(row, v.field)])), value.$connect))
        await this.upsert(assocTable, rows.map(r => ({
          ...Object.fromEntries(shared.map(([k, v]) => [k, getCell(row, v.field)])),
          ...Object.fromEntries(fields.map((k, i) => [k, row[relation.fields[i]]])),
          ...Object.fromEntries(references.map((k, i) => [k, r[relation.references[i] as any]])),
        })) as any)
      }
    }
    if (Object.keys(update).length) {
      await this.set(table, pick(model.format(row), makeArray(model.primary)), update)
    }
  }

  private hasRelation<K extends Keys<S>>(table: K, data: Create<S[K], S>): boolean
  private hasRelation(table: any, data: any) {
    for (const key in data) {
      if (data[key] !== undefined && this.tables[table].fields[key]?.relation) return true
    }
    return false
  }
}
