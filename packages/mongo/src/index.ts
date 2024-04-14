import { BSONType, ClientSession, Collection, Db, IndexDescription, Long, MongoClient, MongoClientOptions, MongoError } from 'mongodb'
import { Binary, Dict, isNullable, makeArray, mapValues, noop, omit, pick } from 'cosmokit'
import { Driver, Eval, executeUpdate, Query, RuntimeError, Selection, z } from 'minato'
import { URLSearchParams } from 'url'
import { Builder } from './builder'

const tempKey = '__temp_minato_mongo__'

export class MongoDriver extends Driver<MongoDriver.Config> {
  static name = 'mongo'

  public client!: MongoClient
  public db!: Db
  public mongo = this

  private builder: Builder = new Builder(this, [])
  private session?: ClientSession
  private _createTasks: Dict<Promise<void>> = {}

  private connectionStringFromConfig() {
    const {
      authDatabase,
      connectOptions,
      host = 'localhost',
      database,
      password,
      protocol = 'mongodb',
      port = protocol.includes('srv') ? null : 27017,
      username,
    } = this.config

    let mongourl = `${protocol}://`
    if (username) mongourl += `${encodeURIComponent(username)}${password ? `:${encodeURIComponent(password)}` : ''}@`
    mongourl += `${host}${port ? `:${port}` : ''}/${authDatabase || database}`
    if (connectOptions) {
      const params = new URLSearchParams(connectOptions)
      mongourl += `?${params}`
    }
    return mongourl
  }

  async start() {
    const url = this.config.uri || this.connectionStringFromConfig()
    this.client = await MongoClient.connect(url, pick(this.config, [
      'writeConcern',
    ]))
    this.db = this.client.db(this.config.database)

    this.define<ArrayBuffer, ArrayBuffer>({
      types: ['binary'],
      dump: value => isNullable(value) ? value : Buffer.from(value),
      load: (value: any) => isNullable(value) ? value : Binary.fromSource(value.buffer),
    })

    this.define<bigint, number | Long>({
      types: ['bigint'],
      dump: value => isNullable(value) ? value : value as any,
      load: value => isNullable(value) ? value : BigInt(value as any),
    })
  }

  stop() {
    return this.client?.close()
  }

  /**
   * https://www.mongodb.com/docs/manual/indexes/
   */
  private async _createIndexes(table: string) {
    const { primary, unique } = this.model(table)
    const coll = this.db.collection(table)
    const newSpecs: IndexDescription[] = []
    const oldSpecs = await coll.indexes()

    ;[primary, ...unique].forEach((keys, index) => {
      // use internal `_id` for single primary fields
      if (primary === keys && !index && this.getVirtualKey(table)) return

      // if the index is already created, skip it
      keys = makeArray(keys)
      const name = (index ? 'unique:' : 'primary:') + keys.join('+')
      if (oldSpecs.find(spec => spec.name === name)) return

      newSpecs.push({
        name,
        key: Object.fromEntries(keys.map(key => [key, 1])),
        unique: true,
        // https://www.mongodb.com/docs/manual/core/index-partial/#partial-indexes
        // mongodb seems to not support $ne in partialFilterExpression
        // so we cannot simply use `{ $ne: null }` to filter out null values
        // below is a workaround for https://github.com/koishijs/koishi/issues/893
        partialFilterExpression: Object.fromEntries(keys.map((key) => [key, {
          $type: [BSONType.date, BSONType.int, BSONType.long, BSONType.string, BSONType.objectId],
        }])),
      })
    })

    if (!newSpecs.length) return
    await coll.createIndexes(newSpecs)
  }

  private async _createFields(table: string) {
    const { fields } = this.model(table)
    const coll = this.db.collection(table)
    const bulk = coll.initializeOrderedBulkOp()
    const virtualKey = this.getVirtualKey(table)
    for (const key in fields) {
      if (virtualKey === key) continue
      const { initial, legacy = [], deprecated } = fields[key]!
      if (deprecated) continue
      const filter = { [key]: { $exists: false } }
      for (const oldKey of legacy) {
        bulk
          .find({ ...filter, [oldKey]: { $exists: true } })
          .update({ $rename: { [oldKey]: key } })
        filter[oldKey] = { $exists: false }
      }
      bulk.find(filter).update({ $set: { [key]: initial ?? null } })
      if (legacy.length) {
        const $unset = Object.fromEntries(legacy.map(key => [key, '']))
        bulk.find({}).update({ $unset })
      }
    }
    if (bulk.batches.length) await bulk.execute()
  }

  private async _migrateVirtual(table: string) {
    const { primary, fields: modelFields } = this.model(table)
    if (Array.isArray(primary)) return
    const fields = this.db.collection('_fields')
    const meta: Dict = { table, field: primary }
    const found = await fields.findOne(meta)
    let virtual = !!found?.virtual
    const useVirtualKey = !!this.getVirtualKey(table)
    // If  _fields table was missing for any reason
    // Test the type of _id to get its possible preference
    if (!found) {
      const doc = await this.db.collection(table).findOne()
      if (doc) {
        virtual = typeof doc._id !== 'object' || (typeof primary === 'string' && modelFields[primary]?.deftype === 'primary')
      } else {
        // Empty collection, just set meta and return
        fields.updateOne(meta, { $set: { virtual: useVirtualKey } }, { upsert: true })
        this.logger.info('Successfully reconfigured table %s', table)
        return
      }
    }
    if (virtual === useVirtualKey) return
    this.logger.info('Start migrating table %s', table)

    if (found?.migrate && await this.db.listCollections({ name: '_migrate_' + table }).hasNext()) {
      this.logger.info('Last time crashed, recover')
    } else {
      await this.db.dropCollection('_migrate_' + table).catch(noop)
      await this.db.collection(table).aggregate([
        { $addFields: { _temp_id: '$_id' } },
        { $unset: ['_id'] },
        { $addFields: useVirtualKey ? { _id: '$' + primary } : { [primary]: '$_temp_id' } },
        { $unset: ['_temp_id', ...useVirtualKey ? [primary] : []] },
        { $out: '_migrate_' + table },
      ]).toArray()
      await fields.updateOne(meta, { $set: { migrate: true } }, { upsert: true })
    }
    await this.db.dropCollection(table).catch(noop)
    await this.db.renameCollection('_migrate_' + table, table)
    await fields.updateOne(meta,
      { $set: { virtual: useVirtualKey, migrate: false } },
      { upsert: true },
    )
    this.logger.info('Successfully migrated table %s', table)
  }

  private async _migratePrimary(table: string) {
    const { primary, autoInc } = this.model(table)
    if (Array.isArray(primary) || !autoInc) return
    const fields = this.db.collection('_fields')
    const meta: Dict = { table, field: primary }
    const found = await fields.findOne(meta)
    if (!isNullable(found?.autoInc)) return

    const coll = this.db.collection(table)
    // Primary _id cannot be modified thus should always meet the requirements
    if (!this.getVirtualKey(table)) {
      const bulk = coll.initializeOrderedBulkOp()
      await coll.find().forEach((data) => {
        bulk
          .find({ [primary]: data[primary] })
          .update({ $set: { [primary]: +data[primary] } })
      })
      if (bulk.batches.length) await bulk.execute()
    }

    const [latest] = await coll.find().sort(this.getVirtualKey(table) ? '_id' : primary, -1).limit(1).toArray()
    await fields.updateOne(meta, {
      $set: { autoInc: latest ? +latest[this.getVirtualKey(table) ? '_id' : primary] : 0 },
    }, { upsert: true })
  }

  private _internalTableTask?: Promise<Collection<Document>>

  async _createInternalTable() {
    return this._internalTableTask ||= this.db.createCollection('_fields').catch(noop)
  }

  /** synchronize table schema */
  async prepare(table: string) {
    await Promise.all([
      this._createInternalTable(),
      this.db.createCollection(table).catch(noop),
    ])

    await this._migrateVirtual(table)
    await Promise.all([
      this._createIndexes(table),
      this._createFields(table),
      this._migratePrimary(table),
    ])

    const $unset = {}
    this.migrate(table, {
      error: this.logger.warn,
      before: () => true,
      after: keys => keys.forEach(key => $unset[key] = ''),
      finalize: async () => {
        if (!Object.keys($unset).length) return
        const coll = this.db.collection(table)
        await coll.updateMany({}, { $unset })
      },
    })
  }

  async drop(table: string) {
    await this.db.dropCollection(table, { session: this.session })
  }

  async dropAll() {
    await Promise.all([
      '_fields',
      ...Object.keys(this.database.tables),
    ].map(name => this.db.dropCollection(name, { session: this.session })))
  }

  private async _collStats() {
    const tables = Object.keys(this.database.tables)
    const entries = await Promise.all(tables.map(async (name) => {
      const coll = this.db.collection(name)
      const [{ storageStats: { count, size } }] = await coll.aggregate([{
        $collStats: { storageStats: {} },
      }], { session: this.session }).toArray()
      return [coll.collectionName, { count, size }] as const
    }))
    return Object.fromEntries(entries)
  }

  async stats() {
    // https://docs.mongodb.com/manual/reference/command/dbStats/#std-label-dbstats-output
    const [stats, tables] = await Promise.all([
      this.db.stats(),
      this._collStats(),
    ])
    // while mongodb's document above says that the `stats.totalSize` is the sum of
    // `stats.dataSize` and `stats.storageSize`, it's actually `undefined` in some cases
    // so we have to calculate it manually.
    const totalSize = stats.indexSize + stats.storageSize
    return { size: totalSize, tables }
  }

  public getVirtualKey(table: string) {
    const { primary, fields } = this.model(table)
    if (typeof primary === 'string' && (this.config.optimizeIndex || fields[primary]?.deftype === 'primary')) {
      return primary
    }
  }

  private patchVirtual(table: string, row: any) {
    const { primary, fields } = this.model(table)
    if (typeof primary === 'string' && (this.config.optimizeIndex || fields[primary]?.deftype === 'primary')) {
      row[primary] = row['_id']
      delete row['_id']
    }
    return row
  }

  private unpatchVirtual(table: string, row: any) {
    const { primary, fields } = this.model(table)
    if (typeof primary === 'string' && (this.config.optimizeIndex || fields[primary]?.deftype === 'primary')) {
      row['_id'] = row[primary]
      delete row[primary]
    }
    return row
  }

  private transformQuery(sel: Selection.Immutable, query: Query.Expr, table: string) {
    return new Builder(this, Object.keys(sel.tables), this.getVirtualKey(table)).query(query)
  }

  async get(sel: Selection.Immutable) {
    const transformer = new Builder(this, Object.keys(sel.tables)).select(sel)
    if (!transformer) return []
    this.logger.debug('%s %s', transformer.table, JSON.stringify(transformer.pipeline))
    return this.db
      .collection(transformer.table)
      .aggregate(transformer.pipeline, { allowDiskUse: true, session: this.session })
      .toArray().then(rows => rows.map(row => this.builder.load(row, sel.model)))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const transformer = new Builder(this, Object.keys(sel.tables)).select(sel)
    if (!transformer) return
    this.logger.debug('%s %s', transformer.table, JSON.stringify(transformer.pipeline))
    const res = await this.db
      .collection(transformer.table)
      .aggregate(transformer.pipeline, { allowDiskUse: true, session: this.session })
      .toArray()
    return this.builder.load(res.length ? res[0][transformer.evalKey!] : transformer.aggrDefault, expr)
  }

  async set(sel: Selection.Mutable, update: {}) {
    const { query, table, model } = sel
    const filter = this.transformQuery(sel, query, table)
    if (!filter) return {}
    const coll = this.db.collection(table)

    const transformer = new Builder(this, Object.keys(sel.tables), this.getVirtualKey(table), '$' + tempKey + '.')
    const $set = this.builder.formatUpdateAggr(model.getType(), mapValues(this.builder.dump(update, model),
      (value: any) => typeof value === 'string' && value.startsWith('$') ? { $literal: value } : transformer.eval(value)))
    const $unset = Object.entries($set)
      .filter(([_, value]) => typeof value === 'object')
      .map(([key, _]) => key)
    const preset = Object.fromEntries(transformer.walkedKeys.map(key => [tempKey + '.' + key, '$' + key]))

    const result = await coll.updateMany(filter, [
      ...transformer.walkedKeys.length ? [{ $set: preset }] : [],
      ...$unset.length ? [{ $unset }] : [],
      { $set },
      ...transformer.walkedKeys.length ? [{ $unset: [tempKey] }] : [],
    ], { session: this.session })
    return { matched: result.matchedCount, modified: result.modifiedCount }
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.transformQuery(sel, query, table)
    if (!filter) return {}
    const result = await this.db.collection(table).deleteMany(filter, { session: this.session })
    return { matched: result.deletedCount, removed: result.deletedCount }
  }

  private shouldEnsurePrimary(table: string) {
    const model = this.model(table)
    const { primary, autoInc } = model
    return typeof primary === 'string' && autoInc && model.fields[primary]?.deftype !== 'primary'
  }

  private shouldFillPrimary(table: string) {
    const model = this.model(table)
    const { primary, autoInc } = model
    return typeof primary === 'string' && autoInc && model.fields[primary]?.deftype === 'primary'
  }

  private async ensurePrimary(table: string, data: any[]) {
    const model = this.model(table)
    const { primary, autoInc } = model
    if (typeof primary === 'string' && autoInc && model.fields[primary]?.deftype !== 'primary') {
      const missing = data.filter(item => !(primary in item))
      if (!missing.length) return
      const doc = await this.db.collection('_fields').findOneAndUpdate(
        { table, field: primary },
        { $inc: { autoInc: missing.length } },
        { session: this.session, upsert: true },
      )
      for (let i = 1; i <= missing.length; i++) {
        missing[i - 1][primary] = (doc!.autoInc ?? 0) + i
      }
    }
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table, model } = sel
    const lastTask = Promise.resolve(this._createTasks[table]).catch(noop)
    return this._createTasks[table] = lastTask.then(async () => {
      const coll = this.db.collection(table)
      await this.ensurePrimary(table, [data])

      try {
        const copy = this.unpatchVirtual(table, { ...this.builder.dump(data, model) })
        const insertedId = (await coll.insertOne(copy, { session: this.session })).insertedId
        if (this.shouldFillPrimary(table)) {
          return { ...data, [model.primary as string]: insertedId }
        } else return data
      } catch (err) {
        if (err instanceof MongoError && err.code === 11000) {
          throw new RuntimeError('duplicate-entry', err.message)
        }
        throw err
      }
    })
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return {}
    const { table, ref, model } = sel
    const coll = this.db.collection(table)

    // If ensure primary, we must figure out number of insertions
    if (this.shouldEnsurePrimary(table)) {
      const original = (await coll.find({
        $or: data.map((item) => {
          return this.transformQuery(sel, pick(item, keys), table)!
        }),
      }, { session: this.session }).toArray()).map(row => this.patchVirtual(table, row))

      const bulk = coll.initializeUnorderedBulkOp()
      const insertion: any[] = []
      for (const update of data) {
        const item = original.find(item => keys.every(key => item[key]?.valueOf() === update[key]?.valueOf()))
        if (item) {
          const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
          const override = this.builder.dump(omit(pick(executeUpdate(item, update, ref), updateFields), keys), model)
          const query = this.transformQuery(sel, pick(item, keys), table)
          if (!query) continue
          bulk.find(query).updateOne({ $set: override })
        } else {
          insertion.push(update)
        }
      }
      await this.ensurePrimary(table, insertion)
      for (const update of insertion) {
        const copy = this.builder.dump(executeUpdate(model.create(), update, ref), model)
        bulk.insert(this.unpatchVirtual(table, copy))
      }
      const result = await bulk.execute({ session: this.session })
      return { inserted: result.insertedCount + result.upsertedCount, matched: result.matchedCount, modified: result.modifiedCount }
    } else {
      const bulk = coll.initializeUnorderedBulkOp()
      const initial = model.create()
      const hasInitial = !!Object.keys(initial).length

      for (const update of data) {
        const query = this.transformQuery(sel, pick(update, keys), table)!
        const transformer = new Builder(this, Object.keys(sel.tables), this.getVirtualKey(table), '$' + tempKey + '.')
        const $set = this.builder.formatUpdateAggr(model.getType(), mapValues(this.builder.dump(update, model),
          (value: any) => typeof value === 'string' && value.startsWith('$') ? { $literal: value } : transformer.eval(value)))
        const $unset = Object.entries($set)
          .filter(([_, value]) => typeof value === 'object')
          .map(([key, _]) => key)
        const preset = Object.fromEntries(transformer.walkedKeys.map(key => [tempKey + '.' + key, {
          $ifNull: ['$' + key, initial[key]],
        }]))

        bulk.find(query).upsert().updateOne([
          ...transformer.walkedKeys.length ? [{ $set: preset }] : [],
          ...hasInitial ? [{ $replaceRoot: { newRoot: { $mergeObjects: [initial, '$$ROOT'] } } }] : [],
          ...$unset.length ? [{ $unset }] : [],
          { $set },
          ...transformer.walkedKeys.length ? [{ $unset: [tempKey] }] : [],
        ])
      }
      const result = await bulk.execute({ session: this.session })
      return { inserted: result.insertedCount + result.upsertedCount, matched: result.matchedCount, modified: result.modifiedCount }
    }
  }

  async withTransaction(callback: (session: this) => Promise<void>) {
    await this.client.withSession(async (session) => {
      const driver = new Proxy(this, {
        get(target, p, receiver) {
          if (p === 'session') return session
          else return Reflect.get(target, p, receiver)
        },
      })
      await session.withTransaction(async () => callback(driver)).catch(async e => {
        if (e instanceof MongoError && e.code === 20 && e.message.includes('Transaction numbers')) {
          this.logger.warn(`MongoDB is currently running as standalone server, transaction is disabled.
Convert to replicaSet to enable the feature.
See https://www.mongodb.com/docs/manual/tutorial/convert-standalone-to-replica-set/`)
          await callback(this)
          return
        }
        throw e
      })
    })
  }
}

export namespace MongoDriver {
  export interface Config extends MongoClientOptions {
    username?: string
    password?: string
    protocol?: string
    host?: string
    port?: number
    /** database name */
    database?: string
    /** default auth database */
    authDatabase?: string
    connectOptions?: ConstructorParameters<typeof URLSearchParams>[0]
    /** connection string (will overwrite all configs except 'name') */
    uri?: string
    /**
     * store single primary key in `_id` field to enhance index performance
     * @default false
     */
    optimizeIndex?: boolean
  }

  export const Config: z<Config> = z.object({
    protocol: z.string().default('mongodb'),
    host: z.string().default('localhost'),
    port: z.natural().max(65535),
    username: z.string(),
    password: z.string().role('secret'),
    database: z.string().required(),
    writeConcern: z.object({
      w: z.union([
        z.const(undefined),
        z.number().required(),
        z.const('majority').required(),
      ]),
      wtimeoutMS: z.number(),
      journal: z.boolean(),
    }),
  }).i18n({
    'en-US': require('./locales/en-US'),
    'zh-CN': require('./locales/zh-CN'),
  })
}

export default MongoDriver
