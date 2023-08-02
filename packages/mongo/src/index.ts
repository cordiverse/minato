import { BSONType, Collection, Db, IndexDescription, MongoClient, MongoError } from 'mongodb'
import { Dict, isNullable, makeArray, noop, omit, pick } from 'cosmokit'
import { Database, Driver, Eval, executeEval, executeUpdate, Query, RuntimeError, Selection } from '@minatojs/core'
import { URLSearchParams } from 'url'
import { Transformer } from './utils'
import Logger from 'reggol'

const logger = new Logger('mongo')
const tempKey = '__temp_minato_mongo__'

export namespace MongoDriver {
  export interface Config {
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
}

interface Result {
  table: string
  pipeline: any[]
}

interface EvalTask extends Result {
  expr: any
  resolve: (value: any) => void
  reject: (reason: unknown) => void
}

export class MongoDriver extends Driver {
  public client!: MongoClient
  public db!: Db
  public mongo = this

  private _evalTasks: EvalTask[] = []
  private _createTasks: Dict<Promise<void>> = {}

  constructor(database: Database, private config: MongoDriver.Config) {
    super(database)
  }

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
    this.client = await MongoClient.connect(url)
    this.db = this.client.db(this.config.database)
  }

  stop() {
    return this.client.close()
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
      if (this.config.optimizeIndex && !index && typeof keys === 'string') return

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
    const { primary } = this.model(table)
    if (Array.isArray(primary)) return
    const fields = this.db.collection('_fields')
    const meta: Dict = { table, field: primary }
    const found = await fields.findOne(meta)
    let virtual = !!found?.virtual
    // If  _fields table was missing for any reason
    // Test the type of _id to get its possible preference
    if (!found) {
      const doc = await this.db.collection(table).findOne()
      if (doc) virtual = typeof doc._id !== 'object'
      else {
        // Empty collection, just set meta and return
        fields.updateOne(meta, { $set: { virtual: this.config.optimizeIndex } }, { upsert: true })
        logger.info('Successfully reconfigured table %s', table)
        return
      }
    }

    if (virtual === !!this.config.optimizeIndex) return
    logger.info('Start migrating table %s', table)

    if (found?.migrate && await this.db.listCollections({ name: '_migrate_' + table }).hasNext()) {
      logger.info('Last time crashed, recover')
    } else {
      await this.db.dropCollection('_migrate_' + table).catch(noop)
      await this.db.collection(table).aggregate([
        { $addFields: { _temp_id: '$_id' } },
        { $unset: ['_id'] },
        { $addFields: this.config.optimizeIndex ? { _id: '$' + primary } : { [primary]: '$_temp_id' } },
        { $unset: ['_temp_id', ...this.config.optimizeIndex ? [primary] : []] },
        { $out: '_migrate_' + table },
      ]).toArray()
      await fields.updateOne(meta, { $set: { migrate: true } }, { upsert: true })
    }
    await this.db.dropCollection(table)
    await this.db.renameCollection('_migrate_' + table, table)
    await fields.updateOne(meta,
      { $set: { virtual: this.config.optimizeIndex, migrate: false } },
      { upsert: true },
    )
    logger.info('Successfully migrated table %s', table)
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
    if (!this.config.optimizeIndex) {
      const bulk = coll.initializeOrderedBulkOp()
      await coll.find().forEach((data) => {
        bulk
          .find({ [primary]: data[primary] })
          .update({ $set: { [primary]: +data[primary] } })
      })
      if (bulk.batches.length) await bulk.execute()
    }

    const [latest] = await coll.find().sort(this.config.optimizeIndex ? '_id' : primary, -1).limit(1).toArray()
    await fields.updateOne(meta, {
      $set: { autoInc: latest ? +latest[this.config.optimizeIndex ? '_id' : primary] : 0 },
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
      error: logger.warn,
      before: () => true,
      after: keys => keys.forEach(key => $unset[key] = ''),
      finalize: async () => {
        if (!Object.keys($unset).length) return
        const coll = this.db.collection(table)
        await coll.updateMany({}, { $unset })
      },
    })
  }

  async drop(table?: string) {
    if (table) {
      await this.db.dropCollection(table)
      return
    }
    await Promise.all([
      '_fields',
      ...Object.keys(this.database.tables),
    ].map(name => this.db.dropCollection(name)))
  }

  private async _collStats() {
    const tables = Object.keys(this.database.tables)
    const entries = await Promise.all(tables.map(async (name) => {
      const coll = this.db.collection(name)
      const { count, size } = await coll.stats()
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

  private getVirtualKey(table: string) {
    const { primary } = this.model(table)
    if (typeof primary === 'string' && this.config.optimizeIndex) {
      return primary
    }
  }

  private patchVirtual(table: string, row: any) {
    const { primary } = this.model(table)
    if (typeof primary === 'string' && this.config.optimizeIndex) {
      row[primary] = row['_id']
      delete row['_id']
    }
    return row
  }

  private unpatchVirtual(table: string, row: any) {
    const { primary } = this.model(table)
    if (typeof primary === 'string' && this.config.optimizeIndex) {
      row['_id'] = row[primary]
      delete row[primary]
    }
    return row
  }

  private transformQuery(query: Query.Expr, table: string) {
    return new Transformer(this.getVirtualKey(table)).query(query)
  }

  private createPipeline(sel: string | Selection.Immutable) {
    if (typeof sel === 'string') {
      sel = this.database.select(sel)
    }

    const { table, query } = sel
    const pipeline: any[] = []
    const result = { pipeline } as Result
    const transformer = new Transformer()
    if (typeof table === 'string') {
      result.table = table
      transformer.virtualKey = this.getVirtualKey(table)
    } else if (table instanceof Selection) {
      const predecessor = this.createPipeline(table)
      if (!predecessor) return
      result.table = predecessor.table
      pipeline.push(...predecessor.pipeline)
    } else {
      for (const [name, subtable] of Object.entries(table)) {
        const predecessor = this.createPipeline(subtable)
        if (!predecessor) return
        if (!result.table) {
          result.table = predecessor.table
          pipeline.push(...predecessor.pipeline, {
            $replaceRoot: { newRoot: { [name]: '$$ROOT' } },
          })
          continue
        }
        const $lookup = {
          from: predecessor.table,
          as: name,
          pipeline: predecessor.pipeline,
        }
        const $unwind = {
          path: `$${name}`,
        }
        pipeline.push({ $lookup }, { $unwind })
        if (sel.args[0].having['$and'].length) {
          transformer.lookup = true
          const $expr = transformer.eval(sel.args[0].having)
          pipeline.push({ $match: { $expr } })
          transformer.lookup = false
        }
      }
    }

    // where
    const filter = transformer.query(query)
    if (!filter) return
    if (Object.keys(filter).length) {
      pipeline.push({ $match: filter })
    }

    if (sel.type === 'get') {
      transformer.modifier(pipeline, sel)
    }

    return result
  }

  async get(sel: Selection.Immutable) {
    const result = this.createPipeline(sel)
    if (!result) return []
    logger.debug('%s %s', result.table, JSON.stringify(result.pipeline))
    return this.db
      .collection(result.table)
      .aggregate(result.pipeline, { allowDiskUse: true })
      .toArray()
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const result = this.createPipeline(sel)
    if (!result) return
    return new Promise<any>((resolve, reject) => {
      this._evalTasks.push({ expr, ...result, resolve, reject })
      process.nextTick(() => this._flushEvalTasks())
    })
  }

  private async _flushEvalTasks() {
    const tasks = this._evalTasks
    if (!tasks.length) return
    this._evalTasks = []

    const stages: any[] = [{ $match: { _id: null } }]
    const transformer = new Transformer()
    for (const task of tasks) {
      const { expr, table, pipeline } = task
      const $ = transformer.createKey()
      const $group: Dict = { _id: null }
      const $project: Dict = { _id: 0 }
      pipeline.push({ $group }, { $project })
      task.expr = { $ }
      $project[$] = transformer.eval(expr, $group)
      stages.push({
        $unionWith: { coll: table, pipeline },
      })
    }

    let data: any
    try {
      const results = await this.db
        .collection('_fields')
        .aggregate(stages, { allowDiskUse: true })
        .toArray()
      data = Object.assign({}, ...results)
    } catch (error) {
      tasks.forEach(task => task.reject(error))
      return
    }

    for (const { expr, resolve, reject } of tasks) {
      try {
        resolve(executeEval({ _: data }, expr))
      } catch (error) {
        reject(error)
      }
    }
  }

  async set(sel: Selection.Mutable, update: {}) {
    const { query, table } = sel
    const filter = this.transformQuery(query, table)
    if (!filter) return
    const coll = this.db.collection(table)

    const transformer = new Transformer(this.getVirtualKey(table), undefined, '$' + tempKey + '.')
    const $set = transformer.eval(update)
    const $unset = Object.entries($set)
      .filter(([_, value]) => typeof value === 'object')
      .map(([key, _]) => key)
    const preset = Object.fromEntries(transformer.walkedKeys.map(key => [tempKey + '.' + key, '$' + key]))

    await coll.updateMany(filter, [
      ...transformer.walkedKeys.length ? [{ $set: preset }] : [],
      ...$unset.length ? [{ $unset }] : [],
      { $set },
      ...transformer.walkedKeys.length ? [{ $unset: [tempKey] }] : [],
    ])
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.transformQuery(query, table)
    if (!filter) return
    await this.db.collection(table).deleteMany(filter)
  }

  private shouldEnsurePrimary(table: string) {
    const model = this.model(table)
    const { primary, autoInc } = model
    return typeof primary === 'string' && autoInc
  }

  private async ensurePrimary(table: string, data: any[]) {
    const model = this.model(table)
    const { primary, autoInc } = model
    if (typeof primary === 'string' && autoInc) {
      const missing = data.filter(item => !(primary in item))
      if (!missing.length) return
      const { value } = await this.db.collection('_fields').findOneAndUpdate(
        { table, field: primary },
        { $inc: { autoInc: missing.length } },
        { upsert: true },
      )
      for (let i = 1; i <= missing.length; i++) {
        missing[i - 1][primary] = (value!.autoInc ?? 0) + i
      }
    }
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table } = sel
    const lastTask = Promise.resolve(this._createTasks[table]).catch(noop)
    return this._createTasks[table] = lastTask.then(async () => {
      const model = this.model(table)
      const coll = this.db.collection(table)
      await this.ensurePrimary(table, [data])
      try {
        data = model.create(data)
        const copy = this.unpatchVirtual(table, { ...data })
        await coll.insertOne(copy)
        return data
      } catch (err) {
        if (err instanceof MongoError && err.code === 11000) {
          throw new RuntimeError('duplicate-entry', err.message)
        }
        throw err
      }
    })
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return
    const { table, ref, model } = sel
    const coll = this.db.collection(table)

    // If ensure primary, we must figure out number of insertions
    if (this.shouldEnsurePrimary(table)) {
      const original = (await coll.find({
        $or: data.map((item) => {
          return this.transformQuery(pick(item, keys), table)
        }),
      }).toArray()).map(row => this.patchVirtual(table, row))

      const bulk = coll.initializeUnorderedBulkOp()
      const insertion: any[] = []
      for (const update of data) {
        const item = original.find(item => keys.every(key => item[key]?.valueOf() === update[key]?.valueOf()))
        if (item) {
          const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
          const override = omit(pick(executeUpdate(item, update, ref), updateFields), keys)
          const query = this.transformQuery(pick(item, keys), table)
          if (!query) continue
          bulk.find(query).updateOne({ $set: override })
        } else {
          insertion.push(update)
        }
      }
      await this.ensurePrimary(table, insertion)
      for (const update of insertion) {
        const copy = executeUpdate(model.create(), update, ref)
        bulk.insert(this.unpatchVirtual(table, copy))
      }
      await bulk.execute()
    } else {
      const bulk = coll.initializeUnorderedBulkOp()
      const initial = model.create()
      const hasInitial = !!Object.keys(initial).length

      for (const update of data) {
        const query = this.transformQuery(pick(update, keys), table)!
        const transformer = new Transformer(this.getVirtualKey(table), undefined, '$' + tempKey + '.')
        const $set = transformer.eval(update)
        const $unset = Object.entries($set)
          .filter(([_, value]) => typeof value === 'object')
          .map(([key, _]) => key)
        const preset = Object.fromEntries(transformer.walkedKeys.map(key => [tempKey + '.' + key, '$' + key]))

        bulk.find(query).upsert().updateOne([
          ...transformer.walkedKeys.length ? [{ $set: preset }] : [],
          ...hasInitial ? [{ $replaceRoot: { newRoot: { $mergeObjects: [initial, '$$ROOT'] } } }] : [],
          ...$unset.length ? [{ $unset }] : [],
          { $set },
          ...transformer.walkedKeys.length ? [{ $unset: [tempKey] }] : [],
        ])
      }
      await bulk.execute()
    }
  }
}

export default MongoDriver
