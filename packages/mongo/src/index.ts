import { Collection, Db, IndexDescription, MongoClient, MongoError } from 'mongodb'
import { Dict, isNullable, makeArray, noop, omit, pick } from 'cosmokit'
import { Database, Driver, Eval, Executable, executeEval, executeUpdate, Modifier, Query, RuntimeError } from '@minatojs/core'
import { URLSearchParams } from 'url'
import { Transformer } from './utils'

namespace MongoDriver {
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

interface EvalTask {
  expr: any
  table: string
  query: Query.Expr
  resolve: (value: any) => void
  reject: (error: Error) => void
}

class MongoDriver extends Driver {
  public client: MongoClient
  public db: Db
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
        // https://docs.mongodb.com/manual/core/index-partial/#std-label-partial-index-with-unique-constraints
        partialFilterExpression: Object.fromEntries(keys.map(key => [key, { $exists: true }])),
      })
    })

    if (!newSpecs.length) return
    await coll.createIndexes(newSpecs)
  }

  private async _createFields(table: string) {
    const { fields } = this.model(table)
    const coll = this.db.collection(table)
    await Promise.all(Object.keys(fields).map((key) => {
      if (isNullable(fields[key].initial)) return
      return coll.updateMany({ [key]: { $exists: false } }, { $set: { [key]: fields[key].initial as never } })
    }))
  }

  private async _migratePrimary(table: string) {
    const { primary, autoInc } = this.model(table)
    if (Array.isArray(primary) || !autoInc) return
    const fields = this.db.collection('_fields')
    const meta: Dict = { table, field: primary }
    const found = await fields.findOne(meta)
    if (found) return

    const coll = this.db.collection(table)
    const bulk = coll.initializeOrderedBulkOp()
    await coll.find().forEach((data) => {
      bulk
        .find({ [primary]: data[primary] })
        .update({ $set: { [primary]: +data[primary] } })
    })
    if (bulk.batches.length) await bulk.execute()

    const [latest] = await coll.find().sort(primary, -1).limit(1).toArray()
    meta.autoInc = latest ? +latest[primary] : 0
    await fields.insertOne(meta)
  }

  private _internalTableTask: Promise<Collection<Document>>

  async _createInternalTable() {
    return this._internalTableTask ||= this.db.createCollection('_fields').catch(noop)
  }

  /** synchronize table schema */
  async prepare(table: string) {
    await Promise.all([
      this._createInternalTable(),
      this.db.createCollection(table).catch(noop)
    ])
    await Promise.all([
      this._createIndexes(table),
      this._createFields(table),
      this._migratePrimary(table),
    ])
  }

  async drop() {
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

  async get(sel: Executable, modifier: Modifier) {
    const { table, fields, query } = sel
    const { offset, limit, sort } = modifier
    const transformer = new Transformer(this.getVirtualKey(table))
    const filter = transformer.query(query)
    if (!filter) return []
    const pipeline: any[] = []
    if (Object.keys(filter).length) {
      pipeline.push({ $match: filter })
    }
    const $set = {}
    const $sort = {}
    const $unset = []
    for (const [expr, dir] of sort) {
      const value = transformer.eval(expr)
      if (typeof value === 'string') {
        $sort[value.slice(1)] = dir === 'desc' ? -1 : 1
      } else {
        const key = transformer.createKey()
        $set[key] = value
        $sort[key] = dir === 'desc' ? -1 : 1
        $unset.push(key)
      }
    }
    if ($unset.length) pipeline.push({ $set })
    if (Object.keys($sort).length) pipeline.push({ $sort })
    if ($unset.length) pipeline.push({ $unset })
    if (limit < Infinity) {
      pipeline.push({ $limit: offset + limit })
    }
    if (offset) {
      pipeline.push({ $skip: offset })
    }
    const data = await this.db
      .collection(table)
      .aggregate(pipeline, { allowDiskUse: true })
      .toArray()
    return data.map((row) => {
      row = this.patchVirtual(table, row)
      return sel.resolveData(row, fields)
    })
  }

  async eval(sel: Executable, expr: Eval.Expr) {
    const { table, query } = sel
    return new Promise<any>((resolve, reject) => {
      this._evalTasks.push({ expr, table, query, resolve, reject })
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
      const { expr, table, query } = task
      transformer.virtualKey = this.getVirtualKey(table)
      task.expr = transformer.eval(expr, (pipeline) => {
        const filter = transformer.query(query) || { _id: null }
        if (Object.keys(filter).length) {
          pipeline.unshift({ $match: filter })
        }
        stages.push({ $unionWith: { coll: table, pipeline } })
      })
    }

    let data: any
    try {
      const results = await this.db
        .collection('user')
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

  async set(sel: Executable, update: {}) {
    const { query, table, ref } = sel
    const filter = this.transformQuery(query, table)
    if (!filter) return
    const indexFields = makeArray(sel.model.primary)
    const coll = this.db.collection(table)
    const original = await coll.find(filter).toArray()
    if (!original.length) return

    const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
    const bulk = coll.initializeUnorderedBulkOp()
    for (const item of original) {
      const row = this.patchVirtual(table, item)
      const query = this.transformQuery(pick(row, indexFields), table)
      bulk.find(query).updateOne({
        $set: pick(executeUpdate(row, update, ref), updateFields),
      })
    }
    await bulk.execute()
  }

  async remove(sel: Executable) {
    const { query, table } = sel
    const filter = this.transformQuery(query, table)
    if (!filter) return
    await this.db.collection(table).deleteMany(filter)
  }

  async create(sel: Executable, data: any) {
    const { table } = sel
    const lastTask = Promise.resolve(this._createTasks[table]).catch(noop)
    return this._createTasks[table] = lastTask.then(async () => {
      const model = this.model(table)
      const coll = this.db.collection(table)
      const { primary, autoInc } = model

      if (typeof primary === 'string' && !(primary in data)) {
        if (autoInc) {
          const { value } = await this.db.collection('_fields').findOneAndUpdate(
            { table, field: primary },
            { $inc: { autoInc: 1 } },
            { upsert: true, returnDocument: 'after' },
          )
          data[primary] = value.autoInc
        }
      }

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

  async upsert(sel: Executable, data: any[], keys: string[]) {
    if (!data.length) return
    const { table, ref, model } = sel
    const coll = this.db.collection(table)
    const original = (await coll.find({
      $or: data.map((item) => {
        return this.transformQuery(pick(item, keys), table)
      }),
    }).toArray()).map(row => this.patchVirtual(table, row))

    const bulk = coll.initializeUnorderedBulkOp()
    for (const update of data) {
      const item = original.find(item => keys.every(key => item[key].valueOf() === update[key].valueOf()))
      if (item) {
        const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
        const override = omit(pick(executeUpdate(item, update, ref), updateFields), keys)
        const query = this.transformQuery(pick(item, keys), table)
        bulk.find(query).updateOne({ $set: override })
      } else {
        const copy = executeUpdate(model.create(), update, ref)
        bulk.insert(this.unpatchVirtual(table, copy))
      }
    }
    await bulk.execute()
  }
}

export default MongoDriver
