import { Db, IndexDescription, MongoClient, MongoError } from 'mongodb'
import { Dict, isNullable, makeArray, MaybeArray, noop, omit, pick } from 'cosmokit'
import { Database, Driver, Eval, Executable, executeEval, executeUpdate, Field, Modifier, Query, RuntimeError } from 'cosmotype'
import { URLSearchParams } from 'url'
import { transformEval, transformQuery } from './utils'

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
    super(database, 'mongo')
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
    super.start()
  }

  stop() {
    super.stop()
    return this.client.close()
  }

  private async _createIndexes(name: string) {
    const { primary, unique } = this.model(name)
    const coll = this.db.collection(name)
    const newSpecs: IndexDescription[] = []
    const oldSpecs = await coll.indexes()
    ;[primary, ...unique].forEach((keys: MaybeArray<string>, index) => {
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

  private async _createFields(name: string) {
    const { fields } = this.model(name)
    const coll = this.db.collection(name)
    await Promise.all(Object.keys(fields).map((key) => {
      if (isNullable(fields[key].initial)) return
      return coll.updateMany({ [key]: { $exists: false } }, { $set: { [key]: fields[key].initial } })
    }))
  }

  /** synchronize table schema */
  async prepare(name: string) {
    await this.db.createCollection(name).catch(noop)
    await Promise.all([
      this._createIndexes(name),
      this._createFields(name),
    ])
  }

  async drop() {
    await Promise.all(Object.keys(this.database.tables).map(name => this.db.dropCollection(name)))
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
    // While MongoDB's document above says that the `stats.totalSize` is the sum of
    // `stats.dataSize` and `stats.storageSize`, it's actually `undefined` in some cases.
    // So we have to calculate it manually.
    const totalSize = stats.indexSize + stats.storageSize
    return { size: totalSize, tables }
  }

  async get(sel: Executable, modifier: Modifier) {
    const { table, fields, query } = sel
    const { offset, limit, sort } = modifier
    const filter = transformQuery(query)
    if (!filter) return []
    let cursor = this.db.collection(table).find(filter)
    if (limit < Infinity) {
      cursor = cursor.limit(offset + limit)
    }
    cursor = cursor.skip(offset)
    cursor = cursor.sort(Object.fromEntries(sort.map(([k, v]) => [k['$'][1], v === 'desc' ? -1 : 1])))
    const data = await cursor.toArray()
    return data.map(row => sel.resolveData(row, fields))
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
    for (const task of tasks) {
      const { expr, table, query } = task
      task.expr = transformEval(expr, (pipeline) => {
        const filter = transformQuery(query) || { _id: null }
        pipeline.unshift({ $match: filter })
        stages.push({ $unionWith: { coll: table, pipeline } })
      })
    }

    let data: any
    try {
      const results = await this.db.collection('user').aggregate(stages).toArray()
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
    const filter = transformQuery(query)
    if (!filter) return
    const indexFields = makeArray(sel.model.primary)
    const coll = this.db.collection(table)
    const original = await coll.find(filter).toArray()
    if (!original.length) return
    const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
    const bulk = coll.initializeUnorderedBulkOp()
    for (const item of original) {
      bulk.find(pick(item, indexFields)).updateOne({
        $set: pick(executeUpdate(item, update, ref), updateFields),
      })
    }
    await bulk.execute()
  }

  async remove(sel: Executable) {
    const { query, table } = sel
    const filter = transformQuery(query)
    if (!filter) return
    await this.db.collection(table).deleteMany(filter)
  }

  async create(sel: Executable, data: any) {
    const { table } = sel
    return this._createTasks[table] = Promise.resolve(this._createTasks[table]).catch(noop).then(async () => {
      const model = this.model(table)
      const coll = this.db.collection(table)
      const { primary, fields, autoInc } = model
      if (autoInc && !Array.isArray(primary) && !(primary in data)) {
        const [latest] = await coll.find().sort(primary, -1).limit(1).toArray()
        data[primary] = latest ? +latest[primary] + 1 : 1
        if (Field.string.includes(fields[primary].type)) {
          data[primary] += ''
          data[primary] = data[primary].padStart(8, '0')
        }
      }
      const copy = model.create(data)
      try {
        await coll.insertOne(copy)
        delete copy['_id']
        return copy
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
    const original = await coll.find({ $or: data.map(item => pick(item, keys)) }).toArray()
    const bulk = coll.initializeUnorderedBulkOp()
    for (const update of data) {
      const item = original.find(item => keys.every(key => item[key].valueOf() === update[key].valueOf()))
      if (item) {
        const updateFields = new Set(Object.keys(update).map(key => key.split('.', 1)[0]))
        const override = omit(pick(executeUpdate(item, update, ref), updateFields), keys)
        bulk.find(pick(item, keys)).updateOne({ $set: override })
      } else {
        bulk.insert(executeUpdate(model.create(), update, ref))
      }
    }
    await bulk.execute()
  }
}

export default MongoDriver
