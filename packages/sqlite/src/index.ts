import { Dict, difference, makeArray, union } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, Model, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import { promises as fs } from 'fs'
import init from '@minatojs/sql.js'
import Logger from 'reggol'

const logger = new Logger('sqlite')

function getTypeDefinition({ type }: Field) {
  switch (type) {
    case 'boolean':
    case 'integer':
    case 'unsigned':
    case 'date':
    case 'time':
    case 'timestamp': return `INTEGER`
    case 'float':
    case 'double':
    case 'decimal': return `REAL`
    case 'char':
    case 'string':
    case 'text':
    case 'list':
    case 'json': return `TEXT`
  }
}

export interface SQLiteFieldInfo {
  name: string
  type: string
  notnull: number
  dflt_value: string
  pk: boolean
}

namespace SQLiteDriver {
  export interface Config {
    path: string
  }
}

class SQLiteBuilder extends Builder {
  constructor(tables: Dict<Model>) {
    super(tables)

    this.evalOperators.$if = (args) => `iif(${args.map(arg => this.parseEval(arg)).join(', ')})`

    this.define<boolean, number>({
      types: ['boolean'],
      dump: value => +value,
      load: (value) => !!value,
    })

    this.define<object, string>({
      types: ['json'],
      dump: value => JSON.stringify(value),
      load: (value, initial) => value ? JSON.parse(value) : initial,
    })

    this.define<string[], string>({
      types: ['list'],
      dump: value => value.join(','),
      load: (value) => value ? value.split(',') : [],
    })

    this.define<Date, number>({
      types: ['date', 'time', 'timestamp'],
      dump: value => value === null ? null : +value,
      load: (value) => value === null ? null : new Date(value),
    })
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) value = +value
    return super.escape(value, field)
  }

  protected createElementQuery(key: string, value: any) {
    return `(',' || ${key} || ',') LIKE ${this.escape('%,' + value + ',%')}`
  }
}

class SQLiteDriver extends Driver {
  db!: init.Database
  sql: Builder
  writeTask?: NodeJS.Timeout
  sqlite!: init.SqlJsStatic

  constructor(database: Database, public config: SQLiteDriver.Config) {
    super(database)

    this.sql = new SQLiteBuilder(database.tables)
  }

  private _getColDefs(table: string, key: string) {
    const model = this.model(table)
    const { initial, nullable = true } = model.fields[key]!
    let def = `\`${key}\``
    if (key === model.primary && model.autoInc) {
      def += ' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT'
    } else {
      const typedef = getTypeDefinition(model.fields[key]!)
      def += ' ' + typedef + (nullable ? ' ' : ' NOT ') + 'NULL'
      if (initial !== undefined && initial !== null) {
        def += ' DEFAULT ' + this.sql.escape(this.sql.dump(model, { [key]: initial })[key])
      }
    }
    return def
  }

  /** synchronize table schema */
  async prepare(table: string) {
    const info = this.#all(`PRAGMA table_info(${escapeId(table)})`) as SQLiteFieldInfo[]
    // WARN: side effecting Tables.config
    const config = this.model(table)
    const keys = Object.keys(config.fields)
    if (info.length) {
      let hasUpdate = false
      for (const key of keys) {
        if (info.some(({ name }) => name === key)) continue
        const def = this._getColDefs(table, key)
        this.#run(`ALTER TABLE ${escapeId(table)} ADD COLUMN ${def}`)
        hasUpdate = true
      }
      if (hasUpdate) {
        logger.info('auto updating table %c', table)
      }
    } else {
      logger.info('auto creating table %c', table)
      const defs = keys.map(key => this._getColDefs(table, key))
      const constraints: string[] = []
      if (config.primary && !config.autoInc) {
        constraints.push(`PRIMARY KEY (${this.#joinKeys(makeArray(config.primary))})`)
      }
      if (config.unique) {
        constraints.push(...config.unique.map(keys => `UNIQUE (${this.#joinKeys(makeArray(keys))})`))
      }
      if (config.foreign) {
        constraints.push(...Object.entries(config.foreign).map(([key, value]) => {
          const [table, key2] = value!
          return `FOREIGN KEY (\`${key}\`) REFERENCES ${escapeId(table)} (\`${key2}\`)`
        }))
      }
      this.#run(`CREATE TABLE ${escapeId(table)} (${[...defs, ...constraints].join(',')})`)
    }
  }

  init(buffer: ArrayLike<number> | null) {
    this.db = new this.sqlite.Database(buffer)
    this.db.create_function('regexp', (pattern, str) => +new RegExp(pattern).test(str))
  }

  async start() {
    const [sqlite, buffer] = await Promise.all([
      init(),
      this.config.path === ':memory:' ? null : fs.readFile(this.config.path).catch<Buffer | null>(() => null),
    ])
    this.sqlite = sqlite
    this.init(buffer)
  }

  #joinKeys(keys?: string[]) {
    return keys?.length ? keys.map(key => `\`${key}\``).join(',') : '*'
  }

  async stop() {
    this.db.close()
  }

  #exec(sql: string, params: any, callback: (stmt: init.Statement) => any) {
    try {
      const stmt = this.db.prepare(sql)
      const result = callback(stmt)
      stmt.free()
      return result
    } catch (e) {
      logger.warn('SQL > %c', sql, params)
      throw e
    }
  }

  #all(sql: string, params: any = []) {
    return this.#exec(sql, params, (stmt) => {
      stmt.bind(params)
      const result: any[] = []
      while (stmt.step()) {
        result.push(stmt.getAsObject())
      }
      return result
    })
  }

  #get(sql: string, params: any = []) {
    return this.#exec(sql, params, stmt => stmt.getAsObject(params))
  }

  #run(sql: string, params: any = [], callback?: () => any) {
    this.#exec(sql, params, stmt => stmt.run(params))
    const result = callback?.()
    if (this.config.path) {
      const data = this.db.export()
      const timer = this.writeTask = setTimeout(() => {
        if (this.writeTask !== timer) return
        fs.writeFile(this.config.path, data)
      }, 0)
      this.init(data)
    }
    return result
  }

  async drop() {
    const tables = Object.keys(this.database.tables)
    for (const table of tables) {
      this.#run(`DROP TABLE ${escapeId(table)}`)
    }
  }

  async stats() {
    const size = this.db.export().byteLength
    return { size }
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return
    this.#run(`DELETE FROM ${escapeId(table)} WHERE ${filter}`)
  }

  async get(sel: Selection.Immutable) {
    const { tables } = sel
    const builder = new SQLiteBuilder(tables)
    const sql = builder.get(sel)
    if (!sql) return []
    const rows = this.#all(sql)
    return rows.map(row => this.sql.load(sel.model, row))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const output = this.sql.parseEval(expr)
    let sql = this.sql.get(sel.table as Selection)
    const prefix = `SELECT ${output} AS value `
    if (sql.startsWith('SELECT * ')) {
      sql = prefix + sql.slice(9)
    } else {
      sql = `${prefix}FROM (${sql}) ${sql.ref}`
    }
    const { value } = this.#get(sql)
    return value
  }

  #update(sel: Selection.Mutable, indexFields: string[], updateFields: string[], update: {}, data: {}) {
    const { ref, table } = sel
    const model = this.model(table)
    const row = this.sql.dump(model, executeUpdate(data, update, ref))
    const assignment = updateFields.map((key) => `${escapeId(key)} = ${this.sql.escape(row[key])}`).join(',')
    const query = Object.fromEntries(indexFields.map(key => [key, row[key]]))
    const filter = this.sql.parseQuery(query)
    this.#run(`UPDATE ${escapeId(table)} SET ${assignment} WHERE ${filter}`)
  }

  async set(sel: Selection.Mutable, update: {}) {
    const { model, table, query } = sel
    const { primary, fields } = model
    const updateFields = [...new Set(Object.keys(update).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const primaryFields = makeArray(primary)
    const data = await this.database.get(table, query, union(primaryFields, updateFields) as [])
    for (const row of data) {
      this.#update(sel, primaryFields, updateFields, update, row)
    }
  }

  #create(table: string, data: {}) {
    const model = this.model(table)
    data = this.sql.dump(model, data)
    const keys = Object.keys(data)
    const sql = `INSERT INTO ${escapeId(table)} (${this.#joinKeys(keys)}) VALUES (${keys.map(key => this.sql.escape(data[key])).join(', ')})`
    return this.#run(sql, [], () => this.#get(`select last_insert_rowid() as id`))
  }

  async create(sel: Selection.Mutable, data: {}) {
    const { model, table } = sel
    data = model.create(data)
    const { id } = this.#create(table, data)
    const { autoInc, primary } = model
    if (!autoInc || Array.isArray(primary)) return data as any
    return { ...data, [primary]: id }
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return
    const { model, table, ref } = sel
    const dataFields = [...new Set(Object.keys(Object.assign({}, ...data)).map((key) => {
      return Object.keys(model.fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const relaventFields = union(keys, dataFields)
    const updateFields = difference(dataFields, keys)
    const results = await this.database.get(table, {
      $or: data.map(item => Object.fromEntries(keys.map(key => [key, item[key]]))),
    }, relaventFields as [])
    for (const item of data) {
      const row = results.find(row => keys.every(key => row[key] === item[key]))
      if (row) {
        this.#update(sel, keys, updateFields, item, row)
      } else {
        this.#create(table, executeUpdate(model.create(), item, ref))
      }
    }
  }
}

export default SQLiteDriver
