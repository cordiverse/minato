import { difference, makeArray, union } from 'cosmokit'
import { Database, Driver, Eval, Executable, executeUpdate, Field, Modifier } from '@minatojs/core'
import { Builder, Caster } from '@minatojs/sql-utils'
import init from 'sql.js'
import { resolve } from 'path'
import { escapeId, format, escape as sqlEscape } from 'sqlstring-sqlite'
import { promises as fsp } from 'fs'
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
    path?: string
  }
}

class SQLiteDriver extends Driver {
  db: init.Database
  sqlite = this
  sql: Builder
  caster: Caster

  constructor(database: Database, public config: SQLiteDriver.Config) {
    super(database)

    this.sql = new class extends Builder {
      format = format

      escapeId = escapeId

      escape(value: any) {
        if (value instanceof Date) {
          return (+value) + ''
        }
        return sqlEscape(value)
      }

      protected createElementQuery(key: string, value: any) {
        return `(',' || ${key} || ',') LIKE ${this.escape('%,' + value + ',%')}`
      }
    }()

    this.caster = new Caster(this.database.tables)
    this.caster.register<boolean, number>({
      types: ['boolean'],
      dump: value => +value,
      load: (value) => !!value,
    })
    this.caster.register<object, string>({
      types: ['json'],
      dump: value => JSON.stringify(value),
      load: (value, initial) => value ? JSON.parse(value) : initial,
    })
    this.caster.register<string[], string>({
      types: ['list'],
      dump: value => value.join(','),
      load: (value) => value ? value.split(',') : [],
    })
    this.caster.register<Date, number>({
      types: ['date', 'time', 'timestamp'],
      dump: value => value === null ? null : +value,
      load: (value) => value === null ? null : new Date(value),
    })
  }

  private _getColDefs(table: string, key: string) {
    const config = this.model(table)
    const { initial, nullable = true } = config.fields[key]
    let def = `\`${key}\``
    if (key === config.primary && config.autoInc) {
      def += ' INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT'
    } else {
      const typedef = getTypeDefinition(config.fields[key])
      def += ' ' + typedef + (nullable ? ' ' : ' NOT ') + 'NULL'
      if (initial !== undefined && initial !== null) {
        def += ' DEFAULT ' + this.sql.escape(this.caster.dump(table, { [key]: initial })[key])
      }
    }
    return def
  }

  /** synchronize table schema */
  async prepare(table: string) {
    const info = this.#all(`PRAGMA table_info(${this.sql.escapeId(table)})`) as SQLiteFieldInfo[]
    // WARN: side effecting Tables.config
    const config = this.model(table)
    const keys = Object.keys(config.fields)
    if (info.length) {
      let hasUpdate = false
      for (const key of keys) {
        if (info.some(({ name }) => name === key)) continue
        const def = this._getColDefs(table, key)
        this.#exec('run', `ALTER TABLE ${this.sql.escapeId(table)} ADD COLUMN ${def}`)
        hasUpdate = true
      }
      if (hasUpdate) {
        logger.info('auto updating table %c', table)
      }
    } else {
      logger.info('auto creating table %c', table)
      const defs = keys.map(key => this._getColDefs(table, key))
      const constraints = []
      if (config.primary && !config.autoInc) {
        constraints.push(`PRIMARY KEY (${this.#joinKeys(makeArray(config.primary))})`)
      }
      if (config.unique) {
        constraints.push(...config.unique.map(keys => `UNIQUE (${this.#joinKeys(makeArray(keys))})`))
      }
      if (config.foreign) {
        constraints.push(...Object.entries(config.foreign).map(([key, [table, key2]]) => {
          return `FOREIGN KEY (\`${key}\`) REFERENCES ${this.sql.escapeId(table)} (\`${key2}\`)`
        }))
      }
      this.#exec('run', `CREATE TABLE ${this.sql.escapeId(table)} (${[...defs, ...constraints].join(',')})`)
    }
  }

  async start() {
    const [sqlite, buffer] = await Promise.all([
      init(),
      this.config.path === ':memory:' ? undefined : fsp.readFile(this.config.path),
    ])
    this.db = new sqlite.Database(buffer)
    this.db.create_function('regexp', (pattern, str) => +new RegExp(pattern).test(str))
  }

  #joinKeys(keys?: string[]) {
    return keys?.length ? keys.map(key => `\`${key}\``).join(',') : '*'
  }

  async stop() {
    this.db.close()
  }

  #exec<K extends 'get' | 'run'>(action: K, sql: string, params: any = []) {
    try {
      const result = this.db.prepare(sql)[action](params)
      return result as any
    } catch (e) {
      logger.warn('SQL > %c', sql)
      throw e
    }
  }

  #all(sql: string, params: any = []) {
    try {
      const stmt = this.db.prepare(sql)
      stmt.bind(params)
      const result = []
      while (stmt.step()) {
        result.push(stmt.getAsObject())
      }
      return result
    } catch (e) {
      logger.warn('SQL > %c', sql)
      throw e
    }
  }

  async drop() {
    const tables = Object.keys(this.database.tables)
    for (const table of tables) {
      this.#exec('run', `DROP TABLE ${this.sql.escapeId(table)}`)
    }
  }

  async stats() {
    if (this.config.path === ':memory:') return {}
    const { size } = await fsp.stat(this.config.path)
    return { size }
  }

  async remove(sel: Executable) {
    const { query, table } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return
    this.#exec('run', `DELETE FROM ${this.sql.escapeId(table)} WHERE ${filter}`)
  }

  async get(sel: Executable, modifier: Modifier) {
    const { table, fields, query } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return []
    const { limit, offset, sort } = modifier
    let sql = `SELECT ${this.#joinKeys(fields ? Object.keys(fields) : null)} FROM ${this.sql.escapeId(table)} WHERE ${filter}`
    if (sort.length) sql += ' ORDER BY ' + sort.map(([key, order]) => `\`${key['$'][1]}\` ${order}`).join(', ')
    if (limit < Infinity) sql += ' LIMIT ' + limit
    if (offset > 0) sql += ' OFFSET ' + offset
    const rows = this.#all(sql)
    return rows.map(row => this.caster.load(table, row))
  }

  async eval(sel: Executable, expr: Eval.Expr) {
    const { table, query } = sel
    const filter = this.sql.parseQuery(query)
    const output = this.sql.parseEval(expr)
    const { value } = this.#exec('get', `SELECT ${output} AS value FROM ${this.sql.escapeId(table)} WHERE ${filter}`)
    return value
  }

  #update(sel: Executable, indexFields: string[], updateFields: string[], update: {}, data: {}) {
    const { ref, table } = sel
    const row = this.caster.dump(table, executeUpdate(data, update, ref))
    const assignment = updateFields.map((key) => `\`${key}\` = ${this.sql.escape(row[key])}`).join(',')
    const query = Object.fromEntries(indexFields.map(key => [key, row[key]]))
    const filter = this.sql.parseQuery(query)
    this.#exec('run', `UPDATE ${this.sql.escapeId(table)} SET ${assignment} WHERE ${filter}`)
  }

  async set(sel: Executable, update: {}) {
    const { model, table, query } = sel
    const { primary, fields } = model
    const updateFields = [...new Set(Object.keys(update).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))
    }))]
    const primaryFields = makeArray(primary)
    const data = await this.database.get(table, query, union(primaryFields, updateFields) as [])
    for (const row of data) {
      this.#update(sel, primaryFields, updateFields, update, row)
    }
  }

  #create(table: string, data: {}) {
    data = this.caster.dump(table, data)
    const keys = Object.keys(data)
    const sql = `INSERT INTO ${this.sql.escapeId(table)} (${this.#joinKeys(keys)}) VALUES (${keys.map(key => this.sql.escape(data[key])).join(', ')})`
    return this.#exec('run', sql)
  }

  async create(sel: Executable, data: {}) {
    const { model, table } = sel
    data = model.create(data)
    const result = this.#create(table, data)
    const { autoInc, primary } = model
    if (!autoInc) return data as any
    return { ...data, [primary as string]: result.lastInsertRowid }
  }

  async upsert(sel: Executable, data: any[], keys: string[]) {
    if (!data.length) return
    const { model, table, ref } = sel
    const dataFields = [...new Set(Object.keys(Object.assign({}, ...data)).map((key) => {
      return Object.keys(model.fields).find(field => field === key || key.startsWith(field + '.'))
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
