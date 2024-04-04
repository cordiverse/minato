import { deepEqual, Dict, difference, isNullable, makeArray } from 'cosmokit'
import { Driver, Eval, executeUpdate, Field, Selection, toArrayBuffer, z } from 'minato'
import { escapeId } from '@minatojs/sql-utils'
import { resolve } from 'node:path'
import { readFile, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import init from '@minatojs/sql.js'
import enUS from './locales/en-US.yml'
import zhCN from './locales/zh-CN.yml'
import { SQLiteBuilder } from './builder'
import { pathToFileURL } from 'node:url'

function getTypeDef({ deftype: type }: Field) {
  switch (type) {
    case 'primary':
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
    case 'binary': return `BLOB`
  }
}

export interface SQLiteFieldInfo {
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: string
  pk: boolean
}

export class SQLiteDriver extends Driver<SQLiteDriver.Config> {
  static name = 'sqlite'

  db!: init.Database
  sql = new SQLiteBuilder(this)
  beforeUnload?: () => void

  private _transactionTask?: Promise<void>

  /** synchronize table schema */
  async prepare(table: string, dropKeys?: string[]) {
    const columns = this.#all(`PRAGMA table_info(${escapeId(table)})`) as SQLiteFieldInfo[]
    const model = this.model(table)
    const columnDefs: string[] = []
    const indexDefs: string[] = []
    const alter: string[] = []
    const mapping: Dict<string> = {}
    let shouldMigrate = false

    // field definitions
    for (const key in model.fields) {
      if (model.fields[key]!.deprecated) {
        if (dropKeys?.includes(key)) shouldMigrate = true
        continue
      }

      const legacy = [key, ...model.fields[key]!.legacy || []]
      const column = columns.find(({ name }) => legacy.includes(name))
      const { initial, nullable = true } = model.fields[key]!
      const typedef = getTypeDef(model.fields[key]!)
      let def = `${escapeId(key)} ${typedef}`
      if (key === model.primary && model.autoInc) {
        def += ' NOT NULL PRIMARY KEY AUTOINCREMENT'
      } else {
        def += (nullable ? ' ' : ' NOT ') + 'NULL'
        if (!isNullable(initial)) {
          def += ' DEFAULT ' + this.sql.escape(this.sql.dump({ [key]: initial }, model)[key])
        }
      }
      columnDefs.push(def)
      if (!column) {
        alter.push('ADD ' + def)
      } else {
        mapping[column.name] = key
        shouldMigrate ||= column.name !== key || column.type !== typedef
      }
    }

    // index definitions
    if (model.primary && !model.autoInc) {
      indexDefs.push(`PRIMARY KEY (${this.#joinKeys(makeArray(model.primary))})`)
    }
    if (model.unique) {
      indexDefs.push(...model.unique.map(keys => `UNIQUE (${this.#joinKeys(makeArray(keys))})`))
    }
    if (model.foreign) {
      indexDefs.push(...Object.entries(model.foreign).map(([key, value]) => {
        const [table, key2] = value!
        return `FOREIGN KEY (\`${key}\`) REFERENCES ${escapeId(table)} (\`${key2}\`)`
      }))
    }

    if (!columns.length) {
      this.logger.info('auto creating table %c', table)
      this.#run(`CREATE TABLE ${escapeId(table)} (${[...columnDefs, ...indexDefs].join(', ')})`)
    } else if (shouldMigrate) {
      // preserve old columns
      for (const { name, type, notnull, pk, dflt_value: value } of columns) {
        if (mapping[name] || dropKeys?.includes(name)) continue
        let def = `${escapeId(name)} ${type}`
        def += (notnull ? ' NOT ' : ' ') + 'NULL'
        if (pk) def += ' PRIMARY KEY'
        if (value !== null) def += ' DEFAULT ' + this.sql.escape(value)
        columnDefs.push(def)
        mapping[name] = name
      }

      const temp = table + '_temp'
      const fields = Object.keys(mapping).map(escapeId).join(', ')
      this.logger.info('auto migrating table %c', table)
      this.#run(`CREATE TABLE ${escapeId(temp)} (${[...columnDefs, ...indexDefs].join(', ')})`)
      try {
        this.#run(`INSERT INTO ${escapeId(temp)} SELECT ${fields} FROM ${escapeId(table)}`)
        this.#run(`DROP TABLE ${escapeId(table)}`)
      } catch (error) {
        this.#run(`DROP TABLE ${escapeId(temp)}`)
        throw error
      }
      this.#run(`ALTER TABLE ${escapeId(temp)} RENAME TO ${escapeId(table)}`)
    } else if (alter.length) {
      this.logger.info('auto updating table %c', table)
      for (const def of alter) {
        this.#run(`ALTER TABLE ${escapeId(table)} ${def}`)
      }
    }

    if (dropKeys) return
    dropKeys = []
    this.migrate(table, {
      error: this.logger.warn,
      before: keys => keys.every(key => columns.some(({ name }) => name === key)),
      after: keys => dropKeys!.push(...keys),
      finalize: () => {
        if (!dropKeys!.length) return
        this.prepare(table, dropKeys)
      },
    })
  }

  async start() {
    if (this.config.path !== ':memory:') {
      this.config.path = resolve(this.ctx.baseDir, this.config.path)
    }
    const isBrowser = process.env.KOISHI_ENV === 'browser'
    const sqlite = await init({
      locateFile: (file: string) => process.env.KOISHI_BASE
        ? process.env.KOISHI_BASE + '/' + file
        : isBrowser
          ? '/modules/@koishijs/plugin-database-sqlite/' + file
          // @ts-ignore
          : createRequire(import.meta.url || pathToFileURL(__filename).href).resolve('@minatojs/sql.js/dist/' + file),
    })
    if (!isBrowser || this.config.path === ':memory:') {
      this.db = new sqlite.Database(this.config.path)
    } else {
      const buffer = await readFile(this.config.path).catch(() => null)
      this.db = new sqlite.Database(this.config.path, buffer)
      if (isBrowser) {
        window.addEventListener('beforeunload', this.beforeUnload = () => {
          this.#export()
        })
      }
    }
    this.db.create_function('regexp', (pattern, str) => +new RegExp(pattern).test(str))
    this.db.create_function('json_array_contains', (array, value) => +(JSON.parse(array) as any[]).includes(JSON.parse(value)))
    this.db.create_function('modulo', (left, right) => left % right)
    this.db.create_function('rand', () => Math.random())

    this.define<boolean, number>({
      types: ['boolean'],
      dump: value => isNullable(value) ? value : +value,
      load: (value) => isNullable(value) ? value : !!value,
    })

    this.define<object, string>({
      types: ['json'],
      dump: value => JSON.stringify(value),
      load: value => typeof value === 'string' ? JSON.parse(value) : value,
    })

    this.define<string[], string>({
      types: ['list'],
      dump: value => Array.isArray(value) ? value.join(',') : value,
      load: value => value ? value.split(',') : [],
    })

    this.define<Date, number>({
      types: ['date', 'time', 'timestamp'],
      dump: value => isNullable(value) ? value as any : +new Date(value),
      load: value => isNullable(value) ? value : new Date(value),
    })

    this.define<ArrayBuffer, ArrayBuffer>({
      types: ['binary'],
      dump: value => isNullable(value) ? value : new Uint8Array(value),
      load: value => isNullable(value) ? value : toArrayBuffer(value),
    })
  }

  #joinKeys(keys?: string[]) {
    return keys?.length ? keys.map(key => `\`${key}\``).join(', ') : '*'
  }

  async stop() {
    await new Promise(resolve => setTimeout(resolve, 0))
    this.db?.close()
    if (this.beforeUnload) {
      this.beforeUnload()
      window.removeEventListener('beforeunload', this.beforeUnload)
    }
  }

  #exec(sql: string, params: any, callback: (stmt: init.Statement) => any) {
    try {
      const stmt = this.db.prepare(sql)
      const result = callback(stmt)
      stmt.free()
      this.logger.debug('> %s', sql, params)
      return result
    } catch (e) {
      this.logger.warn('> %s', sql, params)
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

  #export() {
    const data = this.db.export()
    return writeFile(this.config.path, data)
  }

  #run(sql: string, params: any = [], callback?: () => any) {
    this.#exec(sql, params, stmt => stmt.run(params))
    const result = callback?.()
    return result
  }

  async drop(table: string) {
    this.#run(`DROP TABLE ${escapeId(table)}`)
  }

  async dropAll() {
    const tables = Object.keys(this.database.tables)
    for (const table of tables) {
      this.#run(`DROP TABLE ${escapeId(table)}`)
    }
  }

  async stats() {
    const stats: Driver.Stats = { size: this.db.size(), tables: {} }
    const tableNames: { name: string }[] = this.#all('SELECT name FROM sqlite_master WHERE type="table" ORDER BY name;')
    const dbstats: { name: string; size: number }[] = this.#all('SELECT name, pgsize as size FROM "dbstat" WHERE aggregate=TRUE;')
    tableNames.forEach(tbl => {
      stats.tables[tbl.name] = this.#get(`SELECT COUNT(*) as count FROM ${escapeId(tbl.name)};`)
      stats.tables[tbl.name].size = dbstats.find(o => o.name === tbl.name)!.size
    })
    return stats
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return {}
    const result = this.#run(`DELETE FROM ${escapeId(table)} WHERE ${filter}`, [], () => this.#get(`SELECT changes() AS count`))
    return { matched: result.count, removed: result.count }
  }

  async get(sel: Selection.Immutable) {
    const { model, tables } = sel
    const builder = new SQLiteBuilder(this, tables)
    const sql = builder.get(sel)
    if (!sql) return []
    const rows = this.#all(sql)
    return rows.map(row => builder.load(row, model))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const builder = new SQLiteBuilder(this, sel.tables)
    const inner = builder.get(sel.table as Selection, true, true)
    const output = builder.parseEval(expr, false)
    const { value } = this.#get(`SELECT ${output} AS value FROM ${inner}`)
    return builder.load(value, expr)
  }

  #update(sel: Selection.Mutable, indexFields: string[], updateFields: string[], update: {}, data: {}) {
    const { ref, table } = sel
    const model = this.model(table)
    executeUpdate(data, update, ref)
    const row = this.sql.dump(data, model)
    const assignment = updateFields.map((key) => `${escapeId(key)} = ?`).join(',')
    const query = Object.fromEntries(indexFields.map(key => [key, row[key]]))
    const filter = this.sql.parseQuery(query)
    this.#run(`UPDATE ${escapeId(table)} SET ${assignment} WHERE ${filter}`, updateFields.map((key) => row[key] ?? null))
  }

  async set(sel: Selection.Mutable, update: {}) {
    const { model, table, query } = sel
    const { primary, fields } = model
    const updateFields = [...new Set(Object.keys(update).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const primaryFields = makeArray(primary)
    const data = await this.database.get(table, query)
    for (const row of data) {
      this.#update(sel, primaryFields, updateFields, update, row)
    }
    return { matched: data.length }
  }

  #create(table: string, data: {}) {
    const model = this.model(table)
    data = this.sql.dump(data, model)
    const keys = Object.keys(data)
    const sql = `INSERT INTO ${escapeId(table)} (${this.#joinKeys(keys)}) VALUES (${Array(keys.length).fill('?').join(', ')})`
    return this.#run(sql, keys.map(key => data[key] ?? null), () => this.#get(`SELECT last_insert_rowid() AS id`))
  }

  async create(sel: Selection.Mutable, data: {}) {
    const { model, table } = sel
    const { id } = this.#create(table, data)
    const { autoInc, primary } = model
    if (!autoInc || Array.isArray(primary)) return data as any
    return { ...data, [primary]: id }
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return {}
    const { model, table, ref } = sel
    const result = { inserted: 0, matched: 0, modified: 0 }
    const dataFields = [...new Set(Object.keys(Object.assign({}, ...data)).map((key) => {
      return Object.keys(model.fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]
    let updateFields = difference(dataFields, keys)
    if (!updateFields.length) updateFields = [dataFields[0]]
    // Error: Expression tree is too large (maximum depth 1000)
    const step = Math.floor(960 / keys.length)
    for (let i = 0; i < data.length; i += step) {
      const chunk = data.slice(i, i + step)
      const results = await this.database.get(table, {
        $or: chunk.map(item => Object.fromEntries(keys.map(key => [key, item[key]]))),
      })
      for (const item of chunk) {
        const row = results.find(row => keys.every(key => deepEqual(row[key], item[key], true)))
        if (row) {
          this.#update(sel, keys, updateFields, item, row)
          result.matched++
        } else {
          this.#create(table, executeUpdate(model.create(), item, ref))
          result.inserted++
        }
      }
    }
    return result
  }

  async withTransaction(callback: (session: this) => Promise<void>) {
    if (this._transactionTask) await this._transactionTask
    return this._transactionTask = new Promise<void>((resolve, reject) => {
      this.#run('BEGIN TRANSACTION')
      callback(this).then(() => resolve(this.#run('COMMIT')), (e) => (this.#run('ROLLBACK'), reject(e)))
    })
  }
}

export namespace SQLiteDriver {
  export interface Config {
    path: string
  }

  export const Config: z<Config> = z.object({
    path: z.string().role('path').required(),
  }).i18n({
    'en-US': enUS,
    'zh-CN': zhCN,
  })
}

export default SQLiteDriver
