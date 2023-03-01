import { deepEqual, Dict, difference, isNullable, makeArray, union } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, Model, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import { promises as fs } from 'fs'
import init from '@minatojs/sql.js'
import Logger from 'reggol'

const logger = new Logger('sqlite')

function getTypeDef({ type }: Field) {
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
  cid: number
  name: string
  type: string
  notnull: number
  dflt_value: string
  pk: boolean
}

export namespace SQLiteDriver {
  export interface Config {
    path: string
  }
}

class SQLiteBuilder extends Builder {
  protected escapeMap = {
    "'": "''",
  }

  constructor(tables?: Dict<Model>) {
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
      dump: value => Array.isArray(value) ? value.join(',') : value,
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

export class SQLiteDriver extends Driver {
  db!: init.Database
  sql: Builder
  writeTask?: NodeJS.Timeout
  sqlite!: init.SqlJsStatic

  constructor(database: Database, public config: SQLiteDriver.Config) {
    super(database)

    this.sql = new SQLiteBuilder()
  }

  /** synchronize table schema */
  async prepare(table: string) {
    const info = this.#all(`PRAGMA table_info(${escapeId(table)})`) as SQLiteFieldInfo[]
    const model = this.model(table)
    const columnDefs: string[] = []
    const indexDefs: string[] = []
    const alter: string[] = []
    const mapping: Dict<string> = {}
    let shouldMigrate = false

    // field definitions
    for (const key in model.fields) {
      const legacy = [key, ...model.fields[key]!.legacy || []]
      const column = info.find(({ name }) => legacy.includes(name))
      const { initial, nullable = true } = model.fields[key]!
      const typedef = getTypeDef(model.fields[key]!)
      let def = `${escapeId(key)} ${typedef}`
      if (key === model.primary && model.autoInc) {
        def += ' NOT NULL PRIMARY KEY AUTOINCREMENT'
      } else {
        def += (nullable ? ' ' : ' NOT ') + 'NULL'
        if (!isNullable(initial)) {
          def += ' DEFAULT ' + this.sql.escape(this.sql.dump(model, { [key]: initial })[key])
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

    if (!info.length) {
      logger.info('auto creating table %c', table)
      this.#run(`CREATE TABLE ${escapeId(table)} (${[...columnDefs, ...indexDefs].join(', ')})`)
    } else if (shouldMigrate) {
      // preserve old columns
      for (const column of info) {
        if (mapping[column.name]) continue
        let def = `${escapeId(column.name)} ${column.type}`
        def += (column.notnull ? ' NOT ' : ' ') + 'NULL'
        if (column.pk) def += ' PRIMARY KEY'
        if (column.dflt_value !== null) def += ' DEFAULT ' + this.sql.escape(column.dflt_value)
        columnDefs.push(def)
        mapping[column.name] = column.name
      }

      const temp = table + '_temp'
      const fields = Object.keys(mapping).map(escapeId).join(', ')
      logger.info('auto migrating table %c', table)
      this.#run(`CREATE TABLE ${escapeId(temp)} (${columnDefs.join(', ')})`)
      try {
        this.#run(`INSERT INTO ${escapeId(temp)} SELECT ${fields} FROM ${escapeId(table)}`)
        this.#run(`DROP TABLE ${escapeId(table)}`)
        this.#run(`CREATE TABLE ${escapeId(table)} (${[...columnDefs, ...indexDefs].join(', ')})`)
        this.#run(`INSERT INTO ${escapeId(table)} SELECT * FROM ${escapeId(temp)}`)
      } finally {
        this.#run(`DROP TABLE ${escapeId(temp)}`)
      }
    } else if (alter.length) {
      logger.info('auto updating table %c', table)
      for (const def of alter) {
        this.#run(`ALTER TABLE ${escapeId(table)} ${def}`)
      }
    }
  }

  init(buffer: ArrayLike<number> | null) {
    this.db = new this.sqlite.Database(buffer)
    this.db.create_function('regexp', (pattern, str) => +new RegExp(pattern).test(str))
  }

  async load() {
    if (this.config.path === ':memory:') return null
    return fs.readFile(this.config.path).catch(() => null)
  }

  async start() {
    const [sqlite, buffer] = await Promise.all([
      init({
        locateFile: (file: string) => process.env.KOISHI_BASE
          ? process.env.KOISHI_BASE + '/' + file
          : process.env.KOISHI_ENV === 'browser'
            ? '/' + file
            : require.resolve('@minatojs/sql.js/dist/' + file),
      }),
      this.load(),
    ])
    this.sqlite = sqlite
    this.init(buffer)
  }

  #joinKeys(keys?: string[]) {
    return keys?.length ? keys.map(key => `\`${key}\``).join(', ') : '*'
  }

  async stop() {
    this.db?.close()
  }

  #exec(sql: string, params: any, callback: (stmt: init.Statement) => any) {
    try {
      const stmt = this.db.prepare(sql)
      const result = callback(stmt)
      stmt.free()
      logger.debug('> %s', sql)
      return result
    } catch (e) {
      logger.warn('> %s', sql)
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

  async drop(table?: string) {
    if (table) return this.#run(`DROP TABLE ${escapeId(table)}`)
    const tables = Object.keys(this.database.tables)
    for (const table of tables) {
      this.#run(`DROP TABLE ${escapeId(table)}`)
    }
  }

  async stats() {
    const data = this.db.export()
    this.init(data)
    return { size: data.byteLength }
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return
    this.#run(`DELETE FROM ${escapeId(table)} WHERE ${filter}`)
  }

  async get(sel: Selection.Immutable) {
    const { model, tables } = sel
    const builder = new SQLiteBuilder(tables)
    const sql = builder.get(sel)
    if (!sql) return []
    const rows = this.#all(sql)
    return rows.map(row => builder.load(model, row))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const builder = new SQLiteBuilder(sel.tables)
    const output = builder.parseEval(expr)
    const inner = builder.get(sel.table as Selection, true)
    const { value } = this.#get(`SELECT ${output} AS value FROM ${inner} ${sel.ref}`)
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
    return this.#run(sql, [], () => this.#get(`SELECT last_insert_rowid() AS id`))
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
      const row = results.find(row => keys.every(key => deepEqual(row[key], item[key], true)))
      if (row) {
        this.#update(sel, keys, updateFields, item, row)
      } else {
        this.#create(table, executeUpdate(model.create(), item, ref))
      }
    }
  }
}

export default SQLiteDriver
