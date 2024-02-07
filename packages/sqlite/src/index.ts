import { clone, deepEqual, Dict, difference, isNullable, makeArray } from 'cosmokit'
import { Driver, Eval, executeUpdate, Field, Model, randomId, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import { promises as fs } from 'fs'
import init from '@minatojs/sql.js'
import Logger from 'reggol'

const logger = new Logger('sqlite')

function getTypeDef({ type }: Field) {
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
    this.evalOperators.$concat = (args) => `(${args.map(arg => this.parseEval(arg)).join('||')})`
    this.evalOperators.$modulo = ([left, right]) => `modulo(${this.parseEval(left)}, ${this.parseEval(right)})`
    this.evalOperators.$log = ([left, right]) => isNullable(right)
      ? `log(${this.parseEval(left)})`
      : `log(${this.parseEval(left)}) / log(${this.parseEval(right)})`
    this.evalOperators.$length = (expr) => this.createAggr(expr, value => `count(${value})`, value => {
      if (this.state.sqlType === 'json') {
        this.state.sqlType = 'raw'
        return `${this.jsonLength(value)}`
      } else {
        this.state.sqlType = 'raw'
        return `iif(${value}, LENGTH(${value}) - LENGTH(REPLACE(${value}, ${this.escape(',')}, ${this.escape('')})) + 1, 0)`
      }
    })
    this.evalOperators.$number = (arg) => {
      const value = this.parseEval(arg)
      const res = this.state.sqlType === 'raw' ? `cast(${this.parseEval(arg)} as double)`
        : `cast(${value} / 1000 as integer)`
      this.state.sqlType = 'raw'
      return `ifnull(${res}, 0)`
    }

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
      dump: value => value === null ? null : +new Date(value),
      load: (value) => value === null ? null : new Date(value),
    })
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) value = +value
    else if (value instanceof RegExp) value = value.source
    return super.escape(value, field)
  }

  protected createElementQuery(key: string, value: any) {
    if (this.state.sqlTypes?.[this.unescapeId(key)] === 'json') {
      return this.jsonContains(key, this.quote(JSON.stringify(value)))
    } else {
      return `(',' || ${key} || ',') LIKE ${this.escape('%,' + value + ',%')}`
    }
  }

  protected jsonLength(value: string) {
    return `json_array_length(${value})`
  }

  protected jsonContains(obj: string, value: string) {
    return `json_array_contains(${obj}, ${value})`
  }

  protected jsonUnquote(value: string, pure: boolean = false) {
    return value
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string) {
    if (!this.state.group && !nonaggr) {
      const value = this.parseEval(expr, false)
      return `(select ${aggr(escapeId('value'))} from json_each(${value}) ${randomId()})`
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected groupArray(value: string) {
    const res = this.state.sqlType === 'json' ? `('[' || group_concat(${value}) || ']')` : `('[' || group_concat(json_quote(${value})) || ']')`
    this.state.sqlType = 'json'
    return `ifnull(${res}, json_array())`
  }

  protected transformJsonField(obj: string, path: string) {
    this.state.sqlType = 'raw'
    return `json_extract(${obj}, '$${path}')`
  }
}

export class SQLiteDriver extends Driver<SQLiteDriver.Config> {
  db!: init.Database
  sql = new SQLiteBuilder()
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

    if (!columns.length) {
      logger.info('auto creating table %c', table)
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
      logger.info('auto migrating table %c', table)
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
      logger.info('auto updating table %c', table)
      for (const def of alter) {
        this.#run(`ALTER TABLE ${escapeId(table)} ${def}`)
      }
    }

    if (dropKeys) return
    dropKeys = []
    this.migrate(table, {
      error: logger.warn,
      before: keys => keys.every(key => columns.some(({ name }) => name === key)),
      after: keys => dropKeys!.push(...keys),
      finalize: () => {
        if (!dropKeys!.length) return
        this.prepare(table, dropKeys)
      },
    })
  }

  async start() {
    const isBrowser = process.env.KOISHI_ENV === 'browser'
    const sqlite = await init({
      locateFile: (file: string) => process.env.KOISHI_BASE
        ? process.env.KOISHI_BASE + '/' + file
        : isBrowser
          ? '/modules/@koishijs/plugin-database-sqlite/' + file
          : require.resolve('@minatojs/sql.js/dist/' + file),
    })
    if (!isBrowser || this.config.path === ':memory:') {
      this.db = new sqlite.Database(this.config.path)
    } else {
      const buffer = await fs.readFile(this.config.path).catch(() => null)
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
      logger.debug('> %s', sql, params)
      return result
    } catch (e) {
      logger.warn('> %s', sql, params)
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
    fs.writeFile(this.config.path, data)
  }

  #run(sql: string, params: any = [], callback?: () => any) {
    this.#exec(sql, params, stmt => stmt.run(params))
    const result = callback?.()
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
    const builder = new SQLiteBuilder(tables)
    const sql = builder.get(sel)
    if (!sql) return []
    const rows = this.#all(sql)
    return rows.map(row => builder.load(model, row))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const builder = new SQLiteBuilder(sel.tables)
    const inner = builder.get(sel.table as Selection, true, true)
    const output = builder.parseEval(expr, false)
    const { value } = this.#get(`SELECT ${output} AS value FROM ${inner}`)
    return builder.load(value)
  }

  #update(sel: Selection.Mutable, indexFields: string[], updateFields: string[], update: {}, data: {}) {
    const { ref, table } = sel
    const model = this.model(table)
    const modified = !deepEqual(clone(data), executeUpdate(data, update, ref))
    if (!modified) return 0
    const row = this.sql.dump(model, data)
    const assignment = updateFields.map((key) => `${escapeId(key)} = ?`).join(',')
    const query = Object.fromEntries(indexFields.map(key => [key, row[key]]))
    const filter = this.sql.parseQuery(query)
    this.#run(`UPDATE ${escapeId(table)} SET ${assignment} WHERE ${filter}`, updateFields.map((key) => row[key] ?? null))
    return 1
  }

  async set(sel: Selection.Mutable, update: {}) {
    const { model, table, query } = sel
    const { primary, fields } = model
    const updateFields = [...new Set(Object.keys(update).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const primaryFields = makeArray(primary)
    const data = await this.database.get(table, query)
    let modified = 0
    for (const row of data) {
      modified += this.#update(sel, primaryFields, updateFields, update, row)
    }
    return { matched: data.length, modified }
  }

  #create(table: string, data: {}) {
    const model = this.model(table)
    data = this.sql.dump(model, data)
    const keys = Object.keys(data)
    const sql = `INSERT INTO ${escapeId(table)} (${this.#joinKeys(keys)}) VALUES (${Array(keys.length).fill('?').join(', ')})`
    return this.#run(sql, keys.map(key => data[key] ?? null), () => this.#get(`SELECT last_insert_rowid() AS id`))
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
          result.modified += this.#update(sel, keys, updateFields, item, row)
          result.matched++
        } else {
          this.#create(table, executeUpdate(model.create(), item, ref))
          result.inserted++
        }
      }
    }
    return result
  }

  async withTransaction(callback: (session: Driver) => Promise<void>) {
    if (this._transactionTask) await this._transactionTask
    return this._transactionTask = new Promise<void>((resolve, reject) => {
      this.#run('BEGIN TRANSACTION')
      callback(this).then(() => resolve(this.#run('COMMIT')), (e) => (this.#run('ROLLBACK'), reject(e)))
    })
  }
}

export default SQLiteDriver
