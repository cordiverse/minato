import { createPool, format } from '@vlasky/mysql'
import type { OkPacket, Pool, PoolConfig } from 'mysql'
import { Dict, difference, makeArray, pick, Time } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, isEvalExpr, Model, RuntimeError, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import Logger from 'reggol'

declare module 'mysql' {
  interface UntypedFieldInfo {
    packet: UntypedFieldInfo
  }
}

const logger = new Logger('mysql')

const DEFAULT_DATE = new Date('1970-01-01')

function getIntegerType(length = 11) {
  if (length <= 4) return 'tinyint'
  if (length <= 6) return 'smallint'
  if (length <= 9) return 'mediumint'
  if (length <= 11) return 'int'
  return 'bigint'
}

function getTypeDef({ type, length, precision, scale }: Field) {
  switch (type) {
    case 'float':
    case 'double':
    case 'date':
    case 'time': return type
    case 'timestamp': return 'datetime(3)'
    case 'boolean': return 'bit'
    case 'integer': return getIntegerType(length)
    case 'primary':
    case 'unsigned': return `${getIntegerType(length)} unsigned`
    case 'decimal': return `decimal(${precision}, ${scale}) unsigned`
    case 'char': return `char(${length || 255})`
    case 'string': return `varchar(${length || 255})`
    case 'text': return `text(${length || 65535})`
    case 'list': return `text(${length || 65535})`
    case 'json': return `text(${length || 65535})`
    default: throw new Error(`unsupported type: ${type}`)
  }
}

function isDefUpdated(field: Field, column: ColumnInfo, def: string) {
  const typename = def.split(/[ (]/)[0]
  if (typename === 'text') return !column.DATA_TYPE.endsWith('text')
  if (typename !== column.DATA_TYPE) return true
  switch (field.type) {
    case 'integer':
    case 'unsigned':
    case 'char':
      return !!field.length && !!column.CHARACTER_MAXIMUM_LENGTH && column.CHARACTER_MAXIMUM_LENGTH !== field.length
    case 'decimal':
      return column.NUMERIC_PRECISION !== field.precision || column.NUMERIC_SCALE !== field.scale
    case 'string':
    case 'text':
    case 'list':
    case 'json':
      return false
    default: return false
  }
}

function createIndex(keys: string | string[]) {
  return makeArray(keys).map(escapeId).join(', ')
}

interface ColumnInfo {
  COLUMN_NAME: string
  IS_NULLABLE: 'YES' | 'NO'
  DATA_TYPE: string
  CHARACTER_MAXIMUM_LENGTH: number
  CHARACTER_OCTET_LENGTH: number
  NUMERIC_PRECISION: number
  NUMERIC_SCALE: number
}

interface IndexInfo {
  INDEX_NAME: string
  COLUMN_NAME: string
}

interface QueryTask {
  sql: string
  resolve: (value: any) => void
  reject: (reason: unknown) => void
}

class MySQLBuilder extends Builder {
  // eslint-disable-next-line no-control-regex
  protected escapeRegExp = /[\0\b\t\n\r\x1a'"\\]/g
  protected escapeMap = {
    '\0': '\\0',
    '\b': '\\b',
    '\t': '\\t',
    '\n': '\\n',
    '\r': '\\r',
    '\x1a': '\\Z',
    '\"': '\\\"',
    '\'': '\\\'',
    '\\': '\\\\',
  }

  constructor(tables?: Dict<Model>) {
    super(tables)

    this.define<string[], string>({
      types: ['list'],
      dump: value => value.join(','),
      load: value => value ? value.split(',') : [],
    })
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) {
      value = Time.template('yyyy-MM-dd hh:mm:ss', value)
    } else if (!field && !!value && typeof value === 'object') {
      return `json_extract(${this.quote(JSON.stringify(value))}, '$')`
    }
    return super.escape(value, field)
  }

  toUpdateExpr(item: any, key: string, field?: Field, upsert?: boolean) {
    const escaped = escapeId(key)

    // update directly
    if (key in item) {
      if (!isEvalExpr(item[key]) && upsert) {
        return `VALUES(${escaped})`
      } else if (isEvalExpr(item[key])) {
        return this.parseEval(item[key])
      } else {
        return this.escape(item[key], field)
      }
    }

    // prepare nested layout
    const jsonInit = {}
    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      if (rest.length === 1) continue
      rest.reduce((obj, k) => obj[k] ||= {}, jsonInit)
    }

    // update with json_set
    const valueInit = `ifnull(${escaped}, '{}')`
    let value = valueInit

    // json_set cannot create deeply nested property when non-exist
    // therefore we merge a layout to it
    if (Object.keys(jsonInit).length !== 0) {
      value = `json_merge(${value}, ${this.quote(JSON.stringify(jsonInit))})`
    }

    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      value = `json_set(${value}, '$${rest.map(key => `."${key}"`).join('')}', ${this.parseEval(item[prop])})`
    }

    if (value === valueInit) {
      return escaped
    } else {
      return value
    }
  }
}

export namespace MySQLDriver {
  export interface Config extends PoolConfig {}
}

export class MySQLDriver extends Driver {
  public pool!: Pool
  public config: MySQLDriver.Config
  public sql: MySQLBuilder

  private _queryTasks: QueryTask[] = []

  constructor(database: Database, config?: MySQLDriver.Config) {
    super(database)

    this.config = {
      host: 'localhost',
      port: 3306,
      charset: 'utf8mb4_general_ci',
      multipleStatements: true,
      typeCast: (field, next) => {
        const { orgName, orgTable } = field.packet
        const meta = this.database.tables[orgTable]?.fields[orgName]

        if (Field.string.includes(meta!?.type)) {
          return field.string()
        } else if (meta?.type === 'json') {
          const source = field.string()
          return source ? JSON.parse(source) : meta.initial
        } else if (meta?.type === 'time') {
          const source = field.string()
          if (!source) return meta.initial
          const time = new Date(DEFAULT_DATE)
          const [h, m, s] = source.split(':')
          time.setHours(parseInt(h))
          time.setMinutes(parseInt(m))
          time.setSeconds(parseInt(s))
          return time
        }

        if (field.type === 'BIT') {
          return Boolean(field.buffer()?.readUInt8(0))
        } else {
          return next()
        }
      },
      ...config,
    }

    this.sql = new MySQLBuilder()
  }

  async start() {
    this.pool = createPool(this.config)
  }

  async stop() {
    this.pool.end()
  }

  /** synchronize table schema */
  async prepare(name: string) {
    const [columns, indexes] = await Promise.all([
      this.queue<ColumnInfo[]>([
        `SELECT *`,
        `FROM information_schema.columns`,
        `WHERE TABLE_SCHEMA = ? && TABLE_NAME = ?`,
      ].join(' '), [this.config.database, name]),
      this.queue<IndexInfo[]>([
        `SELECT *`,
        `FROM information_schema.statistics`,
        `WHERE TABLE_SCHEMA = ? && TABLE_NAME = ?`,
      ].join(' '), [this.config.database, name]),
    ])

    const table = this.model(name)
    const { primary, foreign, autoInc } = table
    const fields = { ...table.fields }
    const unique = [...table.unique]
    const create: string[] = []
    const update: string[] = []

    // field definitions
    for (const key in fields) {
      const { deprecated, initial, nullable = true } = fields[key]!
      if (deprecated) continue
      const legacy = [key, ...fields[key]!.legacy || []]
      const column = columns.find(info => legacy.includes(info.COLUMN_NAME))
      let shouldUpdate = column?.COLUMN_NAME !== key

      let def = escapeId(key)
      if (key === primary && autoInc) {
        def += ' int unsigned not null auto_increment'
      } else {
        const typedef = getTypeDef(fields[key]!)
        if (column && !shouldUpdate) {
          shouldUpdate = isDefUpdated(fields[key]!, column, typedef)
        }
        def += ' ' + typedef
        if (makeArray(primary).includes(key)) {
          def += ' not null'
        } else {
          def += (nullable ? ' ' : ' not ') + 'null'
        }
        // blob, text, geometry or json columns cannot have default values
        if (initial && !typedef.startsWith('text')) {
          def += ' default ' + this.sql.escape(initial, fields[key])
        }
      }

      if (!column) {
        create.push(def)
      } else if (shouldUpdate) {
        update.push(`CHANGE ${escapeId(column.COLUMN_NAME)} ${def}`)
      }
    }

    // index definitions
    if (!columns.length) {
      create.push(`PRIMARY KEY (${createIndex(primary)})`)
      for (const key in foreign) {
        const [table, key2] = foreign[key]!
        create.push(`FOREIGN KEY (${escapeId(key)}) REFERENCES ${escapeId(table)} (${escapeId(key2)})`)
      }
    }

    for (const key of unique) {
      let oldIndex: IndexInfo | undefined
      let shouldUpdate = false
      const oldKeys = makeArray(key).map((key) => {
        const legacy = [key, ...fields[key]!.legacy || []]
        const column = columns.find(info => legacy.includes(info.COLUMN_NAME))
        if (column?.COLUMN_NAME !== key) shouldUpdate = true
        return column?.COLUMN_NAME
      })
      if (oldKeys.every(Boolean)) {
        const name = 'unique:' + oldKeys.join('+')
        oldIndex = indexes.find(info => info.INDEX_NAME === name)
      }
      const name = 'unique:' + makeArray(key).join('+')
      if (!oldIndex) {
        create.push(`UNIQUE INDEX ${escapeId(name)} (${createIndex(key)})`)
      } else if (shouldUpdate) {
        create.push(`UNIQUE INDEX ${escapeId(name)} (${createIndex(key)})`)
        update.push(`DROP INDEX ${escapeId(oldIndex.INDEX_NAME)}`)
      }
    }

    if (!columns.length) {
      logger.info('auto creating table %c', name)
      return this.query(`CREATE TABLE ${escapeId(name)} (${create.join(', ')}) COLLATE = ${this.sql.escape(this.config.charset)}`)
    }

    const operations = [
      ...create.map(def => 'ADD ' + def),
      ...update,
    ]
    if (operations.length) {
      // https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
      logger.info('auto updating table %c', name)
      await this.query(`ALTER TABLE ${escapeId(name)} ${operations.join(', ')}`)
    }

    // migrate deprecated fields (do not await)
    const dropKeys: string[] = []
    this.migrate(name, {
      error: logger.warn,
      before: keys => keys.every(key => columns.some(info => info.COLUMN_NAME === key)),
      after: keys => dropKeys.push(...keys),
      finalize: async () => {
        if (!dropKeys.length) return
        logger.info('auto migrating table %c', name)
        await this.query(`ALTER TABLE ${escapeId(name)} ${dropKeys.map(key => `DROP ${escapeId(key)}`).join(', ')}`)
      },
    })
  }

  _joinKeys = (keys: readonly string[]) => {
    return keys ? keys.map(key => key.includes('`') ? key : `\`${key}\``).join(',') : '*'
  }

  _formatValues = (table: string, data: object, keys: readonly string[]) => {
    return keys.map((key) => {
      const field = this.database.tables[table]?.fields[key]
      return this.sql.escape(data[key], field)
    }).join(', ')
  }

  query<T = any>(sql: string): Promise<T> {
    const error = new Error()
    return new Promise((resolve, reject) => {
      this.pool.query(sql, (err: Error, results) => {
        if (!err) return resolve(results)
        logger.warn(sql)
        if (err['code'] === 'ER_DUP_ENTRY') {
          err = new RuntimeError('duplicate-entry', err.message)
        }
        err.stack = err.message + error.stack!.slice(5)
        reject(err)
      })
    })
  }

  queue<T = any>(sql: string, values?: any): Promise<T> {
    sql = format(sql, values)
    logger.debug('> %s', sql)
    if (!this.config.multipleStatements) {
      return this.query(sql)
    }

    return new Promise<any>((resolve, reject) => {
      this._queryTasks.push({ sql, resolve, reject })
      process.nextTick(() => this._flushTasks())
    })
  }

  private async _flushTasks() {
    const tasks = this._queryTasks
    if (!tasks.length) return
    this._queryTasks = []

    try {
      let results = await this.query(tasks.map(task => task.sql).join('; '))
      if (tasks.length === 1) results = [results]
      tasks.forEach((task, index) => {
        task.resolve(results[index])
      })
    } catch (error) {
      tasks.forEach(task => task.reject(error))
    }
  }

  _select<T extends {}>(table: string, fields: readonly (string & keyof T)[], conditional?: string, values?: readonly any[]): Promise<T[]>
  _select(table: string, fields: string[], conditional?: string, values: readonly any[] = []) {
    let sql = `SELECT ${this._joinKeys(fields)} FROM ${table}`
    if (conditional) sql += ` WHERE ${conditional}`
    return this.queue(sql, values)
  }

  async drop(table?: string) {
    if (table) return this.query(`DROP TABLE ${escapeId(table)}`)
    const data = await this._select('information_schema.tables', ['TABLE_NAME'], 'TABLE_SCHEMA = ?', [this.config.database])
    if (!data.length) return
    await this.query(data.map(({ TABLE_NAME }) => `DROP TABLE ${escapeId(TABLE_NAME)}`).join('; '))
  }

  async stats() {
    const data = await this._select('information_schema.tables', ['TABLE_NAME', 'TABLE_ROWS', 'DATA_LENGTH'], 'TABLE_SCHEMA = ?', [this.config.database])
    const stats: Partial<Driver.Stats> = { size: 0 }
    stats.tables = Object.fromEntries(data.map(({ TABLE_NAME: name, TABLE_ROWS: count, DATA_LENGTH: size }) => {
      stats.size += size
      return [name, { count, size }]
    }))
    return stats
  }

  async get(sel: Selection.Immutable) {
    const { model, tables } = sel
    const builder = new MySQLBuilder(tables)
    const sql = builder.get(sel)
    if (!sql) return []
    return this.queue(sql).then((data) => {
      return data.map((row) => builder.load(model, row))
    })
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const builder = new MySQLBuilder(sel.tables)
    const output = builder.parseEval(expr)
    const inner = builder.get(sel.table as Selection, true)
    const [data] = await this.queue(`SELECT ${output} AS value FROM ${inner} ${sel.ref}`)
    return data.value
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { model, query, table, tables } = sel
    const builder = new MySQLBuilder(tables)
    const filter = builder.parseQuery(query)
    const { fields } = model
    if (filter === '0') return
    const updateFields = [...new Set(Object.keys(data).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]

    const update = updateFields.map((field) => {
      const escaped = escapeId(field)
      return `${escaped} = ${builder.toUpdateExpr(data, field, fields[field], false)}`
    }).join(', ')
    await this.query(`UPDATE ${escapeId(table)} ${sel.ref} SET ${update} WHERE ${filter}`)
  }

  async remove(sel: Selection.Mutable) {
    const { query, table, tables } = sel
    const builder = new MySQLBuilder(tables)
    const filter = builder.parseQuery(query)
    if (filter === '0') return
    await this.query(`DELETE FROM ${escapeId(table)} WHERE ` + filter)
  }

  async create(sel: Selection.Mutable, data: {}) {
    const { table, model } = sel
    const { autoInc, primary } = model
    const formatted = this.sql.dump(model, data)
    const keys = Object.keys(formatted)
    const header = await this.query<OkPacket>([
      `INSERT INTO ${escapeId(table)} (${keys.map(escapeId).join(', ')})`,
      `VALUES (${keys.map(key => this.sql.escape(formatted[key])).join(', ')})`,
    ].join(' '))
    if (!autoInc) return data as any
    return { ...data, [primary as string]: header.insertId } as any
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return
    const { model, table, tables, ref } = sel
    const builder = new MySQLBuilder(tables)

    const merged = {}
    const insertion = data.map((item) => {
      Object.assign(merged, item)
      return model.format(executeUpdate(model.create(), item, ref))
    })
    const initFields = Object.keys(model.fields).filter(key => !model.fields[key]?.deprecated)
    const dataFields = [...new Set(Object.keys(merged).map((key) => {
      return initFields.find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const updateFields = difference(dataFields, keys)

    const createFilter = (item: any) => builder.parseQuery(pick(item, keys))
    const createMultiFilter = (items: any[]) => {
      if (items.length === 1) {
        return createFilter(items[0])
      } else if (keys.length === 1) {
        const key = keys[0]
        return builder.parseQuery({ [key]: items.map(item => item[key]) })
      } else {
        return items.map(createFilter).join(' OR ')
      }
    }

    const update = updateFields.map((field) => {
      const escaped = escapeId(field)
      const branches: Dict<any[]> = {}
      data.forEach((item) => {
        (branches[builder.toUpdateExpr(item, field, model.fields[field], true)] ??= []).push(item)
      })

      const entries = Object.entries(branches)
        .map(([expr, items]) => [createMultiFilter(items), expr])
        .sort(([a], [b]) => a.length - b.length)
        .reverse()

      let value = entries[0][1]
      for (let index = 1; index < entries.length; index++) {
        value = `if(${entries[index][0]}, ${entries[index][1]}, ${value})`
      }
      return `${escaped} = ${value}`
    }).join(', ')

    await this.query([
      `INSERT INTO ${escapeId(table)} (${initFields.map(escapeId).join(', ')})`,
      `VALUES (${insertion.map(item => this._formatValues(table, item, initFields)).join('), (')})`,
      `ON DUPLICATE KEY UPDATE ${update}`,
    ].join(' '))
  }
}

export default MySQLDriver
