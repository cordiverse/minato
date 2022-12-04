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

function getTypeDefinition({ type, length, precision, scale }: Field) {
  switch (type) {
    case 'float':
    case 'double':
    case 'date':
    case 'time': return type
    case 'timestamp': return 'datetime(3)'
    case 'boolean': return 'bit'
    case 'integer': return getIntegerType(length)
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

function createIndex(keys: string | string[]) {
  return makeArray(keys).map(escapeId).join(', ')
}

interface ColumnInfo {
  COLUMN_NAME: string
  IS_NULLABLE: 'YES' | 'NO'
  DATA_TYPE: string
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
  constructor(tables: Dict<Model>) {
    super(tables)

    this.define<string[], string>({
      types: ['list'],
      dump: value => value.join(','),
      load: (value) => value ? value.split(',') : [],
    })
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) {
      value = Time.template('yyyy-MM-dd hh:mm:ss', value)
    }
    return super.escape(value, field)
  }
}

namespace MySQLDriver {
  export interface Config extends PoolConfig {}
}

class MySQLDriver extends Driver {
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
        } else if (meta?.type === 'list') {
          const source = field.string()
          return source ? source.split(',') : []
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

    this.sql = new MySQLBuilder(database.tables)
  }

  async start() {
    this.pool = createPool(this.config)
  }

  async stop() {
    this.pool.end()
  }

  private _getColDefs(name: string, columns: ColumnInfo[], indexes: IndexInfo[]) {
    const table = this.model(name)
    const { primary, foreign, autoInc } = table
    const fields = { ...table.fields }
    const unique = [...table.unique]
    const create: string[] = []
    const update: string[] = []

    // field definitions
    for (const key in fields) {
      let shouldUpdate = false
      const legacy = columns.find(info => info.COLUMN_NAME === key)
      const { initial, nullable = true } = fields[key]!

      let def = escapeId(key)
      if (key === primary && autoInc) {
        def += ' int unsigned not null auto_increment'
      } else {
        const typedef = getTypeDefinition(fields[key]!)
        // const typename = typedef.split(/[ (]/)[0]
        // if (legacy && legacy.DATA_TYPE !== typename) {
        //   logger.warn(`${name}.${key} data type mismatch: ${legacy.DATA_TYPE} => ${typedef}`)
        //   shouldUpdate = true
        // }
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

      if (!legacy) {
        create.push(def)
      } else if (shouldUpdate) {
        update.push(def)
      }
    }

    // index definitions
    if (!columns.length) {
      create.push(`primary key (${createIndex(primary)})`)
      for (const key in foreign) {
        const [table, key2] = foreign[key]!
        create.push(`foreign key (${escapeId(key)}) references ${escapeId(table)} (${escapeId(key2)})`)
      }
    }
    for (const key of unique) {
      const name = makeArray(key).join('_')
      const legacy = indexes.find(info => info.INDEX_NAME === name)
      if (!legacy) create.push(`unique index (${createIndex(key)})`)
    }

    return [create, update]
  }

  /** synchronize table schema */
  async prepare(name: string) {
    const [columns, indexes] = await Promise.all([
      this.queue<ColumnInfo[]>(`
        SELECT COLUMN_NAME, IS_NULLABLE, DATA_TYPE
        FROM information_schema.columns
        WHERE TABLE_SCHEMA = ? && TABLE_NAME = ?
      `, [this.config.database, name]),
      this.queue<IndexInfo[]>(`
        SELECT COLUMN_NAME, INDEX_NAME
        FROM information_schema.statistics
        WHERE TABLE_SCHEMA = ? && TABLE_NAME = ?
      `, [this.config.database, name]),
    ])

    const [create, update] = this._getColDefs(name, columns, indexes)
    if (!columns.length) {
      logger.info('auto creating table %c', name)
      return this.queue(`CREATE TABLE ?? (${create.join(',')}) COLLATE = ?`, [name, this.config.charset])
    }

    const operations = [
      ...create.map(def => 'ADD ' + def),
      ...update.map(def => 'MODIFY ' + def),
    ]
    if (operations.length) {
      // https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
      logger.info('auto updating table %c', name)
      await this.queue(`ALTER TABLE ?? ${operations.join(',')}`, [name])
    }
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

  query<T = any>(sql: string, values?: any): Promise<T> {
    const error = new Error()
    return new Promise((resolve, reject) => {
      sql = format(sql, values)
      logger.debug(sql)
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
    if (!this.config.multipleStatements) {
      return this.query(sql, values)
    }

    sql = format(sql, values)
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
    logger.debug(`[select] ${table}: ${fields ? fields.join(', ') : '*'}`)
    const sql = 'SELECT '
      + this._joinKeys(fields)
      + (table.includes('.') ? `FROM ${table}` : ' FROM `' + table + '`')
      + (conditional ? ' WHERE ' + conditional : '')
    return this.queue(sql, values)
  }

  async drop() {
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
      return data.map((row) => model.parse(row))
    })
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
    const [data] = await this.queue(sql)
    return data.value
  }

  private toUpdateExpr(item: any, field: string, upsert: boolean) {
    const escaped = escapeId(field)

    // update directly
    if (field in item) {
      if (isEvalExpr(item[field]) || !upsert) {
        return this.sql.parseEval(item[field])
      } else {
        return `VALUES(${escaped})`
      }
    }

    // update with json_set
    const valueInit = `ifnull(${escaped}, '{}')`
    let value = valueInit
    for (const key in item) {
      if (!key.startsWith(field + '.')) continue
      const rest = key.slice(field.length + 1).split('.')
      value = `json_set(${value}, '$${rest.map(key => `."${key}"`).join('')}', ${this.sql.parseEval(item[key])})`
    }

    if (value === valueInit) {
      return escaped
    } else {
      return value
    }
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { model, query, table } = sel
    const filter = this.sql.parseQuery(query)
    const { fields } = model
    if (filter === '0') return
    const updateFields = [...new Set(Object.keys(data).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]

    const update = updateFields.map((field) => {
      const escaped = escapeId(field)
      return `${escaped} = ${this.toUpdateExpr(data, field, false)}`
    }).join(', ')

    await this.query(`UPDATE ${table} SET ${update} WHERE ${filter}`)
  }

  async remove(sel: Selection.Mutable) {
    const { query, table } = sel
    const filter = this.sql.parseQuery(query)
    if (filter === '0') return
    await this.query('DELETE FROM ?? WHERE ' + filter, [table])
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
    const { model, table, ref } = sel

    const merged = {}
    const insertion = data.map((item) => {
      Object.assign(merged, item)
      return model.format(executeUpdate(model.create(), item, ref))
    })
    const initFields = Object.keys(model.fields)
    const dataFields = [...new Set(Object.keys(merged).map((key) => {
      return initFields.find(field => field === key || key.startsWith(field + '.'))!
    }))]
    const updateFields = difference(dataFields, keys)

    const createFilter = (item: any) => this.sql.parseQuery(pick(item, keys))
    const createMultiFilter = (items: any[]) => {
      if (items.length === 1) {
        return createFilter(items[0])
      } else if (keys.length === 1) {
        const key = keys[0]
        return this.sql.parseQuery({ [key]: items.map(item => item[key]) })
      } else {
        return items.map(createFilter).join(' OR ')
      }
    }

    const update = updateFields.map((field) => {
      const escaped = escapeId(field)
      const branches: Dict<any[]> = {}
      data.forEach((item) => {
        (branches[this.toUpdateExpr(item, field, true)] ??= []).push(item)
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
