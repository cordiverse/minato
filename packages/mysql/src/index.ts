import { Binary, Dict, difference, isNullable, makeArray, pick } from 'cosmokit'
import { createPool, format } from '@vlasky/mysql'
import type { OkPacket, Pool, PoolConfig, PoolConnection } from 'mysql'
import { Driver, Eval, executeUpdate, Field, RuntimeError, Selection, z } from 'minato'
import { escapeId, isBracketed } from '@minatojs/sql-utils'
import { Compat, MySQLBuilder } from './builder'
import zhCN from './locales/zh-CN.yml'
import enUS from './locales/en-US.yml'

declare module 'mysql' {
  interface UntypedFieldInfo {
    packet: UntypedFieldInfo
  }
}

const timeRegex = /(\d+):(\d+):(\d+)(\.(\d+))?/

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
  SEQ_IN_INDEX: string
  COLLATION: 'A' | 'D'
  NULLABLE: string
  NON_UNIQUE: string
}

interface QueryTask {
  sql: string
  resolve: (value: any) => void
  reject: (reason: unknown) => void
}

export class MySQLDriver extends Driver<MySQLDriver.Config> {
  static name = 'mysql'

  public pool!: Pool
  public sql: MySQLBuilder = new MySQLBuilder(this)

  private session?: PoolConnection
  private _compat: Compat = {}
  private _queryTasks: QueryTask[] = []

  async start() {
    this.pool = createPool({
      host: 'localhost',
      port: 3306,
      charset: 'utf8mb4_general_ci',
      multipleStatements: true,
      typeCast: (field, next) => {
        const { orgName, orgTable } = field.packet
        const meta = this.database.tables[orgTable]?.fields[orgName]

        if (field.type === 'BIT') {
          return Boolean(field.buffer()?.readUint8(0))
        } else if (meta?.type?.type === 'json') {
        // for backward compatibility
          return field.string() || meta.initial
        } else if (field.type === 'LONGLONG') {
          return field.string()
        } else {
          return next()
        }
      },
      ...this.config,
    })

    const [version, timezone] = Object.values((await this.query(`SELECT version(), @@GLOBAL.time_zone`))[0]) as string[]
    // https://jira.mariadb.org/browse/MDEV-30623
    this._compat.maria = version.includes('MariaDB')
    // https://jira.mariadb.org/browse/MDEV-26506
    this._compat.maria105 = !!version.match(/10.5.\d+-MariaDB/)
    // For json_table
    this._compat.mysql57 = !!version.match(/5.7.\d+/)

    this._compat.timezone = timezone

    if (this._compat.mysql57 || this._compat.maria) {
      await this._setupCompatFunctions()
    }

    this.define<object, string>({
      types: ['json'],
      dump: value => value as any,
      load: value => typeof value === 'string' ? JSON.parse(value) : value,
    })

    this.define<string[], string>({
      types: ['list'],
      dump: value => Array.isArray(value) ? value.join(',') : value,
      load: value => value ? value.split(',') : [],
    })

    this.define<Date, any>({
      types: ['time'],
      dump: value => value,
      load: (value) => {
        if (!value || typeof value === 'object') return value
        const date = new Date(0)
        const parsed = timeRegex.exec(value)
        if (!parsed) throw Error(`unexpected time value: ${value}`)
        date.setHours(+parsed[1], +parsed[2], +parsed[3], +(parsed[5] ?? 0))
        return date
      },
    })

    this.define<ArrayBuffer, ArrayBuffer>({
      types: ['binary'],
      dump: value => value,
      load: value => isNullable(value) ? value : Binary.fromSource(value),
    })

    this.define<number, number>({
      types: Field.number as any,
      dump: value => value,
      load: value => isNullable(value) ? value : +value,
    })

    this.define<bigint, string>({
      types: ['bigint'],
      dump: value => isNullable(value) ? value : value.toString(),
      load: value => isNullable(value) ? value : BigInt(value),
    })
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
    const fields = table.avaiableFields()
    const unique = [...table.unique]
    const create: string[] = []
    const update: string[] = []
    const alterInit = Object.create(null)

    // field definitions
    for (const key in fields) {
      const { initial, nullable = true } = fields[key]!
      const legacy = [key, ...fields[key]!.legacy || []]
      const column = columns.find(info => legacy.includes(info.COLUMN_NAME))
      let shouldUpdate = column?.COLUMN_NAME !== key

      let def = escapeId(key)
      if (key === primary && autoInc) {
        def += ' int unsigned not null auto_increment'
      } else {
        const typedef = this.getTypeDef(fields[key]!)
        if (column && !shouldUpdate) {
          shouldUpdate = this.isDefUpdated(fields[key]!, column, typedef)
        }
        def += ' ' + typedef
        if (makeArray(primary).includes(key)) {
          def += ' not null'
        } else {
          def += (nullable ? ' ' : ' not ') + 'null'
        }
        // blob, text, geometry or json columns cannot have default values
        if (!isNullable(initial) && !typedef.startsWith('text') && !typedef.endsWith('blob')) {
          def += ' default ' + this.sql.escape(initial, fields[key])
        }

        if (!column && !isNullable(initial) && (typedef.startsWith('text') || typedef.endsWith('blob'))) {
          alterInit[key] = this.sql.escape(initial, fields[key])
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
      this.logger.info('auto creating table %c', name)
      return this.query(`CREATE TABLE ${escapeId(name)} (${create.join(', ')}) COLLATE = ${this.sql.escape(this.config.charset ?? 'utf8mb4_general_ci')}`)
    }

    const operations = [
      ...create.map(def => 'ADD ' + def),
      ...update,
    ]
    if (operations.length) {
      // https://dev.mysql.com/doc/refman/5.7/en/alter-table.html
      this.logger.info('auto updating table %c', name)
      await this.query(`ALTER TABLE ${escapeId(name)} ${operations.join(', ')}`)
    }

    if (Object.keys(alterInit).length) {
      await this.query(`UPDATE ${escapeId(name)} SET ${Object.entries(alterInit).map(([key, val]) => `${escapeId(key)} = ${val}`).join(', ')}`)
    }

    // migrate deprecated fields (do not await)
    const dropKeys: string[] = []
    await this.migrate(name, {
      error: this.logger.warn,
      before: keys => keys.every(key => columns.some(info => info.COLUMN_NAME === key)),
      after: keys => dropKeys.push(...keys),
      finalize: async () => {
        if (!dropKeys.length) return
        this.logger.info('auto migrating table %c', name)
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

  async _setupCompatFunctions() {
    try {
      await this.query(`DROP FUNCTION IF EXISTS minato_cfunc_sum`)
      await this.query(`CREATE FUNCTION minato_cfunc_sum (j JSON) RETURNS DOUBLE DETERMINISTIC BEGIN DECLARE n int; DECLARE i int; DECLARE r DOUBLE;
DROP TEMPORARY TABLE IF EXISTS mtt; CREATE TEMPORARY TABLE mtt (value JSON); SELECT json_length(j) into n; set i = 0; WHILE i<n DO
INSERT INTO mtt VALUES(json_extract(j, concat('$[', i, ']'))); SET i=i+1; END WHILE; SELECT sum(value) INTO r FROM mtt; RETURN r; END`)
      await this.query(`DROP FUNCTION IF EXISTS minato_cfunc_avg`)
      await this.query(`CREATE FUNCTION minato_cfunc_avg (j JSON) RETURNS DOUBLE DETERMINISTIC BEGIN DECLARE n int; DECLARE i int; DECLARE r DOUBLE;
DROP TEMPORARY TABLE IF EXISTS mtt; CREATE TEMPORARY TABLE mtt (value JSON); SELECT json_length(j) into n; set i = 0; WHILE i<n DO
INSERT INTO mtt VALUES(json_extract(j, concat('$[', i, ']'))); SET i=i+1; END WHILE; SELECT avg(value) INTO r FROM mtt; RETURN r; END`)
      await this.query(`DROP FUNCTION IF EXISTS minato_cfunc_min`)
      await this.query(`CREATE FUNCTION minato_cfunc_min (j JSON) RETURNS DOUBLE DETERMINISTIC BEGIN DECLARE n int; DECLARE i int; DECLARE r DOUBLE;
DROP TEMPORARY TABLE IF EXISTS mtt; CREATE TEMPORARY TABLE mtt (value JSON); SELECT json_length(j) into n; set i = 0; WHILE i<n DO
INSERT INTO mtt VALUES(json_extract(j, concat('$[', i, ']'))); SET i=i+1; END WHILE; SELECT min(value) INTO r FROM mtt; RETURN r; END`)
      await this.query(`DROP FUNCTION IF EXISTS minato_cfunc_max`)
      await this.query(`CREATE FUNCTION minato_cfunc_max (j JSON) RETURNS DOUBLE DETERMINISTIC BEGIN DECLARE n int; DECLARE i int; DECLARE r DOUBLE;
DROP TEMPORARY TABLE IF EXISTS mtt; CREATE TEMPORARY TABLE mtt (value JSON); SELECT json_length(j) into n; set i = 0; WHILE i<n DO
INSERT INTO mtt VALUES(json_extract(j, concat('$[', i, ']'))); SET i=i+1; END WHILE; SELECT max(value) INTO r FROM mtt; RETURN r; END`)
    } catch (e) {
      this.logger.warn(`Failed to setup compact functions: ${e}`)
    }
  }

  query<T = any>(sql: string, debug = true): Promise<T> {
    const error = new Error()
    return new Promise((resolve, reject) => {
      if (debug) this.logger.debug('> %s', sql)
      ;(this.session ?? this.pool).query(sql, (err: Error, results) => {
        if (!err) return resolve(results)
        this.logger.warn('> %s', sql)
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
    if (this.session || !(this.config.multipleStatements ?? true)) {
      return this.query(sql)
    }

    this.logger.debug('> %s', sql)
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
      let results = await this.query(tasks.map(task => task.sql).join('; '), false)
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

  async drop(table: string) {
    await this.query(`DROP TABLE ${escapeId(table)}`)
  }

  async dropAll() {
    const data = await this._select('information_schema.tables', ['TABLE_NAME'], 'TABLE_SCHEMA = ?', [this.config.database])
    if (!data.length) return
    await this.query([
      'SET foreign_key_checks = 0',
      ...data.map(({ TABLE_NAME }) => `DROP TABLE ${escapeId(TABLE_NAME)}`),
      'SET foreign_key_checks = 1',
    ].join('; '))
  }

  async stats() {
    const data = await this._select('information_schema.tables', ['TABLE_NAME', 'TABLE_ROWS', 'DATA_LENGTH'], 'TABLE_SCHEMA = ?', [this.config.database])
    const stats: Partial<Driver.Stats> = { size: 0 }
    stats.tables = Object.fromEntries(data.map(({ TABLE_NAME: name, TABLE_ROWS: count, DATA_LENGTH: size }) => {
      stats.size! += +size
      return [name, { count: +count, size: +size }]
    }))
    return stats
  }

  async get(sel: Selection.Immutable) {
    const { model, tables } = sel
    const builder = new MySQLBuilder(this, tables, this._compat)
    const sql = builder.get(sel)
    if (!sql) return []
    return Promise.all([...builder.prequeries, sql].map(x => this.queue(x))).then((data) => {
      return data.at(-1).map((row) => builder.load(row, model))
    })
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const builder = new MySQLBuilder(this, sel.tables, this._compat)
    const inner = builder.get(sel.table as Selection, true, true)
    const output = builder.parseEval(expr, false)
    const ref = isBracketed(inner) ? sel.ref : ''
    const sql = `SELECT ${output} AS value FROM ${inner} ${ref}`
    return Promise.all([...builder.prequeries, sql].map(x => this.queue(x))).then((data) => {
      return builder.load(data.at(-1)[0].value, expr)
    })
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { model, query, table, tables, ref } = sel
    const builder = new MySQLBuilder(this, tables, this._compat)
    const filter = builder.parseQuery(query)
    const fields = model.avaiableFields()
    if (filter === '0') return {}
    const updateFields = [...new Set(Object.keys(data).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]

    const update = updateFields.map((field) => {
      const escaped = escapeId(field)
      return `${escaped} = ${builder.toUpdateExpr(data, field, fields[field], false)}`
    }).join(', ')
    const sql = [...builder.prequeries, `UPDATE ${escapeId(table)} ${ref} SET ${update} WHERE ${filter}`].join('; ')
    const result = await this.query(sql)
    return { matched: result.affectedRows, modified: result.changedRows }
  }

  async remove(sel: Selection.Mutable) {
    const { query, table, tables } = sel
    const builder = new MySQLBuilder(this, tables, this._compat)
    const filter = builder.parseQuery(query)
    if (filter === '0') return {}
    const result = await this.query(`DELETE FROM ${escapeId(table)} WHERE ` + filter)
    return { matched: result.affectedRows, removed: result.affectedRows }
  }

  async create(sel: Selection.Mutable, data: {}) {
    const { table, model } = sel
    const { autoInc, primary } = model
    const formatted = this.sql.dump(data, model)
    const keys = Object.keys(formatted)
    const header = await this.query<OkPacket>([
      `INSERT INTO ${escapeId(table)} (${keys.map(escapeId).join(', ')})`,
      `VALUES (${keys.map(key => this.sql.escape(formatted[key])).join(', ')})`,
    ].join(' '))
    if (!autoInc) return data as any
    return { ...data, [primary as string]: header.insertId } as any
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return {}
    const { model, table, tables, ref } = sel
    const builder = new MySQLBuilder(this, tables, this._compat)

    const merged = {}
    const insertion = data.map((item) => {
      Object.assign(merged, item)
      return model.format(executeUpdate(model.create(), item, ref))
    })
    const initFields = Object.keys(model.avaiableFields())
    const dataFields = [...new Set(Object.keys(merged).map((key) => {
      return initFields.find(field => field === key || key.startsWith(field + '.'))!
    }))]
    let updateFields = difference(dataFields, keys)
    if (!updateFields.length) updateFields = dataFields.length ? [dataFields[0]] : []

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

    const result = await this.query([
      `INSERT INTO ${escapeId(table)} (${initFields.map(escapeId).join(', ')})`,
      `VALUES (${insertion.map(item => this._formatValues(table, item, initFields)).join('), (')})`,
      update ? `ON DUPLICATE KEY UPDATE ${update}` : '',
    ].join(' '))
    const records = +(/^&Records:\s*(\d+)/.exec(result.message)?.[1] ?? result.affectedRows)
    if (!result.message && !result.insertId) return { inserted: 0, matched: result.affectedRows, modified: 0 }
    if (!result.message && result.affectedRows > 1) return { inserted: 0, matched: result.affectedRows / 2, modified: result.affectedRows / 2 }
    return { inserted: records - result.changedRows, matched: result.changedRows, modified: result.affectedRows - records }
  }

  async withTransaction(callback: (session: any) => Promise<void>) {
    return new Promise<void>((resolve, reject) => {
      this.pool.getConnection((err, conn) => {
        if (err) {
          this.logger.warn('getConnection failed: ', err)
          return
        }
        conn.beginTransaction(() => callback(conn).then(
          () => conn.commit(() => resolve()),
          (e) => conn.rollback(() => reject(e)),
        ).finally(() => conn.release()))
      })
    })
  }

  async getIndexes(table: string) {
    const indexes = await this.queue<IndexInfo[]>([
      `SELECT *`,
      `FROM information_schema.statistics`,
      `WHERE TABLE_SCHEMA = ? && TABLE_NAME = ?`,
    ].join(' '), [this.config.database, table])
    const result: Dict<Driver.Index> = {}
    for (const { INDEX_NAME: name, COLUMN_NAME: key, COLLATION: direction, NON_UNIQUE: unique } of indexes) {
      if (!result[name]) result[name] = { name, unique: unique !== '1', keys: {} }
      result[name].keys[key] = direction === 'A' ? 'asc' : direction === 'D' ? 'desc' : direction
    }
    return Object.values(result)
  }

  async createIndex(table: string, index: Driver.Index) {
    const keyFields = Object.entries(index.keys).map(([key, direction]) => `${escapeId(key)} ${direction ?? 'asc'}`).join(', ')
    await this.query(
      `ALTER TABLE ${escapeId(table)} ADD ${index.unique ? 'UNIQUE' : ''} INDEX ${index.name ? escapeId(index.name) : ''} (${keyFields})`,
    )
  }

  async dropIndex(table: string, name: string) {
    await this.query(`DROP INDEX ${escapeId(name)} ON ${escapeId(table)}`)
  }

  private getTypeDef({ deftype: type, length, precision, scale }: Field) {
    const getIntegerType = (length = 4) => {
      if (length <= 1) return 'tinyint'
      if (length <= 2) return 'smallint'
      if (length <= 3) return 'mediumint'
      if (length <= 4) return 'int'
      return 'bigint'
    }

    switch (type) {
      case 'float':
      case 'double':
      case 'date': return type
      case 'time': return 'time(3)'
      case 'timestamp': return 'datetime(3)'
      case 'boolean': return 'bit'
      case 'integer':
        if ((length || 0) > 8) this.logger.warn(`type ${type}(${length}) exceeds the max supported length`)
        return getIntegerType(length)
      case 'primary':
      case 'unsigned':
        if ((length || 0) > 8) this.logger.warn(`type ${type}(${length}) exceeds the max supported length`)
        return `${getIntegerType(length)} unsigned`
      case 'bigint': return getIntegerType(8)
      case 'decimal': return `decimal(${precision ?? 10}, ${scale ?? 0}) unsigned`
      case 'char': return `char(${length || 255})`
      case 'string': return (length || 255) > 65536 ? 'longtext' : `varchar(${length || 255})`
      case 'text': return (length || 255) > 65536 ? 'longtext' : `text(${length || 65535})`
      case 'binary': return (length || 65537) > 65536 ? 'longblob' : `blob`
      case 'list': return `text(${length || 65535})`
      case 'json': return `text(${length || 65535})`
      default: throw new Error(`unsupported type: ${type}`)
    }
  }

  private isDefUpdated(field: Field, column: ColumnInfo, def: string) {
    const typename = def.split(/[ (]/)[0]
    if (typename === 'text') return !column.DATA_TYPE.endsWith('text')
    if (typename !== column.DATA_TYPE) return true
    switch (field.deftype) {
      case 'integer':
      case 'unsigned':
      case 'char':
      case 'string':
        return !!field.length && !!column.CHARACTER_MAXIMUM_LENGTH && +column.CHARACTER_MAXIMUM_LENGTH !== field.length
      case 'decimal':
        return +column.NUMERIC_PRECISION !== field.precision || +column.NUMERIC_SCALE !== field.scale
      case 'text':
      case 'list':
      case 'json':
        return false
      default: return false
    }
  }
}

export namespace MySQLDriver {
  export interface Config extends PoolConfig {}

  export const Config: z<Config> = z.intersect([
    z.object({
      host: z.string().default('localhost'),
      port: z.natural().max(65535).default(3306),
      user: z.string().default('root'),
      password: z.string().role('secret'),
      database: z.string().required(),
    }),
    z.object({
      ssl: z.union([
        z.const(undefined),
        z.object({
          ca: z.string(),
          cert: z.string(),
          sigalgs: z.string(),
          ciphers: z.string(),
          clientCertEngine: z.string(),
          crl: z.string(),
          dhparam: z.string(),
          ecdhCurve: z.string(),
          honorCipherOrder: z.boolean(),
          key: z.string(),
          privateKeyEngine: z.string(),
          privateKeyIdentifier: z.string(),
          maxVersion: z.string(),
          minVersion: z.string(),
          passphrase: z.string(),
          pfx: z.string(),
          rejectUnauthorized: z.boolean(),
          secureOptions: z.natural(),
          secureProtocol: z.string(),
          sessionIdContext: z.string(),
          sessionTimeout: z.number(),
        }),
      ]) as any,
    }),
  ]).i18n({
    'en-US': enUS,
    'zh-CN': zhCN,
  })
}

export default MySQLDriver
