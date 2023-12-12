import postgres from 'postgres'
import { Dict, difference, isNullable, makeArray, pick, Time } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, isEvalExpr, Model, randomId, Selection } from '@minatojs/core'
import { Builder } from '@minatojs/sql-utils'
import Logger from 'reggol'

const logger = new Logger('postgres')
const timeRegex = /(\d+):(\d+):(\d+)/

interface ColumnInfo {
  table_catalog: string
  table_schema: string
  table_name: string
  column_name: string
  ordinal_position: number
  column_default: any
  is_nullable: string
  data_type: string
  character_maximum_length: number
  is_identity: string
  is_updatable: string
}

interface TableInfo {
  table_catalog: string
  table_schema: string
  table_name: string
  table_type: string
  self_referencing_column_name: null
  reference_generation: null
  user_defined_type_catalog: null
  user_defined_type_schema: null
  user_defined_type_name: null
  is_insertable_into: string
  is_typed: string
  commit_action: null
}

interface FieldInfo {
  key: string
  names: string[]
  field?: Field
  column?: ColumnInfo | undefined
  def?: string
}

function escapeId(value: string) {
  return '"' + value.replace(/"/g, '""') + '"'
}

function getTypeDef(field: Field & { autoInc?: boolean }) {
  let { type, length, precision, scale, initial, autoInc } = field
  let def = ''
  if (['primary', 'unsigned', 'integer'].includes(type)) {
    length ||= 4
    if (precision) def += `NUMERIC(${precision}, ${scale ?? 0})`
    else if (length <= 2) def += autoInc ? 'SMALLSERIAL' : 'SMALLINT'
    else if (length <= 4) def += autoInc ? 'SERIAL' : 'INTEGER'
    else {
      if (length > 8) logger.warn(`type ${type}(${length}) exceeds the max supported length`)
      def += autoInc ? 'BIGSERIAL' : 'BIGINT'
    }

    if (!isNullable(initial) && !autoInc) def += ` DEFAULT ${initial}`
  } else if (type === 'decimal') {
    def += `DECIMAL(${precision}, ${scale})`
    if (!isNullable(initial)) def += ` DEFAULT ${initial}`
  } else if (type === 'float') {
    def += 'REAL'
    if (!isNullable(initial)) def += ` DEFAULT ${initial}`
  } else if (type === 'double') {
    def += 'DOUBLE PRECISION'
    if (!isNullable(initial)) def += ` DEFAULT ${initial}`
  } else if (type === 'char') {
    def += `VARCHAR(${length || 64}) `
    if (!isNullable(initial)) def += ` DEFAULT '${initial.replace(/'/g, "''")}'`
  } else if (type === 'string') {
    def += `VARCHAR(${length || 255})`
    if (!isNullable(initial)) def += ` DEFAULT '${initial.replace(/'/g, "''")}'`
  } else if (type === 'text') {
    def += `TEXT`
    if (!isNullable(initial)) def += ` DEFAULT '${initial.replace(/'/g, "''")}'`
  } else if (type === 'boolean') {
    def += 'BOOLEAN'
    if (!isNullable(initial)) def += ` DEFAULT ${initial}`
  } else if (type === 'list') {
    def += 'TEXT[]'
    if (initial) {
      def += ` DEFAULT ${transformArray(initial)}`
    }
  } else if (type === 'json') {
    def += 'JSONB'
    if (initial) def += ` DEFAULT '${JSON.stringify(initial)}'::JSONB` // TODO
  } else if (type === 'date') {
    def += 'TIMESTAMP WITH TIME ZONE'
    if (initial) def += ` DEFAULT ${formatTime(initial)}`
  } else if (type === 'time') {
    def += 'TIME WITH TIME ZONE'
    if (initial) def += ` DEFAULT ${formatTime(initial)}`
  } else if (type === 'timestamp') {
    def += 'TIMESTAMP WITH TIME ZONE'
    if (initial) def += ` DEFAULT ${formatTime(initial)}`
  } else throw new Error(`unsupported type: ${type}`)

  return def
}

function formatTime(time: Date) {
  const year = time.getFullYear().toString()
  const month = Time.toDigits(time.getMonth() + 1)
  const date = Time.toDigits(time.getDate())
  const hour = Time.toDigits(time.getHours())
  const min = Time.toDigits(time.getMinutes())
  const sec = Time.toDigits(time.getSeconds())
  const ms = Time.toDigits(time.getMilliseconds(), 3)
  let timezone = Time.toDigits(time.getTimezoneOffset() / -60)
  if (!timezone.startsWith('-')) timezone = `+${timezone}`
  return `${year}-${month}-${date} ${hour}:${min}:${sec}.${ms}${timezone}`
}

function transformArray(arr: any[]) {
  return `ARRAY[${arr.map(v => `'${v.replace(/'/g, "''")}'`).join(',')}]::TEXT[]`
}

class PostgresBuilder extends Builder {
  // eslint-disable-next-line no-control-regex
  protected escapeRegExp = /[\0\b\t\n\r\x1a'\\]/g
  protected escapeMap = {
    '\0': '\\0',
    '\b': '\\b',
    '\t': '\\t',
    '\n': '\\n',
    '\r': '\\r',
    '\x1a': '\\Z',
    '\'': '\'\'',
    '\\': '\\\\',
  }

  protected $true = 'TRUE'
  protected $false = 'FALSE'

  constructor(public tables?: Dict<Model>) {
    super(tables)

    this.queryOperators = {
      ...this.queryOperators,
      $regex: (key, value) => this.createRegExpQuery(key, value),
      $regexFor: (key, value) => `${this.escape(value)} ~ ${key}`,
      $size: (key, value) => {
        if (!value) return this.logicalNot(key)
        if (this.state.sqlTypes?.[this.unescapeId(key)] === 'json') {
          return `${this.jsonLength(key)} = ${this.escape(value)}`
        } else {
          return `${key} IS NOT NULL AND ARRAY_LENGTH(${key}, 1) = ${value}`
        }
      },
    }

    this.evalOperators = {
      ...this.evalOperators,
      $if: (args) => {
        const type = this.getLiteralType(args[1]) ?? this.getLiteralType(args[2]) ?? 'text'
        return `(SELECT CASE WHEN ${this.parseEval(args[0], 'boolean')} THEN ${this.parseEval(args[1], type)} ELSE ${this.parseEval(args[2], type)} END)`
      },
      $ifNull: (args) => {
        const type = args.map(this.getLiteralType).find(x => x) ?? 'text'
        return `coalesce(${args.map(arg => this.parseEval(arg, type)).join(', ')})`
      },

      // number
      $add: (args) => `(${args.map(arg => this.parseEval(arg, 'double precision')).join(' + ')})`,
      $multiply: (args) => `(${args.map(arg => this.parseEval(arg, 'double precision')).join(' * ')})`,

      $eq: this.binary('=', 'text'),

      $sum: (expr) => this.createAggr(expr, value => `coalesce(sum(${value})::double precision, 0)`, undefined, 'double precision'),
      $avg: (expr) => this.createAggr(expr, value => `avg(${value})::double precision`, undefined, 'double precision'),
      $min: (expr) => this.createAggr(expr, value => `min(${value})`, undefined, 'double precision'),
      $max: (expr) => this.createAggr(expr, value => `max(${value})`, undefined, 'double precision'),
      $count: (expr) => this.createAggr(expr, value => `count(distinct ${value})::integer`),
      $length: (expr) => this.createAggr(expr, value => `count(${value})::integer`, value => {
        if (this.state.sqlType === 'json') {
          this.state.sqlType = 'raw'
          return `${this.jsonLength(value)}`
        } else {
          this.state.sqlType = 'raw'
          return `COALESCE(ARRAY_LENGTH(${value}, 1), 0)`
        }
      }),

      $concat: (args) => `${args.map(arg => this.parseEval(arg, 'text')).join('||')}`,
    }

    this.define<Date, string>({
      types: ['time'],
      dump: date => date ? formatTime(date) : null,
      load: str => {
        if (isNullable(str)) return str
        const date = new Date(0)
        const parsed = timeRegex.exec(str)
        if (!parsed) throw Error(`unexpected time value: ${str}`)
        date.setHours(+parsed[1], +parsed[2], +parsed[3])
        return date
      },
    })

    this.define<string[], any>({
      types: ['list'],
      dump: value => '{' + value.join(',') + '}',
      load: value => value,
    })
  }

  upsert(table: string) {
    this.modifiedTable = table
  }

  protected binary(operator: string, eltype: string = 'double precision') {
    return ([left, right]) => {
      const type = this.getLiteralType(left) ?? this.getLiteralType(right) ?? eltype
      return `(${this.parseEval(left, type)} ${operator} ${this.parseEval(right, type)})`
    }
  }

  private getLiteralType(expr: any) {
    if (typeof expr === 'string') return 'text'
    else if (typeof expr === 'number') return 'double precision'
    else if (typeof expr === 'string') return 'boolean'
  }

  parseEval(expr: any, outtype: boolean | string = false): string {
    this.state.sqlType = 'raw'
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date) {
      return this.escape(expr)
    }
    return outtype ? this.jsonUnquote(this.parseEvalExpr(expr), false, typeof outtype === 'string' ? outtype : undefined) : this.parseEvalExpr(expr)
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} ~ ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    if (this.state.sqlTypes?.[this.unescapeId(key)] === 'json') {
      return this.jsonContains(key, this.quote(JSON.stringify(value)))
    } else {
      return `${key} && ARRAY['${value}']::TEXT[]`
    }
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string, eltype?: string) {
    if (!this.state.group && !nonaggr) {
      const value = this.parseEval(expr, false)
      return `(select ${aggr(this.jsonUnquote(this.escapeId('value'), true, eltype))} from jsonb_array_elements(${value}) ${randomId()})`
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected transformJsonField(obj: string, path: string) {
    this.state.sqlType = 'json'
    return `jsonb_extract_path(${obj}, ${path.slice(1).replace('.', ',')})`
  }

  protected jsonLength(value: string) {
    return `jsonb_array_length(${value})`
  }

  protected jsonContains(obj: string, value: string) {
    return `(${obj} @> ${value})`
  }

  protected jsonUnquote(value: string, pure: boolean = false, type?: string) {
    if (pure && type) return `(jsonb_build_object('v', ${value})->>'v')::${type}`
    const res = type && this.state.sqlType === 'json' ? `(jsonb_build_object('v', ${value})->>'v')::${type}` : value
    this.state.sqlType = 'raw'
    return res
  }

  protected jsonQuote(value: string, pure: boolean = false) {
    if (pure) return `to_jsonb(${value})`
    const res = this.state.sqlType === 'raw' ? `to_jsonb(${value})` : value
    this.state.sqlType = 'json'
    return res
  }

  protected groupObject(fields: any) {
    const parse = (expr) => {
      const value = this.parseEval(expr, false)
      return this.state.sqlType === 'json' ? `to_jsonb(${value})` : `${value}`
    }
    const res = `jsonb_build_object(` + Object.entries(fields).map(([key, expr]) => `'${key}', ${parse(expr)}`).join(',') + `)`
    this.state.sqlType = 'json'
    return res
  }

  protected groupArray(value: string) {
    this.state.sqlType = 'json'
    return `coalesce(jsonb_agg(${value}), '[]'::jsonb)`
  }

  escapeId = escapeId

  escapeKey(value: string) {
    return `'${value}'`
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) {
      value = formatTime(value)
    } else if (!field && !!value && typeof value === 'object') {
      return `${this.quote(JSON.stringify(value))}::jsonb`
    }
    return super.escape(value, field)
  }

  toUpdateExpr(item: any, key: string, field?: Field, upsert?: boolean) {
    const escaped = this.escapeId(key)
    // update directly
    if (key in item) {
      if (!isEvalExpr(item[key]) && upsert) {
        return `excluded.${escaped}`
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
      rest.reduce((obj, k) => obj[k] ??= {}, jsonInit)
    }

    // update with json_set
    const valueInit = this.modifiedTable ? `coalesce(${this.escapeId(this.modifiedTable)}.${escaped}, '{}')::jsonb` : `coalesce(${escaped}, '{}')::jsonb`
    let value = valueInit

    // json_set cannot create deeply nested property when non-exist
    // therefore we merge a layout to it
    if (Object.keys(jsonInit).length !== 0) {
      value = `(${value} || jsonb ${this.quote(JSON.stringify(jsonInit))})`
    }

    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      value = `jsonb_set(${value}, '{${rest.map(key => `"${key}"`).join(',')}}', ${this.jsonQuote(this.parseEval(item[prop]), true)}, true)`
    }

    if (value === valueInit) {
      return this.modifiedTable ? `${this.escapeId(this.modifiedTable)}.${escaped}` : escaped
    } else {
      return value
    }
  }
}

export namespace PostgresDriver {
  export interface Config<T extends Record<string, postgres.PostgresType> = {}> extends postgres.Options<T> {
    host: string
    port: number
    username: string
    password: string
    database: string
  }
}

export class PostgresDriver extends Driver {
  public sql!: postgres.Sql
  public config: PostgresDriver.Config

  private session?: postgres.TransactionSql
  private _counter = 0

  constructor(database: Database, config: PostgresDriver.Config) {
    super(database)

    this.config = {
      onnotice: () => { },
      debug(_, query, parameters) {
        logger.debug(`> %s` + (parameters.length ? `\nparameters: %o` : ``), query, parameters.length ? parameters : '')
      },
      ...config,
    }
  }

  async start() {
    this.sql = postgres(this.config)
  }

  async stop() {
    await this.sql.end()
  }

  async query(sql: string) {
    return await (this.session ?? this.sql).unsafe(sql).catch(e => {
      logger.warn('> %s', sql)
      throw e
    })
  }

  async prepare(name: string) {
    const columns: ColumnInfo[] = await this.sql`
      SELECT *
      FROM information_schema.columns
      WHERE table_schema = 'public'
      AND table_name = ${name}`
    const table = this.model(name)
    const { fields } = table
    const primary = makeArray(table.primary)

    const create: FieldInfo[] = []
    const rename: FieldInfo[] = []
    Object.entries(fields).forEach(([key, field]) => {
      const names = [key].concat(field?.legacy ?? [])
      const column = columns?.find(c => names.includes(c.column_name))
      const isPrimary = primary.includes(key)

      let def: string | undefined
      if (!column) {
        def = getTypeDef(Object.assign({
          primary: isPrimary,
          autoInc: isPrimary && table.autoInc,
        }, field))
      }

      const info = { key, field, names, column, def }

      if (!column) create.push(info)
      else if (key !== column.column_name) rename.push(info)
    })

    if (!columns?.length) {
      await this.sql`
        CREATE TABLE ${this.sql(name)}
        (${this.sql.unsafe(create.map(f => `"${f.key}" ${f.def}`).join(','))}, _pg_mtime BIGINT,
        PRIMARY KEY(${this.sql(primary)}))`
      return
    }

    if (rename?.length) {
      await this.sql.unsafe(
        rename.map(f => `ALTER TABLE "${name}" RENAME "${f.column?.column_name}" TO "${f.key}"`).join(';'),
      )
    }

    if (create?.length) {
      await this.sql.unsafe(
        `ALTER TABLE "${name}" ${create.map(f => `ADD "${f.key}" ${f.def}`).join(',')}`,
      )
    }

    const drop: string[] = []
    this.migrate(name, {
      error: logger.warn,
      before: keys => keys.every(key => columns.some(c => c.column_name === key)),
      after: keys => drop.push(...keys),
      finalize: async () => {
        if (!drop.length) return
        logger.info('auto migrating table %c', name)
        await this.sql`
          ALTER TABLE ${this.sql(name)}
          ${this.sql.unsafe(drop.map(key => `DROP "${key}"`).join(', '))}`
      },
    })
  }

  async drop(table?: string) {
    if (table) {
      await this.sql`DROP TABLE IF EXISTS "${this.sql(table)}" CASCADE`
      return
    }
    const tables: TableInfo[] = await this.sql`
      SELECT *
      FROM information_schema.tables
      WHERE table_schema = 'public'`
    if (!tables.length) return
    await this.sql`DROP TABLE IF EXISTS ${this.sql(tables.map(t => t.table_name))} CASCADE`
  }

  async stats(): Promise<Partial<Driver.Stats>> {
    const tables = await this.sql`
      SELECT *
      FROM information_schema.tables
      WHERE table_schema = 'public'`
    const tableStats = await this.query(
      tables.map(({ table_name: name }) => {
        return `SELECT '${name}' AS name,
          pg_total_relation_size('${name}') AS size,
          COUNT(*) AS count FROM ${escapeId(name)}`
      }).join(' UNION '),
    ).then(s => s.map(t => [t.name, { size: +t.size, count: +t.count }]))

    return {
      size: tableStats.reduce((p, c) => p += c[1].size, 0),
      tables: Object.fromEntries(tableStats),
    }
  }

  async get(sel: Selection.Immutable) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.get(sel)
    if (!query) return []
    return this.query(query).then(data => {
      return data.map(row => builder.load(sel.model, row))
    })
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr<any, boolean>) {
    const builder = new PostgresBuilder(sel.tables)
    const inner = builder.get(sel.table as Selection, true, true)
    const output = builder.parseEval(expr, false)
    const ref = inner.startsWith('(') && inner.endsWith(')') ? sel.ref : ''
    const [data] = await this.query(`SELECT ${output} AS value FROM ${inner} ${ref}`)
    return builder.load(data?.value)
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { model, query, table, tables, ref } = sel
    const builder = new PostgresBuilder(tables)
    const filter = builder.parseQuery(query)
    const { fields } = model
    if (filter === '0') return {}
    const updateFields = [...new Set(Object.keys(data).map((key) => {
      return Object.keys(fields).find(field => field === key || key.startsWith(field + '.'))!
    }))]

    const update = updateFields.map((field) => {
      const escaped = builder.escapeId(field)
      return `${escaped} = ${builder.toUpdateExpr(data, field, fields[field], false)}`
    }).join(', ')
    const result = await this.query(`UPDATE ${builder.escapeId(table)} ${ref} SET ${update} WHERE ${filter} RETURNING *`)
    return { matched: result.length }
  }

  async remove(sel: Selection.Mutable) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseQuery(sel.query)
    if (query === 'FALSE') return {}
    const { count } = await this.query(`DELETE FROM ${sel.table} WHERE ${query}`)
    return { matched: count, removed: count }
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table, model } = sel
    const builder = new PostgresBuilder(sel.tables)
    const formatted = builder.dump(model, data)
    const keys = Object.keys(formatted)
    const [row] = await this.query(`
      INSERT INTO ${builder.escapeId(table)} (${keys.map(builder.escapeId).join(', ')})
      VALUES (${keys.map(key => builder.escape(formatted[key])).join(', ')})
      RETURNING *`)
    return builder.load(model, row)
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return {}
    const { model, table, tables, ref } = sel
    const builder = new PostgresBuilder(tables)
    builder.upsert(table)

    this._counter = (this._counter + 1) % 256
    const mtime = Date.now() * 256 + this._counter
    const merged = {}
    const insertion = data.map((item) => {
      Object.assign(merged, item)
      return model.format(executeUpdate(model.create(), item, ref))
    })
    const initFields = Object.keys(model.fields).filter(key => !model.fields[key]?.deprecated)
    const dataFields = [...new Set(Object.keys(merged).map((key) => {
      return initFields.find(field => field === key || key.startsWith(field + '.'))!
    }))]
    let updateFields = difference(dataFields, keys)
    if (!updateFields.length) updateFields = [dataFields[0]]

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
    const formatValues = (table: string, data: object, keys: readonly string[]) => {
      return keys.map((key) => {
        const field = this.database.tables[table]?.fields[key]
        return builder.escape(data[key], field)
      }).join(', ')
    }

    const update = updateFields.map((field) => {
      const escaped = builder.escapeId(field)
      const branches: Dict<any[]> = {}
      data.forEach((item) => {
        (branches[builder.toUpdateExpr(item, field, model.fields[field], true)] ??= []).push(item)
      })

      const entries = Object.entries(branches)
        .map(([expr, items]) => [createMultiFilter(items), expr])
        .sort(([a], [b]) => a.length - b.length)
        .reverse()

      let value = 'CASE '
      for (let index = 0; index < entries.length; index++) {
        value += `WHEN (${entries[index][0]}) THEN (${entries[index][1]}) `
      }
      value += 'END'
      return `${escaped} = ${value}`
    }).join(', ')

    const result = await this.query(`
      INSERT INTO ${builder.escapeId(table)} (${initFields.map(builder.escapeId).join(', ')})
      VALUES (${insertion.map(item => formatValues(table, item, initFields)).join('), (')})
      ON CONFLICT (${keys.map(builder.escapeId).join(', ')})
      DO UPDATE SET ${update}, _pg_mtime = ${mtime}
      RETURNING _pg_mtime as rtime
    `)
    return { inserted: result.filter(({ rtime }) => +rtime !== mtime).length, matched: result.filter(({ rtime }) => +rtime === mtime).length }
  }

  async withTransaction(callback: (session: Driver) => Promise<void>) {
    return await this.sql.begin(async (conn) => {
      const driver = new Proxy(this, {
        get(target, p, receiver) {
          if (p === 'session') return conn
          else return Reflect.get(target, p, receiver)
        },
      })

      await callback(driver)
      await conn.unsafe(`COMMIT`)
    })
  }
}

export default PostgresDriver
