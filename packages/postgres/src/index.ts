import postgres from 'postgres'
import { Dict, difference, isNullable, makeArray, pick, Time } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, isEvalExpr, Model, Modifier, Selection } from '@minatojs/core'
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

interface PostgresBuilderConfig {
  upsert?: boolean
}

function type(field: Field & { autoInc?: boolean }) {
  let { type, length, precision, scale, initial, autoInc } = field
  let def = ''
  if (['primary', 'unsigned', 'integer'].includes(type)) {
    length ||= 4
    if (precision) def += `NUMERIC(${precision}, ${scale ?? 0})`
    else if (autoInc) {
      if (length <= 2) def += 'SERIAL'
      else if (length <= 4) def += 'SMALLSERIAL'
      else if (length <= 8) def += 'BIGSERIAL'
      else throw new Error(`unsupported type: ${type}`)
    } else if (length <= 2) def += 'SMALLINT'
    else if (length <= 4) def += 'INTEGER'
    else if (length <= 8) def += 'BIGINT'
    else new Error(`unsupported type: ${type}`)

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
    def += `VARCHAR(${length || 65535})`
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
    def += 'DATE' // TODO: default
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
  protected $true = 'TRUE'
  protected $false = 'FALSE'
  upsert = false
  table = ''

  constructor(public tables?: Dict<Model>, public config?: PostgresBuilderConfig) {
    super(tables)

    this.define<Date, string>({
      types: ['time'],
      dump: date => formatTime(date),
      load: str => {
        if (isNullable(str)) return str
        const date = new Date(0)
        const parsed = timeRegex.exec(str)
        if (!parsed) throw Error(`unexpected time value: ${str}`)
        date.setHours(+parsed[1], +parsed[2], +parsed[3])
        return date
      },
    })

    this.queryOperators = {
      ...this.queryOperators,
      $regex: (key, value) => this.createRegExpQuery(key, value),
      $regexFor: (key, value) => `${this.escape(value)} ~ ${key}`,
      $size: (key, value) => {
        if (!value) return this.logicalNot(key)
        return `${key} IS NOT NULL AND ARRAY_LENGTH(${key}, 1) = ${value}`
      },
      $el: (key, value) => {
        if (Array.isArray(value)) {
          return `${key} && ARRAY['${value.map(v => v.replace(/'/g, "''")).join("','")}']::TEXT[]`
        } else if (typeof value !== 'number' && typeof value !== 'string') {
          throw new TypeError('query expr under $el is not supported')
        } else {
          return `${key} && ARRAY['${value}']::TEXT[]`
        }
      },
    }

    this.evalOperators = {
      ...this.evalOperators,
      $if: (args) => `(SELECT CASE WHEN ${this.parseEval(args[0])} THEN ${this.parseEval(args[1])} ELSE ${this.parseEval(args[2])} END)`,
      $ifNull: (args) => `coalesce(${args.map(arg => this.parseEval(arg)).join(', ')})`,

      $sum: (expr) => this.createAggr(expr, value => `coalesce(sum(${value})::integer, 0)`),
      $avg: (expr) => this.createAggr(expr, value => `avg(${value})::double precision`),
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

      $concat: (args) => `${args.map(arg => this.parseEval(arg)).join('||')}`,
    }

    this.define<string[], any>({
      types: ['list'],
      dump: value => '{' + value.join(',') + '}',
      load: value => value,
    })
  }

  protected parseFieldQuery(key: string, query) {
    if (this.upsert) return super.parseFieldQuery(`${this.escapeId(this.table)}.${key}`, query)
    return super.parseFieldQuery(key, query)
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} ~ ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected transformJsonField(obj: string, path: string) {
    this.state.sqlType = 'json'
    return `jsonb_extract_path(${obj}, ${path.slice(1).replace('.', ',')})`
  }

  protected getRecursive(args: string | string[]) {
    if (typeof args === 'string') {
      return this.getRecursive(['_', args])
    }
    const [table, key] = args
    const fields = this.tables?.[table]?.fields || {}
    if (fields[key]?.expr) {
      return this.parseEvalExpr(fields[key]?.expr)
    }

    const prefix = (() => {
      if (this.upsert && key in fields) {
        return `${this.escapeId(this.tables![table].name)}.`
      } else if (this.upsert) {
        return `${this.escapeId(this.table)}.`
      } else if (table === '_') {
        return ''
      } else if (this.config?.upsert) {
        return `${this.escapeId(table)}.`
      } else if (key in fields || (Object.keys(this.tables!).length === 1 && table in this.tables!)) {
        return ''
      } else {
        return `${this.escapeId(table)}.`
      }
    })()

    return this.transformKey(key, fields, prefix, `${table}.${key}`)
  }

  protected jsonQuote(value: string, pure: boolean = false) {
    if (pure) return `to_jsonb(${value})`
    const res = this.state.sqlType === 'raw' ? `to_jsonb(${value})` : value
    this.state.sqlType = 'json'
    return res
  }

  escapeId(value: string) {
    return '"' + value.replace(/"/g, '""') + '"'
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) {
      value = formatTime(value)
    } else if (!field && !!value && typeof value === 'object') {
      return `${this.quote(JSON.stringify(value))}::jsonb`
    }
    return super.escape(value, field)
  }

  toUpdateExpr(item: any, key: string, field?: Field, upsert?: boolean, table?: string) {
    const escaped = this.escapeId(key)
    this.table = table!
    // update directly
    // console.log(item, key)
    if (key in item) {
      if (!isEvalExpr(item[key]) && upsert) {
        return `excluded.${escaped}`
      } else if (isEvalExpr(item[key])) {
        return this.parseEval(item[key])
      } else {
        // console.log(1, this.escape(item[key], field))
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
    const valueInit = this.upsert ? `coalesce(${this.escapeId(table!)}.${escaped}, '{}')::jsonb` : `coalesce(${escaped}, '{}')::jsonb`
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
      return `${this.escapeId(table!)}.${escaped}`
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
  private builder: PostgresBuilder

  constructor(database: Database, config: PostgresDriver.Config) {
    super(database)

    this.config = {
      onnotice: () => { },
      debug(_, query, parameters) {
        logger.debug('> %s\n parameters: %o', query, parameters)
      },
      ...config,
    }
    this.builder = new PostgresBuilder()
  }

  async start() {
    this.sql = postgres(this.config)
  }

  async stop() {
    await this.sql.end()
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
        def = type(Object.assign({
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
        (${this.sql.unsafe(create.map(f => `"${f.key}" ${f.def}`).join(','))},
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
        `ALTER TABLE "${name}" ${rename.map(f => `ADD "${f.key}" ${f.def}`).join(',')}`,
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

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]) {
    if (!data.length) return {}
    const { model, table, tables, ref } = sel
    const builder = new PostgresBuilder(tables)
    builder.upsert = true

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

    const update = updateFields.map((field) => {
      const escaped = builder.escapeId(field)
      const branches: Dict<any[]> = {}
      data.forEach((item) => {
        (branches[builder.toUpdateExpr(item, field, model.fields[field], true, table)] ??= []).push(item)
      })

      const entries = Object.entries(branches)
        .map(([expr, items]) => [createMultiFilter(items), expr])
        .sort(([a], [b]) => a.length - b.length)
        .reverse()

      let value = 'CASE '
      // let value = entries[0][1]
      for (let index = 0; index < entries.length; index++) {
        // value = `(CASE WHEN (${entries[index][0]}) THEN (${entries[index][1]}) ELSE (${value}) END)`
        value += `WHEN (${entries[index][0]}) THEN (${entries[index][1]}) `
      }
      value += 'END'
      return `${escaped} = ${value}`
    }).join(', ')

    try {
      const result = await this.sql.unsafe(`
      INSERT INTO ${builder.escapeId(table)} (${initFields.map(builder.escapeId).join(', ')})
      VALUES (${insertion.map(item => this._formatValues(table, item, initFields)).join('), (')})
      ON CONFLICT (${keys.map(builder.escapeId).join(', ')})
      DO UPDATE SET ${update}
    `)
    } catch (e) { logger.error(e) }
    return {}
    // const records = +(/^&Records:\s*(\d+)/.exec(result.message)?.[1] ?? result.affectedRows)
    // return { inserted: records - result.changedRows, modified: result.affectedRows - records }
  }

  _formatValues = (table: string, data: object, keys: readonly string[]) => {
    return keys.map((key) => {
      const field = this.database.tables[table]?.fields[key]
      return this.builder.escape(data[key], field)
    }).join(', ')
  }

  async upsert2(sel: Selection.Mutable, data: Dict<any>[], keys: string[]): Promise<Driver.WriteResult> {
    if (!data.length) return {}
    const builder = new PostgresBuilder(sel.tables, { upsert: true })
    const comma = this.sql.unsafe(',')

    const sqls: {
      expr: postgres.PendingQuery<any>[]
      values: Dict<any>
    }[] = []

    for (const row of data) {
      const expr: postgres.PendingQuery<any>[] = []
      const values: Dict<any> = {}
      for (const [key, value] of Object.entries(row)) {
        if (!isEvalExpr(value)) {
          if (isEvalExpr(value)) {
            expr.push(this.sql.unsafe(`"${key}"=${builder.parseEval(value)}`))
            values[key] = builder.escape(value, sel.tables[sel.table]?.fields[key])
          } else {
            expr.push(this.sql`${this.sql(key)}=${value}`)
            values[key] = value
          }
          expr.push(comma)
        }
      }
      expr.splice(-1, 1)
      sqls.push({ expr, values })
    }

    const d = await Promise.all(sqls.map(sql => {
      return this.sql`
      INSERT INTO ${this.sql(sel.table)} ${this.sql(sql.values)}
      ON CONFLICT (${this.sql(keys)})
      DO UPDATE SET ${sql.expr}`
    }))
    console.log(d)
    // postgres's upsert cannot distinguish between the quantities of modify and insert.
    return {}
  }

  async get(sel: Selection.Immutable) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.get(sel)
    if (!query) return []
    return this.sql.unsafe(query).then(data => {
      return data.map(row => builder.load(sel.model, row))
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

  async eval(sel: Selection.Immutable, expr: Eval.Expr<any, boolean>) {
    const builder = new PostgresBuilder(sel.tables)
    const inner = builder.get(sel.table as Selection, true, true)
    const output = builder.parseEval(expr, false)
    const ref = inner.startsWith('(') && inner.endsWith(')') ? sel.ref : ''
    const [data] = await this.sql.unsafe(`SELECT ${output} AS value FROM ${inner} ${ref}`)
    return data?.value
  }

  async remove(sel: Selection.Mutable): Promise<Driver.WriteResult> {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseQuery(sel.query)
    if (query === 'FALSE') return {}
    const { count } = await this.sql.unsafe(`DELETE FROM ${sel.table} WHERE ${query}`)
    return { removed: count }
  }

  async stats(): Promise<Partial<Driver.Stats>> {
    const tables = await this.sql`
      SELECT *
      FROM information_schema.tables
      WHERE table_schema = 'public'`
    const tableStats = await this.sql.unsafe(
      tables.map(({ table_name: name }) => {
        return `SELECT '${name}' AS name,
          pg_total_relation_size('${name}') AS size,
          COUNT(*) AS count FROM ${name}`
      }).join(' UNION '),
    ).then(s => s.map(t => [t.name, { size: +t.size, count: +t.count }]))

    return {
      size: tableStats.reduce((p, c) => p += c[1].size, 0),
      tables: Object.fromEntries(tableStats),
    }
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table, model } = sel
    const builder = new PostgresBuilder(sel.tables)
    const formatted = builder.dump(model, data)
    const keys = Object.keys(formatted)
    const [row] = await this.sql.unsafe(`
      INSERT INTO ${builder.escapeId(table)} (${keys.map(builder.escapeId).join(', ')})
      VALUES (${keys.map(key => builder.escape(formatted[key])).join(', ')})
      RETURNING *`)
    return builder.load(model, row)
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
    const result = await this.sql.unsafe(`UPDATE ${builder.escapeId(table)} ${ref} SET ${update} WHERE ${filter}`)
    return {}
  }

  async set2(sel: Selection.Mutable, data: any): Promise<Driver.WriteResult> {
    const builder = new PostgresBuilder(sel.tables)
    const comma = this.sql.unsafe(',')
    const query = builder.parseQuery(sel.query)
    if (query === 'FALSE') return {}

    const expr: postgres.PendingQuery<any>[] = []
    for (const [key, value] of Object.entries(builder.dump(sel.model, data) as Dict<any>)) {
      if (isEvalExpr(value)) {
        expr.push(this.sql.unsafe(`"${key}"=${builder.parseEval(value)}`))
      } else {
        expr.push(this.sql`${this.sql(key)}=${value}`)
      }
      expr.push(comma)
    }
    expr.splice(-1, 1)

    const { count } = await this.sql`
      UPDATE ${this.sql(sel.table)} ${this.sql(sel.ref)}
      SET ${expr}
      WHERE ${this.sql.unsafe(query)}`

    return { modified: count }
  }
}

export default PostgresDriver
