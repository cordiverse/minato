import postgres from 'postgres'
import { Dict, isNullable, makeArray, Time } from 'cosmokit'
import { Database, Driver, Eval, Field, isEvalExpr, Model, Modifier, Selection } from '@minatojs/core'
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
    def += 'JSON'
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

      $sum: (expr) => `coalesce(sum(${this.parseAggr(expr)})::integer, 0)`,
      $avg: (expr) => `avg(${this.parseAggr(expr)})::double precision`,
      $count: (expr) => `count(distinct ${this.parseAggr(expr)})::integer`,

      $concat: (args) => `${args.map(arg => this.parseEval(arg)).join('||')}`,
    }
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} ~ ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createMemberQuery(key: string, value: any[], notStr = '') {
    if (!value.length) return notStr ? 'TRUE' : 'FALSE'
    return `${key}${notStr} in (${value.map(val => this.escape(val)).join(', ')})`
  }

  protected logicalAnd(conditions: string[]) {
    if (!conditions.length) return 'TRUE'
    if (conditions.includes('FALSE')) return 'FALSE'
    return conditions.join(' AND ')
  }

  protected logicalOr(conditions: string[]) {
    if (!conditions.length) return 'FALSE'
    if (conditions.includes('TRUE')) return 'TRUE'
    return `(${conditions.join(' OR ')})`
  }

  suffix(modifier: Modifier) {
    const { limit, offset, sort, group, having } = modifier
    let sql = ''
    if (group?.length) {
      sql += ` GROUP BY ${group.map(this.escapeId).join(', ')}`
      const filter = this.parseEval(having)
      if (filter !== 'TRUE') sql += ` HAVING ${filter}`
    }
    if (sort.length) {
      sql += ' ORDER BY ' + sort.map(([expr, dir]) => {
        return `${this.parseEval(expr)} ${dir.toUpperCase()}`
      }).join(', ')
    }
    if (limit < Infinity) sql += ' LIMIT ' + limit
    if (offset > 0) sql += ' OFFSET ' + offset
    return sql
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
      if (table === '_') {
        return ''
      } else if (this.config?.upsert) {
        return `${this.escapeId(table)}.`
      } else if (key in fields || (Object.keys(this.tables!).length === 1 && table in this.tables!)) {
        return ''
      } else {
        return `${this.escapeId(table)}.`
      }
    })()

    return this.transformKey(key, fields, prefix)
  }

  get(sel: Selection.Immutable, inline = false) {
    const { args, table, query, ref, model } = sel
    const filter = this.parseQuery(query)
    if (filter === 'FALSE') return

    const fields = args[0].fields ?? Object.fromEntries(Object
      .entries(model.fields)
      .filter(([, field]) => !field!.deprecated)
      .map(([key]) => [key, { $: [ref, key] }]))
    const keys = Object.entries(fields).map(([key, value]) => {
      key = this.escapeId(key)
      value = this.parseEval(value)
      return key === value ? key : `${value} AS ${key}`
    }).join(', ')
    let prefix: string | undefined
    if (typeof table === 'string') {
      prefix = this.escapeId(table)
    } else if (table instanceof Selection) {
      prefix = this.get(table, true)
      if (!prefix) return
    } else {
      prefix = Object.entries(table).map(([key, table]) => {
        if (typeof table !== 'string') {
          return `${this.get(table, true)} AS ${this.escapeId(key)}`
        } else {
          return key === table ? this.escapeId(table) : `${this.escapeId(table)} AS ${this.escapeId(key)}`
        }
      }).join(' JOIN ')
      const filter = this.parseEval(args[0].having)
      if (filter !== 'TRUE') prefix += ` ON ${filter}`
    }

    let suffix = this.suffix(args[0])
    if (filter !== 'TRUE') {
      suffix = ` WHERE ${filter}` + suffix
    }
    if (!prefix.includes(' ') || prefix.startsWith('(')) {
      suffix = ` ${ref}` + suffix
    }

    if (inline && !args[0].fields && !suffix) return prefix
    const result = `SELECT ${keys} FROM ${prefix}${suffix}`
    return inline ? `(${result})` : result
  }

  escapeId(value: string) {
    return '"' + value.replace(/"/g, '""') + '"'
  }

  escape(value: any, field?: Field<any>) {
    if (value instanceof Date) {
      value = formatTime(value)
    } else if (!field && !!value && typeof value === 'object') {
      return `json_extract_path(${this.quote(JSON.stringify(value))}, '$')`
    }
    return super.escape(value, field)
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

  constructor(database: Database, config: PostgresDriver.Config) {
    super(database)

    this.config = {
      onnotice: () => { },
      ...config,
    }
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

    if (columns?.length) {
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

  async upsert(sel: Selection.Mutable, data: Dict<any>[], keys: string[]): Promise<Driver.WriteResult> {
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

    await Promise.all(sqls.map(sql => {
      return this.sql`
      INSERT INTO ${this.sql(sel.table)} ${this.sql(sql.values)}
      ON CONFLICT (${this.sql(keys)})
      DO UPDATE SET ${sql.expr}`
    }))

    return {

    }
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
    const query = builder.parseEval(expr)
    const sub = builder.get(sel.table as Selection, true)
    const [data] = await this.sql.unsafe(`SELECT ${query} AS value FROM ${sub} "${sel.ref}"`)
    return data?.value
  }

  async remove(sel: Selection.Mutable) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseQuery(sel.query)
    if (query === 'FALSE') return
    await this.sql.unsafe(`DELETE FROM ${sel.table} WHERE ${query}`)
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
    const builder = new PostgresBuilder(sel.tables)
    data = builder.dump(sel.model, data)
    const [row] = await this.sql`
      INSERT INTO ${this.sql(sel.table)} ${this.sql(data)}
      RETURNING *`
    return row
  }

  async set(sel: Selection.Mutable, data: any) {
    const builder = new PostgresBuilder(sel.tables)
    const comma = this.sql.unsafe(',')
    const query = builder.parseQuery(sel.query)
    if (query === 'FALSE') return

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

    await this.sql`
      UPDATE ${this.sql(sel.table)} ${this.sql(sel.ref)}
      SET ${expr}
      WHERE ${this.sql.unsafe(query)}`
  }
}

export default PostgresDriver
