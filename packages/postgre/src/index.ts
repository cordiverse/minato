import postgres from 'postgres'
import { Dict, difference, makeArray, pick, Time } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, isEvalExpr, Model, Modifier, RuntimeError, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import Logger from 'reggol'

const logger = new Logger('postgres')

interface ColumnInfo {
  table_catalog: string;
  table_schema: string;
  table_name: string;
  column_name: string;
  ordinal_position: number;
  column_default: any;
  is_nullable: string;
  data_type: string;
  character_maximum_length: number;
  is_identity: string;
  is_updatable: string;
}

interface TableInfo {
  table_catalog: string;
  table_schema: string;
  table_name: string;
  table_type: string;
  self_referencing_column_name: null;
  reference_generation: null;
  user_defined_type_catalog: null;
  user_defined_type_schema: null;
  user_defined_type_name: null;
  is_insertable_into: string;
  is_typed: string;
  commit_action: null;
}

interface FieldOperation {
  key: string
  names: string[]
  field?: Field
  column?: ColumnInfo | undefined
  operations?: 'create' | 'rename'
}

function type({ type, length, precision, scale, autoInc, initial }: Field & { autoInc?: boolean }) {
  switch (type) {
    case 'primary':
    case 'unsigned':
    case 'integer': {
      length ??= 4
      const numLen = precision || Math.ceil(Math.log10(Math.pow(2, length * 8 - 1))) // TODO: discuss about length define
      if (autoInc) {
        if (length <= 2) return 'SERIAL'
        if (length <= 8) return 'BIGSERIAL'
        if (length <= 4) return 'SMALLSERIAL'
        throw new Error(`unsupported type: ${type}`)
      }
      let sql: string
      if (scale) sql = `NUMERIC(${numLen}, ${scale})`
      else if (length <= 2) sql = 'SMALLINT'
      else if (length <= 4) sql = 'INTEGER'
      else if (length <= 8) sql = 'BIGINT'
      else sql = `NUMERIC(${numLen})`
      sql += ` DEFAULT ${initial || 0}`
      return sql
    }
    case 'float': return 'REAL'
    case 'double': return 'DOUBLE PRECISION'
    case 'char': return `varchar(${length || 64}) DEFAULT ''`
    case 'string': return `varchar(${length || 255}) DEFAULT ''`
    case 'text': return `varchar(${length || 65535}) DEFAULT ''`
    case 'json': return 'TEXT' // TODO: search about pgsql json schema
    default: throw new Error(`unsupported type: ${type}`)
  }
}

class PostgresBuilder extends Builder {

}

export namespace PostgresDriver {
  export interface Config<T extends Record<string, postgres.PostgresType> = {}> extends postgres.Options<T> {
    host: string
    port: number
    username: string
    password: string
    database: string
    schema: string
  }
}

export class PostgresDriver extends Driver {
  public sql!: postgres.Sql

  constructor(database: Database, public config: PostgresDriver.Config) {
    super(database)
  }

  async start() {
    this.sql = postgres(this.config)
    await this.sql`SET search_path = ${this.config.schema}`
  }

  async stop() {
    await this.sql.end()
  }

  async prepare(name: string) {
    const columns: ColumnInfo[] = await this.sql
      `SELECT *
      FROM information_schema.columns
      WHERE table_schema = ${this.config.schema}
      AND table_name = ${name}`

    const table = this.model('name')
    const { fields } = table
    const operations: postgres.PendingQuery<any>[] = []

    const a: FieldOperation[] = Object.entries(fields).map(([key, field]) => {
      const names = [key].concat(field?.legacy ?? [])
      const column = columns.find(c => names.includes(c.column_name))
      const operation: FieldOperation['operations'] = (() => {
        if (!column) return 'create'
        if (name !== column.column_name) return 'rename'
      })()
      const def = this.sql(`${name}`)
      return { key, field, names, column, operation }
    })

    if (!columns?.length) {
      this.sql`CREATE TABLE ${this.sql(name)} (${1})`
      return
    }
  }

  async upsert(sel: Selection.Mutable, data: any[], keys: string[]): Promise<void> {
    // TODO
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
    if (table) return void await this.sql`DROP TABLE ${table}`
    const tables: TableInfo[] = await this.sql
      `SELECT *
      FROM information_schema.tables
      WHERE table_schema = ${this.config.schema}`
    if (!tables.length) return
    await this.sql`DROP TABLE ${this.sql(tables.map(t => t.table_name))};`
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr<any, boolean>) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseEval(expr)
    const sub = builder.get(sel.table as Selection, true)
    const [data] = await this.sql
      `SELECT ${this.sql(query)} AS value
      FROM ${this.sql(sub)} ${this.sql(sel.ref)}`
    return data?.value
  }

  async remove(sel: Selection.Mutable) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseQuery(sel.query)
    if (query === '0') return
    await this.sql`DELETE FROM ${this.sql(sel.table)} WHERE ${this.sql(query)}`
  }

  async stats(): Promise<Partial<Driver.Stats>> {
    const tables = await this.sql
      `SELECT *
      FROM information_schema.tables
      WHERE table_schema = ${this.config.schema}`
    const tableStats = await this.sql.unsafe(
      tables.map(({ table_name: name }) => {
        return `SELECT '${name}' AS name,
          pg_total_relation_size('${name}') AS size,
          COUNT(*) AS count FROM ${name}`
      }).join(' UNION ')
    ).then(s => s.map(t => [t.name, { size: +t.size, count: +t.count }]))

    return {
      size: tableStats.reduce((p, c) => p += c[1].size, 0),
      tables: Object.fromEntries(tableStats)
    }
  }

  async create(sel: Selection.Mutable, data: any) {
    const [row] = await this.sql
      `INSERT INTO ${this.sql(sel.table)} ${this.sql(data)}
      RETURNING *`
    return row
  }

  async set(sel: Selection.Mutable, data: any) {
    const builder = new PostgresBuilder(sel.tables)
    const query = builder.parseQuery(sel.query)
    if (query === '0') return
    await this.sql
      `UPDATE ${this.sql(sel.table)} ${this.sql(sel.ref)}
      SET ${this.sql(data)}
      WHERE ${this.sql(query)}`
  }
}
