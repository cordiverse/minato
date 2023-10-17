import postgres from 'postgres'
import { Dict, difference, makeArray, pick, Time } from 'cosmokit'
import { Database, Driver, Eval, executeUpdate, Field, isEvalExpr, Model, RuntimeError, Selection } from '@minatojs/core'
import { Builder, escapeId } from '@minatojs/sql-utils'
import Logger from 'reggol'

const logger = new Logger('postgres')

export interface ColumnInfo {
  table_catalog:            string;
  table_schema:             string;
  table_name:               string;
  column_name:              string;
  ordinal_position:         number;
  column_default:           any;
  is_nullable:              string;
  data_type:                string;
  character_maximum_length: number;
  is_identity:              string;
  is_updatable:             string;
}


class PostgresBuilder extends Builder {

}

export namespace PostgresDriver {
  export interface Config<T extends Record<string, postgres.PostgresType> = {}> extends postgres.Options<T> {
    host: string
    port: number
    database: string
    username: string
    password: string
  }
}

export class PostgresDriver extends Driver {
  public sql: PostgresBuilder
  public pgsql!: postgres.Sql

  constructor (database: Database, public config: PostgresDriver.Config) {
    super(database)

    this.sql = new PostgresBuilder()
  }

  async start(): Promise<void> {
    this.pgsql = postgres(this.config)
  }

  async stop(): Promise<void> {
    await this.pgsql.end()
  }

  async prepare(name: string): Promise<void> {
    const columns: ColumnInfo[] = await this.pgsql
    `SELECT *
    FROM information_schema.columns
    WHERE TABLE_SCHEMA = ${this.config.database}
    AND TABLE_NAME = ${name}`

    const table = this.model('name')
    const { fields } = table
    const operations: postgres.PendingQuery<any>[] = []

    for (const key in fields) {
      const field = fields[key] as Field<any>
      if (field.deprecated) continue
      const names = [key].concat(field.legacy ?? [])
      const column = columns.find(c => names.includes(c.column_name))
      let shouldUpdate = column?.column_name !== key


    }
  }

}
