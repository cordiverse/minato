import { Context } from 'cordis'
import Database from '@cordisjs/plugin-database'
import PostgresDriver from '@cordisjs/plugin-database-postgres'
import Logger from '@cordisjs/plugin-logger'
import test from '@cordisjs/database-tests'

describe('@cordisjs/plugin-database-postgres', () => {
  const ctx = new Context()

  before(async () => {
    await ctx.plugin(Logger)
    await ctx.plugin(Database)
    await ctx.plugin(PostgresDriver, {
      host: 'localhost',
      port: 5432,
      user: 'koishi',
      password: 'koishi@114514',
      database: 'test',
    })
  })

  after(async () => {
    await ctx.database.dropAll()
    await ctx.database.stopAll()
  })

  test(ctx, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
