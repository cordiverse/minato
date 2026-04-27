import { Context } from 'cordis'
import Database from '@cordisjs/plugin-database'
import MySQLDriver from '@cordisjs/plugin-database-mysql'
import Logger from '@cordisjs/plugin-logger'
import test from '@cordisjs/database-tests'

describe('@cordisjs/plugin-database-mysql', () => {
  const ctx = new Context()

  before(async () => {
    await ctx.plugin(Logger)
    await ctx.plugin(Database)
    await ctx.plugin(MySQLDriver, {
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
