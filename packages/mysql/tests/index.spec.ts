import { Context } from 'cordis'
import Database from 'minato'
import MySQLDriver from '@minatojs/driver-mysql'
import Logger from '@cordisjs/plugin-logger'
import test from '@minatojs/tests'

describe('@minatojs/driver-mysql', () => {
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
