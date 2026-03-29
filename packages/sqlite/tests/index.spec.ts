import { Context } from 'cordis'
import Database from 'minato'
import SQLiteDriver from '@minatojs/driver-sqlite'
import Logger from '@cordisjs/plugin-logger'
import test from '@minatojs/tests'

describe('@minatojs/driver-sqlite', () => {
  const ctx = new Context()

  before(async () => {
    await ctx.plugin(Logger)
    await ctx.plugin(Database)
    await ctx.plugin(SQLiteDriver, {
      path: new URL('test.db', import.meta.url).href,
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
