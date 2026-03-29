import { Context } from 'cordis'
import Database from 'minato'
import PostgresDriver from '@minatojs/driver-postgres'
import Logger from '@cordisjs/plugin-logger'
// import Logger from 'reggol'
import test from '@minatojs/tests'

// const logger = new Logger('postgres')

describe('@minatojs/driver-postgres', () => {
  const ctx = new Context()

  before(async () => {
    // logger.level = 3
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
    // logger.level = 2
  })

  test(ctx, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
