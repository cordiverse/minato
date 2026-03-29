import { Context } from 'cordis'
import Database from 'minato'
import MongoDriver from '@minatojs/driver-mongo'
import Logger from '@cordisjs/plugin-logger'
import test from '@minatojs/tests'
// import Logger from 'reggol'

// const logger = new Logger('mongo')

describe('@minatojs/driver-mongo', () => {
  const ctx = new Context()

  before(async () => {
    // logger.level = 3
    await ctx.plugin(Logger)
    await ctx.plugin(Database)
    await ctx.plugin(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: true,
    })
  })

  after(async () => {
    await ctx.database.dropAll()
    await ctx.database.stopAll()
    // logger.level = 2
  })

  test(ctx, {
    model: {
      object: {
        aggregateNull: false,
      }
    },
    transaction: {
      abort: false
    }
  })
})
