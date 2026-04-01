import { Context } from 'cordis'
import Database from 'minato'
import MongoDriver from '@minatojs/driver-mongo'
import Logger from '@cordisjs/plugin-logger'
import test from '@minatojs/tests'

describe('@minatojs/driver-mongo', () => {
  const ctx = new Context()

  before(async () => {
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
