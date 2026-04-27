import { Context } from 'cordis'
import Database from '@cordisjs/plugin-database'
import MongoDriver from '@cordisjs/plugin-database-mongo'
import Logger from '@cordisjs/plugin-logger'
import test from '@cordisjs/database-tests'

describe('@cordisjs/plugin-database-mongo', () => {
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
