import { Context } from 'cordis'
import Database from '@cordisjs/plugin-database'
import MemoryDriver from '@cordisjs/plugin-database-memory'
import Logger from '@cordisjs/plugin-logger'
import test from '@cordisjs/database-tests'

describe('@cordisjs/plugin-database-memory', () => {
  const ctx = new Context()

  before(async () => {
    await ctx.plugin(Logger)
    await ctx.plugin(Database)
    await ctx.plugin(MemoryDriver)
  })

  after(async () => {
    await ctx.database.dropAll()
    await ctx.database.stopAll()
  })

  test(ctx, {
    migration: false,
    update: {
      index: false,
    },
    json: {
      query: {
        nullableComparator: false,
      },
    },
    model: {
      fields: {
        cast: false,
        typeModel: false,
      },
      object: {
        nullableComparator: false,
        typeModel: false,
      },
    },
    query: {
      comparison: {
        nullableComparator: false,
      },
    },
    relation: {
      select: {
        nullableComparator: false,
      },
      create: {
        nullableComparator: false,
      },
      modify: {
        nullableComparator: false,
      },
    },
  })
})
