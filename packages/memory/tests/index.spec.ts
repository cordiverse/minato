import { Context } from 'cordis'
import Database from 'minato'
import MemoryDriver from '@minatojs/driver-memory'
import Logger from '@cordisjs/plugin-logger'
import test from '@minatojs/tests'

const ctx = new Context()

describe('@minatojs/driver-memory', () => {
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
