import { Database } from 'minato'
import MemoryDriver from '@minatojs/driver-memory'
import test from '@minatojs/tests'

describe('@minatojs/driver-memory', () => {
  const database = new Database()

  before(async () => {
    await database.connect(MemoryDriver, {})
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
  })

  test(database, {
    migration: false,
    update: {
      index: false,
    },
    model: {
      fields: {
        cast: false,
        typeModel: false,
      },
      object: {
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
