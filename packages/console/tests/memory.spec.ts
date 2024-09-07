import MemoryDriver from '@minatojs/driver-memory'
import Logger from 'reggol'
import test from '@minatojs/tests'
import { setup, prepare } from './utils'

const logger = new Logger('memory')

describe('@minatojs/console/memory', () => {
  const [database, databaseC] = setup()

  before(async () => {
    logger.level = 3
    await database.connect(MemoryDriver, {})

    await prepare(database, databaseC)
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(databaseC, {
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
