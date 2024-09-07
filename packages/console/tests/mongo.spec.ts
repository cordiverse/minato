import MongoDriver from '@minatojs/driver-mongo'
import Logger from 'reggol'
import test from '@minatojs/tests'
import { setup, prepare } from './utils'

const logger = new Logger('mongo')

describe('@minatojs/console/mongo', () => {
  const [database, databaseC] = setup()

  before(async () => {
    logger.level = 3
    await database.connect(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: true,
    })

    await prepare(database, databaseC)
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(databaseC, {
    model: {
      object: {
        aggregateNull: false,
      }
    },
    transaction: {
      abort: false
    },
    migration: false,
  })
})
