import { Database } from 'minato'
import MongoDriver from '@minatojs/driver-mongo'
import test from '@minatojs/tests'
import Logger from 'reggol'

const logger = new Logger('mongo')

describe('@minatojs/driver-mongo', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
    await database.connect(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: true,
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(database, process.argv.includes('--enable-transaction-abort') ? {} : {
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
