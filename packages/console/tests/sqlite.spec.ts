import { join } from 'path'
import SQLiteDriver from '@minatojs/driver-sqlite'
import Logger from 'reggol'
import test from '@minatojs/tests'
import { setup, prepare } from './utils'

const logger = new Logger('sqlite')

describe('@minatojs/console/sqlite', () => {
  const [database, databaseC] = setup()

  before(async () => {
    logger.level = 3
    await database.connect(SQLiteDriver, {
      path: join(__dirname, 'test.db'),
    })

    await prepare(database, databaseC)
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(databaseC, {
    query: {
      list: {
        elementQuery: false,
      },
    },
    migration: false,
  })
})
