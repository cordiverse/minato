import { join } from 'path'
import { Database } from 'minato'
import Logger from 'reggol'
import test from '@minatojs/tests'

const logger = new Logger('sqlite')

describe('@minatojs/driver-sqlite', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
    await database.connect('sqlite', {
      path: join(__dirname, 'test.db'),
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(database, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
