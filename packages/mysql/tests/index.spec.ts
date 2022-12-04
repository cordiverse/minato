import { Database } from 'minato'
import Logger from 'reggol'
import test from '@minatojs/tests'

const logger = new Logger('mysql')

describe('@minatojs/driver-mysql', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
    await database.connect('mysql', {
      user: 'koishi',
      password: 'koishi@114514',
      database: 'test',
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
