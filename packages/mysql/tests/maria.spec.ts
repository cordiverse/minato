import { Database } from 'minato'
import Logger from 'reggol'
import test from '@minatojs/tests'

const logger = new Logger('maria')

describe('@minatojs/driver-mysql/maria', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
    await database.connect('mysql', {
      port: 3307,
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
