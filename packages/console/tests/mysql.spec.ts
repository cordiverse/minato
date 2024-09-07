import MySQLDriver from '@minatojs/driver-mysql'
import Logger from 'reggol'
import test from '@minatojs/tests'
import { setup, prepare } from './utils'

const logger = new Logger('mysql')

describe('@minatojs/console/mysql', () => {
  const [database, databaseC] = setup()

  before(async () => {
    logger.level = 3
    await database.connect(MySQLDriver, {
      user: 'koishi',
      password: 'koishi@114514',
      database: 'test',
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
