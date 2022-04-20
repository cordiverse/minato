import { Database } from 'cosmotype'
import test from '@cosmotype/tests'
import MySQLDriver from '@cosmotype/driver-mysql'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new MySQLDriver(database, {
    user: 'koishi',
    password: 'koishi@114514',
    database: 'test',
  })

  before(async () => {
    await driver.start()
  })

  after(async () => {
    await driver.drop()
    await driver.stop()
  })

  test(database, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
