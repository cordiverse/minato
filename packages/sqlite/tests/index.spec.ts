import { join } from 'path'
import { Database } from 'cosmotype'
import test from '@cosmotype/tests'
import SQLiteDriver from '@cosmotype/driver-sqlite'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new SQLiteDriver(database, {
    path: join(__dirname, 'test.db'),
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
