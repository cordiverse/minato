import { Database } from 'cosmotype'
import test from '@cosmotype/tests'
import MongoDriver from '@cosmotype/driver-mongo'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new MongoDriver(database, {
    host: 'localhost',
    port: 27017,
    database: 'test',
  })

  before(async () => {
    await driver.start()
  })

  after(async () => {
    await driver.drop()
    await driver.stop()
  })

  test(database)
})
