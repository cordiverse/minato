import { Database } from 'cosmotype'
import test from '@cosmotype/test-utils'
import LevelDriver from '@cosmotype/driver-level'
import { resolve } from 'path'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new LevelDriver(database, {
    location: resolve(__dirname, 'temp'),
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
