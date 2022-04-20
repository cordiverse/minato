import { Database } from 'cosmotype'
import test from '@cosmotype/tests'
import MemoryDriver from '@cosmotype/driver-memory'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new MemoryDriver(database)

  before(async () => {
    await driver.start()
  })

  after(async () => {
    await driver.drop()
    await driver.stop()
  })

  test(database)
})
