import { Database } from 'cosmotype'
import test from '@cosmotype/test-utils'
import MemoryDriver from '@cosmotype/driver-memory'

describe('Memory Database', () => {
  const database = new Database()
  const driver = new MemoryDriver(database)

  before(() => driver.start())
  after(() => driver.stop())

  test(database)
})
