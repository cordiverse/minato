import { Database } from 'cosmotype'
import test from '@cosmotype/tests'

describe('Memory Database', () => {
  const database = new Database()

  before(async () => {
    await database.connect('memory')
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
  })

  test(database)
})
