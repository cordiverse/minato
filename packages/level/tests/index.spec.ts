import { Database } from 'cosmotype'
import { resolve } from 'path'
import test from '@cosmotype/tests'

describe('Memory Database', () => {
  const database = new Database()

  before(async () => {
    await database.connect('level', {
      location: resolve(__dirname, 'temp'),
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
  })

  test(database)
})
