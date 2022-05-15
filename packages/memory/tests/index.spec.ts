import { Database } from 'minato'
import test from '@minatojs/tests'

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
