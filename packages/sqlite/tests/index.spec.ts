import { join } from 'path'
import { Database } from 'minato'
import test from '@minatojs/tests'

describe('Memory Database', () => {
  const database = new Database()

  before(async () => {
    await database.connect('sqlite', {
      path: join(__dirname, 'test.db'),
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
  })

  test(database, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
