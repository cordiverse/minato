import { Database } from 'cosmotype'
import test from '@cosmotype/tests'

describe('Memory Database', () => {
  const database = new Database()

  before(async () => {
    await database.connect('mysql', {
      user: 'koishi',
      password: 'koishi@114514',
      database: 'test',
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
