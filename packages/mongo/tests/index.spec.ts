import { Database } from 'minato'
import test from '@minatojs/tests'

describe('@minatojs/driver-mongo', () => {
  const database = new Database()

  before(async () => {
    await database.connect('mongo', {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: true,
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
  })

  test(database)
})
