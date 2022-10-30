import { Database } from 'minato'
import { resolve } from 'path'
import test from '@minatojs/tests'

describe('@minatojs/driver-level', () => {
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
