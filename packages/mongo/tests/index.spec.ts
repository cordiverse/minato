import { Database } from 'minato'
import test from '@minatojs/tests'
import Logger from 'reggol'

const logger = new Logger('mongo')

describe('@minatojs/driver-mongo', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
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
    logger.level = 2
  })

  test(database)
})
