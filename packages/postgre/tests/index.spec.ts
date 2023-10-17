import { Database } from 'minato'
import Logger from 'reggol'
import test from '@minatojs/tests'

const logger = new Logger('postgres')

describe('@minatojs/driver-postgres', () => {
  const database = new Database()

  before(async () => {
    logger.level = 3
    await database.connect('postgres', {
      host: 'localhost',
      port: 5432,
      username: 'koishi',
      password: 'koishi@114514',
      database: 'test',
    })
  })

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  test(database, {
    query: {
      list: {
        elementQuery: false,
      },
    },
  })
})
