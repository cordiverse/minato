import { Database } from 'minato'
import Logger from 'reggol'
import { expect } from 'chai'
import { } from 'chai-shape'
import { MongoDriver } from '@minatojs/driver-mongo'

const logger = new Logger('mongo')

interface Foo {
  id?: number
  text?: string
  value?: number
  bool?: boolean
  list?: number[]
  timestamp?: Date
  date?: Date
  time?: Date
  regex?: string
}

interface Tables {
  temp1: Foo
}

describe('@minatojs/driver-mongo/migrate-virtualKey', () => {
  const database: Database<Tables> = new Database()

  const initialize = async (optimizeIndex: boolean) => {
    logger.level = 3
    await database.connect('mongo', {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: optimizeIndex,
    })
  }

  const finalize = async () => {
    await database.stopAll()
    logger.level = 2
  }

  after(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  it('reset optimizeIndex', async () => {
    await initialize(false)

    database.extend('temp1', {
      id: 'unsigned',
      text: 'string',
      value: 'integer',
      bool: 'boolean',
      list: 'list',
      timestamp: 'timestamp',
      date: 'date',
      time: 'time',
      regex: 'string',
    }, {
      autoInc: true,
    })

    const table: Foo[] = []
    table.push(await database.create('temp1', {
      text: 'awesome foo',
      timestamp: new Date('2000-01-01'),
      date: new Date('2020-01-01'),
      time: new Date('2020-01-01 12:00:00'),
    }))
    table.push(await database.create('temp1', { text: 'awesome bar' }))
    table.push(await database.create('temp1', { text: 'awesome baz' }))
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await finalize()
    await initialize(true)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await finalize()
    await initialize(false)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await finalize()
    await initialize(true)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await finalize()
    await initialize(false)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)
  })
})
