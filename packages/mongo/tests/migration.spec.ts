import { Database, Primary } from 'minato'
import Logger from 'reggol'
import { expect } from 'chai'
import { } from 'chai-shape'
import { MongoDriver } from '@minatojs/driver-mongo'

const logger = new Logger('mongo')

interface Foo {
  id?: Primary
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

  const resetConfig = async (optimizeIndex: boolean) => {
    await database.stopAll()
    await database.connect('mongo', {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: optimizeIndex,
    })
  }

  beforeEach(async () => {
    logger.level = 3
    await database.connect('mongo', {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: false,
    })
  })

  afterEach(async () => {
    await database.dropAll()
    await database.stopAll()
    logger.level = 2
  })

  it('reset optimizeIndex', async () => {
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

    await resetConfig(true)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await resetConfig(false)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)
  })

  it('using primary', async () => {
    database.extend('temp1', {
      id: 'primary',
      text: 'string',
      value: 'integer',
      bool: 'boolean',
      list: 'list',
      timestamp: 'timestamp',
      date: 'date',
      time: 'time',
      regex: 'string',
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

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('temp1', {})).to.eventually.have.shape(table)
  })
})
