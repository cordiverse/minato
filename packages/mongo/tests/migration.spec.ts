import { $, Database, Primary } from 'minato'
import { Context, ForkScope, Logger } from 'cordis'
import { expect } from 'chai'
import { } from 'chai-shape'
import MongoDriver from '@minatojs/driver-mongo'

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

interface Bar {
  id?: Primary
  text?: string
  value?: number
  bool?: boolean
  list?: number[]
  timestamp?: Date
  date?: Date
  time?: Date
  regex?: string
  foreign?: Primary
}

interface Tables {
  temp1: Foo
  temp2: Bar
}

describe('@minatojs/driver-mongo/migrate-virtualKey', () => {
  const ctx = new Context()
  ctx.plugin(Database)

  const database = ctx.model as Database<Tables>
  let fork: ForkScope

  const resetConfig = async (optimizeIndex: boolean) => {
    fork?.dispose()
    fork = ctx.plugin(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex,
    })
    await ctx.events.flush()
  }

  before(() => ctx.start())

  beforeEach(async () => {
    logger.level = 3
    fork = ctx.plugin(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: false,
    })
    await ctx.events.flush()
  })

  afterEach(async () => {
    await database.dropAll()
    fork?.dispose()
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
    database.extend('temp2', {
      id: 'primary',
      text: 'string',
      value: 'integer',
      bool: 'boolean',
      list: 'list',
      timestamp: 'timestamp',
      date: 'date',
      time: 'time',
      regex: 'string',
      foreign: 'primary',
    })

    const table: Bar[] = []
    table.push(await database.create('temp2', {
      text: 'awesome foo',
      timestamp: new Date('2000-01-01'),
      date: new Date('2020-01-01'),
      time: new Date('2020-01-01 12:00:00'),
    }))
    table.push(await database.create('temp2', { text: 'awesome bar' }))
    table.push(await database.create('temp2', { text: 'awesome baz' }))
    await expect(database.get('temp2', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('temp2', {})).to.eventually.have.shape(table)

    await (Object.values(database.drivers)[0] as MongoDriver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('temp2', {})).to.eventually.have.shape(table)

    // query & eval
    table.push(await database.create('temp2', { foreign: table[0].id }))
    await expect(database.get('temp2', {})).to.eventually.have.shape(table)
    await expect(database.get('temp2', { foreign: table[0].id })).to.eventually.have.shape([table[3]])
    await expect(database.get('temp2', row => $.eq(row.foreign, table[0].id!))).to.eventually.have.shape([table[3]])
  })
})
