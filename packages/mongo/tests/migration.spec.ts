import { $, Database, Driver, Primary } from 'minato'
import { Context, EffectScope } from 'cordis'
import MongoDriver from '@minatojs/driver-mongo'
import { expect } from '@minatojs/tests'

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

  const database = ctx.minato as Database<Tables>
  let fork: EffectScope<Context> | undefined

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
    fork = ctx.intercept('logger', { level: 3 }).plugin(MongoDriver, {
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
      unique: ['id'],
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
    await expect(database.get('temp1', {})).to.eventually.deep.eq(table)

    await resetConfig(true)
    await expect(database.get('temp1', {})).to.eventually.deep.eq(table)

    await resetConfig(false)
    await expect(database.get('temp1', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('temp1', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('temp1', {})).to.eventually.deep.eq(table)
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

    await database.remove('temp2', {})

    const table: Bar[] = []
    table.push(await database.create('temp2', {
      text: 'awesome foo',
      timestamp: new Date('2000-01-01'),
      date: new Date('2020-01-01'),
      time: new Date('2020-01-01 12:00:00'),
    }))
    table.push(await database.create('temp2', { text: 'awesome bar' }))
    table.push(await database.create('temp2', { text: 'awesome baz' }))
    await expect(database.get('temp2', {})).to.eventually.deep.eq(table)

    await expect(database.get('temp2', table[0].id?.toString() as any)).to.eventually.deep.eq([table[0]])
    await expect(database.get('temp2', { id: table[0].id?.toString() as any })).to.eventually.deep.eq([table[0]])
    await expect(database.get('temp2', row => $.eq(row.id, $.literal(table[0].id?.toString(), 'primary') as any))).to.eventually.deep.eq([table[0]])

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('temp2', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('temp2', {})).to.eventually.deep.eq(table)

    // query & eval
    table.push(await database.create('temp2', { foreign: table[0].id }))
    await expect(database.get('temp2', {})).to.eventually.deep.eq(table)
    await expect(database.get('temp2', { foreign: table[0].id })).to.eventually.deep.eq([table[3]])
    await expect(database.get('temp2', row => $.eq(row.foreign, table[0].id!))).to.eventually.deep.eq([table[3]])
  })
})
