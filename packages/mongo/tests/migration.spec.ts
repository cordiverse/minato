import { $, Database, Driver, Primary } from 'minato'
import { Context, Fiber } from 'cordis'
import MongoDriver from '@minatojs/driver-mongo'
import Logger from '@cordisjs/plugin-logger'
import { ObjectId } from 'mongodb'
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

interface Baz {
  id?: number
  text?: string
  value?: number
}

interface Qux {
  id?: Primary
  text?: string
  value?: number
}

interface RawDoc {
  _id: number | ObjectId
  id?: number | ObjectId
  text?: string
  value?: number
}

declare module 'minato' {
  interface Tables {
    mongo1: Foo
    mongo2: Bar
    mongo3: Baz
    mongo4: Qux
  }
}

describe('@minatojs/driver-mongo/migrate-virtualKey', () => {
  const ctx = new Context()

  let database: Database
  let fiber: Fiber | undefined

  const resetConfig = async (optimizeIndex: boolean) => {
    await fiber?.dispose()
    fiber = await ctx.plugin(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex,
    })
    database.refresh()
  }

  before(async () => {
    await ctx.plugin(Database)
    await ctx.plugin(Logger)
    fiber = await ctx.plugin(MongoDriver, {
      host: 'localhost',
      port: 27017,
      database: 'test',
      optimizeIndex: false,
    })
    database = ctx.model as Database
  })

  after(async () => {
    await database.dropAll()
    await fiber?.dispose()
  })

  const getCollection = (table: string) => {
    const driver = database['getDriver'](table) as MongoDriver
    return driver.db.collection<RawDoc>(table)
  }

  it('reset optimizeIndex', async () => {
    database.extend('mongo1', {
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
    table.push(await database.create('mongo1', {
      text: 'awesome foo',
      timestamp: new Date('2000-01-01'),
      date: new Date('2020-01-01'),
      time: new Date('2020-01-01 12:00:00'),
    }))
    table.push(await database.create('mongo1', { text: 'awesome bar' }))
    table.push(await database.create('mongo1', { text: 'awesome baz' }))
    await expect(database.get('mongo1', {})).to.eventually.deep.eq(table)

    await resetConfig(true)
    await expect(database.get('mongo1', {})).to.eventually.deep.eq(table)

    await resetConfig(false)
    await expect(database.get('mongo1', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('mongo1', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('mongo1', {})).to.eventually.deep.eq(table)
  })

  it('using primary', async () => {
    database.extend('mongo2', {
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

    await database.remove('mongo2', {})

    const table: Bar[] = []
    table.push(await database.create('mongo2', {
      text: 'awesome foo',
      timestamp: new Date('2000-01-01'),
      date: new Date('2020-01-01'),
      time: new Date('2020-01-01 12:00:00'),
    }))
    table.push(await database.create('mongo2', { text: 'awesome bar' }))
    table.push(await database.create('mongo2', { text: 'awesome baz' }))
    await expect(database.get('mongo2', {})).to.eventually.deep.eq(table)

    await expect(database.get('mongo2', table[0].id?.toString() as any)).to.eventually.deep.eq([table[0]])
    await expect(database.get('mongo2', { id: table[0].id?.toString() as any })).to.eventually.deep.eq([table[0]])
    await expect(database.get('mongo2', row => $.eq(row.id, $.literal(table[0].id?.toString(), 'primary') as any))).to.eventually.deep.eq([table[0]])

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(true)
    await expect(database.get('mongo2', {})).to.eventually.deep.eq(table)

    await (Object.values(database.drivers)[0] as Driver).drop('_fields')
    await resetConfig(false)
    await expect(database.get('mongo2', {})).to.eventually.deep.eq(table)

    // query & eval
    table.push(await database.create('mongo2', { foreign: table[0].id }))
    await expect(database.get('mongo2', {})).to.eventually.deep.eq(table)
    await expect(database.get('mongo2', { foreign: table[0].id })).to.eventually.deep.eq([table[3]])
    await expect(database.get('mongo2', row => $.eq(row.foreign, table[0].id!))).to.eventually.deep.eq([table[3]])
  })

  it('upsert for ensurePrimary path', async () => {
    await resetConfig(true)
    database.extend('mongo3', {
      id: 'unsigned',
      text: 'string',
      value: 'integer',
    }, {
      autoInc: true,
    })

    await database.remove('mongo3', {})

    const existing = await database.create('mongo3', { text: 'before', value: 1 })
    const insertedId = existing.id! + 1
    await expect(database.upsert('mongo3', [
      { id: existing.id, text: 'after', value: 2 },
      { id: insertedId, text: 'inserted', value: 3 },
    ])).to.eventually.have.shape({ inserted: 1, matched: 1 })

    await expect(database.get('mongo3', {})).to.eventually.have.deep.members([
      { id: existing.id, text: 'after', value: 2 },
      { id: insertedId, text: 'inserted', value: 3 },
    ])

    const docs = await getCollection('mongo3')
      .find({}, { projection: { _id: 1, id: 1, text: 1, value: 1 } })
      .sort({ _id: 1 })
      .toArray()

    expect(docs).to.have.length(2)
    expect(docs.every(doc => !('id' in doc))).to.equal(true)
    expect(docs.find(doc => doc._id === existing.id)).to.have.shape({ _id: existing.id, text: 'after', value: 2 })
    expect(docs.find(doc => doc._id === insertedId)).to.have.shape({ _id: insertedId, text: 'inserted', value: 3 })

    await resetConfig(false)
  })

  it('upsert for pipeline path', async () => {
    database.extend('mongo4', {
      id: 'primary',
      text: 'string',
      value: 'integer',
    })

    await database.remove('mongo4', {})

    const existing = await database.create('mongo4', { text: 'before', value: 1 })
    const insertedId = new ObjectId() as unknown as Primary
    await expect(database.upsert('mongo4', [
      { id: existing.id, text: 'after', value: 2 },
      { id: insertedId, text: 'inserted', value: 3 },
    ])).to.eventually.have.shape({ inserted: 1, matched: 1 })

    const [updated] = await database.get('mongo4', { id: existing.id?.toString() as any })
    expect(updated).to.have.shape({ text: 'after', value: 2 })
    expect(updated.id?.toString()).to.equal(existing.id?.toString())

    const [inserted] = await database.get('mongo4', { id: insertedId.toString() as any })
    expect(inserted).to.have.shape({ text: 'inserted', value: 3 })
    expect(inserted.id?.toString()).to.equal(insertedId.toString())

    const docs = await getCollection('mongo4')
      .find({}, { projection: { _id: 1, id: 1, text: 1, value: 1 } })
      .toArray()

    expect(docs).to.have.length(2)
    expect(docs.every(doc => !('id' in doc))).to.equal(true)
    expect(docs.find(doc => doc._id?.toString() === existing.id?.toString())).to.have.shape({ text: 'after', value: 2 })
    expect(docs.find(doc => doc._id?.toString() === insertedId.toString())).to.have.shape({ text: 'inserted', value: 3 })
  })
})
