import { Database } from 'minato'
import { expect } from 'chai'
import { deepEqual, noop, omit } from 'cosmokit'

interface Qux {
  id: number
  text: string
  number: number
  value: number
  flag: boolean
  obj: object
}

interface Qux2 {
  id: number
  flag: boolean
}

interface Tables {
  qux: Qux
  qux2: Qux2
}

interface MigrationOptions {
  definition?: boolean
}

function MigrationTests(database: Database<Tables>, options: MigrationOptions = {}) {
  const { definition = true } = options

  beforeEach(async () => {
    await database.drop('qux').catch(noop)
  })

  it('alter field', async () => {
    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
      number: 'unsigned',
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo', number: 100 },
      { id: 2, text: 'bar', number: 200 },
    ])

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo', number: 100 },
      { id: 2, text: 'bar', number: 200 },
    ])

    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
    })

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])
  })

  it('should migrate field', async () => {
    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
      number: 'unsigned',
      flag: 'boolean',
    }, {
      unique: ['number'],
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo', number: 100, flag: true },
      { id: 2, text: 'bar', number: 200, flag: false },
    ])

    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      value: { type: 'unsigned', legacy: ['number'] },
      text: { type: 'string', length: 256, legacy: ['string'] },
    }, {
      unique: ['value'],
    })

    database.extend('qux2', {
      id: 'unsigned',
      flag: 'boolean',
    })

    await database.prepared()

    database.migrate('qux', {
      flag: 'boolean',
    }, async (database) => {
      const data = await database.get('qux', {}, ['id', 'flag'])
      await database.upsert('qux2', data)
    })

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo', value: 100 },
      { id: 2, text: 'bar', value: 200 },
    ])

    await expect(database.get('qux2', {})).to.eventually.deep.equal([
      { id: 1, flag: true },
      { id: 2, flag: false },
    ])
  })

  it('set json initial', async () => {
    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    database.extend('qux', {
      obj: {
        type: 'json',
        initial: {},
        nullable: false,
      }
    })

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo', obj: {} },
      { id: 2, text: 'bar', obj: {} },
    ])
  })

  it('indexes', async () => {
    const driver = Object.values(database.drivers)[0]
    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      number: 'unsigned',
    })

    await database.upsert('qux', [
      { id: 1, number: 1 },
      { id: 2, number: 2 },
    ])

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, number: 1 },
      { id: 2, number: 2 },
    ])

    database.extend('qux', {
      id: 'unsigned',
      number: 'unsigned',
    }, {
      indexes: ['number'],
    })

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, number: 1 },
      { id: 2, number: 2 },
    ])

    let indexes = await driver.getIndexes('qux')
    expect(indexes.find(ind => deepEqual(omit(ind, ['name']), {
      unique: false,
      keys: {
        number: 'asc',
      },
    }))).to.not.be.undefined

    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      value: {
        type: 'unsigned',
        legacy: ['number'],
      },
    }, {
      indexes: ['value'],
    })

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, value: 1 },
      { id: 2, value: 2 },
    ])

    indexes = await driver.getIndexes('qux')
    expect(indexes.find(ind => deepEqual(omit(ind, ['name']), {
      unique: false,
      keys: {
        value: 'asc',
      },
    }))).to.not.be.undefined

    database.extend('qux', {}, {
      indexes: [{
        name: 'named-index',
        keys: {
          id: 'asc',
          value: 'asc',
        }
      }],
    })

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, value: 1 },
      { id: 2, value: 2 },
    ])

    indexes = await driver.getIndexes('qux')
    expect(indexes.find(ind => deepEqual(ind, {
      name: 'named-index',
      unique: false,
      keys: {
        id: 'asc',
        value: 'asc',
      },
    }))).to.not.be.undefined

    database.extend('qux', {
      text: 'string',
    }, {
      indexes: [{
        name: 'named-index',
        keys: {
          text: 'asc',
          value: 'asc',
        }
      }],
    })

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, value: 1, text: '' },
      { id: 2, value: 2, text: '' },
    ])

    indexes = await driver.getIndexes('qux')
    expect(indexes.find(ind => deepEqual(ind, {
      name: 'named-index',
      unique: false,
      keys: {
        text: 'asc',
        value: 'asc',
      },
    }))).to.not.be.undefined
  })

  definition && it('immutable model', async () => {
    const driver = Object.values(database.drivers)[0]
    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    await expect(database.get('qux', {})).to.eventually.have.deep.members([
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])

    Reflect.deleteProperty(database.tables, 'qux')
    driver.config.migrateStrategy = 'never'
    database.extend('qux', {
      id: 'unsigned',
      text: 'integer' as any,
    })

    await expect(database.upsert('qux', [
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])).to.eventually.be.rejectedWith('immutable')
    await expect(database.get('qux', {})).to.eventually.be.rejectedWith('immutable')

    Reflect.deleteProperty(database.tables, 'qux')
    Reflect.deleteProperty(database['prepareTasks'], 'qux')
    driver.config.migrateStrategy = 'auto'
    driver.config.readonly = true

    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
    })

    await expect(database.get('qux', {})).to.eventually.be.fulfilled
    await expect(database.set('qux', 1, { text: 'foo' })).to.eventually.be.rejectedWith('read-only')
    await expect(database.upsert('qux', [
      { id: 1, text: 'foo' },
      { id: 2, text: 'bar' },
    ])).to.eventually.be.rejectedWith('read-only')
    await expect(database.remove('qux', 1)).to.eventually.be.rejectedWith('read-only')

    Reflect.deleteProperty(database.tables, 'qux')
    Reflect.deleteProperty(database['prepareTasks'], 'qux')
    driver.config.migrateStrategy = 'auto'
    driver.config.readonly = false
  })
}

export default MigrationTests
