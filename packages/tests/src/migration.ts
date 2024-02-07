import { Database, Flatten, Indexable, Keys } from 'minato'
import { expect } from 'chai'

interface Qux {
  id: number
  text: string
  number: number
  value: number
  flag: boolean
}

interface Qux2 {
  id: number
  flag: boolean
}

interface Tables {
  qux: Qux
  qux2: Qux2
}

function MigrationTests(database: Database<Tables>) {
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
}

export default MigrationTests
