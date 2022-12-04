import { Database } from '@minatojs/core'
import { expect } from 'chai'

interface Qux {
  id: number
  text: string
  number: number
  value: number
  flag: boolean
}

interface Tables {
  'mig:qux': Qux
}

function MigrationTests(database: Database<Tables>) {
  it('should migrate', async () => {
    database.extend('mig:qux', {
      id: 'unsigned',
      text: 'string(64)',
      number: 'unsigned',
      flag: 'boolean',
    }, {
      unique: ['number'],
    })

    await database.upsert('mig:qux', [
      { id: 1, text: 'foo', number: 100 },
    ])

    Reflect.deleteProperty(database.tables, 'mig:qux')

    database.extend('mig:qux', {
      id: 'unsigned',
      value: { type: 'unsigned', legacy: ['number'] },
      text: { type: 'string', length: 256, legacy: ['string'] },
    }, {
      unique: ['value'],
    })

    await expect(database.get('mig:qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo', value: 100 },
    ])
  })
}

export default MigrationTests
