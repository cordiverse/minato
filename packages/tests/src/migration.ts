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
  qux: Qux
}

function MigrationTests(database: Database<Tables>) {
  it('should migrate', async () => {
    database.extend('qux', {
      id: 'unsigned',
      text: 'string(64)',
      number: 'unsigned',
      flag: 'boolean',
    }, {
      unique: ['number'],
    })

    await database.upsert('qux', [
      { id: 1, text: 'foo', number: 100 },
    ])

    Reflect.deleteProperty(database.tables, 'qux')

    database.extend('qux', {
      id: 'unsigned',
      value: { type: 'unsigned', legacy: ['number'] },
      text: { type: 'string', length: 256, legacy: ['string'] },
    }, {
      unique: ['value'],
    })

    await expect(database.get('qux', {})).to.eventually.deep.equal([
      { id: 1, text: 'foo', value: 100 },
    ])
  })
}

export default MigrationTests
