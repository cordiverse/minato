import { $, Database } from '@minatojs/core'
import { expect } from 'chai'
import { setup } from './utils'

interface Table1 {
  id: number
  value: number
}

interface Tables {
  table1: Table1
}

function SortModifiers(database: Database<Tables>) {
  database.extend('table1', {
    id: 'unsigned',
    value: 'integer',
  }, {
    autoInc: true,
  })

  before(async () => {
    await setup(database, 'table1', [
      { id: 1, value: 0 },
      { id: 2, value: 2 },
      { id: 3, value: 2 },
    ])
  })

  it('shorthand', async () => {
    await expect(database.get('table1', {}, {
      sort: { id: 'desc', value: 'asc' }
    })).to.eventually.deep.equal([
      { id: 3, value: 2 },
      { id: 2, value: 2 },
      { id: 1, value: 0 },
    ])

    await expect(database.get('table1', {}, {
      sort: { value: 'asc', id: 'desc' }
    })).to.eventually.deep.equal([
      { id: 1, value: 0 },
      { id: 3, value: 2 },
      { id: 2, value: 2 },
    ])
  })

  it('callback', async () => {
    await expect(database
      .select('table1')
      .orderBy(row => $.subtract(row.id, row.value))
      .execute()
    ).to.eventually.deep.equal([
      { id: 2, value: 2 },
      { id: 1, value: 0 },
      { id: 3, value: 2 },
    ])
  })

  it('limit', async () => {
    await expect(database
      .select('table1')
      .orderBy('id', 'desc')
      .limit(1)
      .offset(2)
      .execute()
    ).to.eventually.deep.equal([
      { id: 1, value: 0 },
    ])
  })
}

export default SortModifiers
