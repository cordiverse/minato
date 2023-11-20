import { $, Database } from '@minatojs/core'
import { expect } from 'chai'

interface Bar {
  id: number
  text?: string
  num?: number
  bool?: boolean
  list?: string[]
  timestamp?: Date
  date?: Date
  time?: Date
}

interface Tables {
  temptx: Bar
}

function TransactionOperations(database: Database<Tables>) {
  database.extend('temptx', {
    id: 'unsigned',
    text: 'string',
    num: 'integer',
    bool: 'boolean',
    list: 'list',
    timestamp: 'timestamp',
    date: 'date',
    time: 'time',
  }, {
    autoInc: true,
  })
}

namespace TransactionOperations {
  const merge = <T>(a: T, b: Partial<T>): T => ({ ...a, ...b })

  const magicBorn = new Date('1970/08/17')

  const barTable: Bar[] = [
    { id: 1, bool: true },
    { id: 2, text: 'pku' },
    { id: 3, num: 1989 },
    { id: 4, list: ['1', '1', '4'] },
    { id: 5, timestamp: magicBorn },
    { id: 6, date: magicBorn },
    { id: 7, time: new Date('1970-01-01 12:00:00') },
  ]

  async function setup<K extends keyof Tables>(database: Database<Tables>, name: K, table: Tables[K][]) {
    await database.remove(name, {})
    const result: Tables[K][] = []
    for (const item of table) {
      result.push(await database.create(name, item as any))
    }
    return result
  }

  export function commit(database: Database<Tables>) {
    it('create', async () => {
      const table = barTable.map(bar => merge(database.tables.temptx.create(), bar))
      let counter = 0
      await expect(database.withTransaction(async (database) => {
        for (const index in barTable) {
          const bar = await database.create('temptx', barTable[index])
          barTable[index].id = bar.id
          expect(bar).to.have.shape(table[index])
          counter++
        }
        await expect(database.get('temptx', {})).to.eventually.have.length(barTable.length)
      })).to.be.fulfilled
      expect(counter).to.equal(barTable.length)
      await expect(database.get('temptx', {})).to.eventually.have.length(barTable.length)
    })

    it('set', async () => {
      const table = await setup(database, 'temptx', barTable)
      const data = table.find(bar => bar.timestamp)!
      data.list = ['2', '3', '3']
      const magicIds = table.slice(2, 4).map((data) => {
        data.list = ['2', '3', '3']
        return data.id
      })
      await expect(database.withTransaction(async (database) => {
        await expect(database.set('temptx', {
          $or: [
            { id: magicIds },
            { timestamp: magicBorn },
          ],
        }, { list: ['2', '3', '3'] })).to.eventually.have.shape({ matched: 3 })
        await expect(database.get('temptx', {})).to.eventually.have.shape(table)
      })).to.be.fulfilled
      await expect(database.get('temptx', {})).to.eventually.have.shape(table)
    })

    it('upsert new records', async () => {
      await database.remove('temptx', {})
      await expect(database.withTransaction(async (database) => {
        const table = await setup(database, 'temptx', barTable)
        const data = [
          { id: table[table.length - 1].id + 1, text: 'wm"lake' },
          { id: table[table.length - 1].id + 2, text: 'by\'tower' },
        ]
        table.push(...data.map(bar => merge(database.tables.temptx.create(), bar)))
        await expect(database.upsert('temptx', data)).to.eventually.have.shape({ inserted: 2, matched: 0 })
      })).to.be.fulfilled
      await expect(database.get('temptx', {})).to.eventually.have.length(9)
    })

    it('upsert using expressions', async () => {
      const table = await setup(database, 'temptx', barTable)
      const data2 = table.find(item => item.id === 2)!
      const data3 = table.find(item => item.id === 3)!
      const data9 = table.find(item => item.id === 9)
      data2.num = data2.id * 2
      data3.num = data3.num! + 3
      expect(data9).to.be.undefined
      table.push({ id: 9, num: 999 })
      await expect(database.withTransaction(async (database) => {
        await expect(database.upsert('temptx', row => [
          { id: 2, num: $.multiply(2, row.id) },
          { id: 3, num: $.add(3, row.num) },
          { id: 9, num: 999 },
        ])).to.eventually.have.shape({ inserted: 1, matched: 2 })
        await expect(database.get('temptx', {})).to.eventually.have.shape(table)
      })).to.be.fulfilled
      await expect(database.get('temptx', {})).to.eventually.have.shape(table)
    })

    it('remove', async () => {
      await setup(database, 'temptx', barTable)
      await expect(database.withTransaction(async (database) => {
        await expect(database.remove('temptx', { id: 2 })).to.eventually.deep.equal({ removed: 1 })
        await expect(database.get('temptx', {})).eventually.length(6)
        await expect(database.remove('temptx', { id: 2 })).to.eventually.deep.equal({ removed: 0 })
        await expect(database.get('temptx', {})).eventually.length(6)
        await expect(database.remove('temptx', {})).to.eventually.deep.equal({ removed: 6 })
        await expect(database.get('temptx', {})).eventually.length(0)
      })).to.be.fulfilled
      await expect(database.get('temptx', {})).eventually.length(0)
    })
  }

  export function abort(database: Database<Tables>) {
    it('create', async () => {
      const table = barTable.map(bar => merge(database.tables.temptx.create(), bar))
      let counter = 0
      await expect(database.withTransaction(async (database) => {
        for (const index in barTable) {
          const bar = await database.create('temptx', barTable[index])
          barTable[index].id = bar.id
          expect(bar).to.have.shape(table[index])
          counter++
        }
        await expect(database.get('temptx', {})).to.eventually.have.length(barTable.length)
        throw new Error('oops')
      })).to.be.rejected
      expect(counter).to.equal(barTable.length)
      await expect(database.get('temptx', {})).to.eventually.have.length(0)
    })

    it('set', async () => {
      const table = await setup(database, 'temptx', barTable)
      const data = table.find(bar => bar.timestamp)!
      data.list = ['2', '3', '3']
      const magicIds = table.slice(2, 4).map((data) => {
        data.list = ['2', '3', '3']
        return data.id
      })
      await expect(database.withTransaction(async (database) => {
        await expect(database.set('temptx', {
          $or: [
            { id: magicIds },
            { timestamp: magicBorn },
          ],
        }, { list: ['2', '3', '3'] })).to.eventually.have.shape({ matched: 3 })
        await expect(database.get('temptx', {})).to.eventually.have.shape(table)
        throw new Error('oops')
      })).to.be.rejected
      await expect(database.get('temptx', {})).to.eventually.have.shape(barTable)
    })

    it('upsert new records', async () => {
      await database.remove('temptx', {})
      await expect(database.withTransaction(async (database) => {
        const table = await setup(database, 'temptx', barTable)
        const data = [
          { id: table[table.length - 1].id + 1, text: 'wm"lake' },
          { id: table[table.length - 1].id + 2, text: 'by\'tower' },
        ]
        table.push(...data.map(bar => merge(database.tables.temptx.create(), bar)))
        await expect(database.upsert('temptx', data)).to.eventually.have.shape({ inserted: 2, matched: 0 })
        throw new Error('oops')
      })).to.be.rejected
      await expect(database.get('temptx', {})).to.eventually.have.length(0)
    })

    it('upsert using expressions', async () => {
      const table = await setup(database, 'temptx', barTable)
      const data2 = table.find(item => item.id === 2)!
      const data3 = table.find(item => item.id === 3)!
      const data9 = table.find(item => item.id === 9)
      data2.num = data2.id * 2
      data3.num = data3.num! + 3
      expect(data9).to.be.undefined
      table.push({ id: 9, num: 999 })
      await expect(database.withTransaction(async (database) => {
        await expect(database.upsert('temptx', row => [
          { id: 2, num: $.multiply(2, row.id) },
          { id: 3, num: $.add(3, row.num) },
          { id: 9, num: 999 },
        ])).to.eventually.have.shape({ inserted: 1, matched: 2 })
        await expect(database.get('temptx', {})).to.eventually.have.shape(table)
        throw new Error('oops')
      })).to.be.rejected
      await expect(database.get('temptx', {})).to.eventually.have.shape(barTable)
    })

    it('remove', async () => {
      await setup(database, 'temptx', barTable)
      await expect(database.withTransaction(async (database) => {
        await expect(database.remove('temptx', { id: 2 })).to.eventually.deep.equal({ removed: 1 })
        await expect(database.get('temptx', {})).eventually.length(6)
        await expect(database.remove('temptx', { id: 2 })).to.eventually.deep.equal({ removed: 0 })
        await expect(database.get('temptx', {})).eventually.length(6)
        await expect(database.remove('temptx', {})).to.eventually.deep.equal({ removed: 6 })
        await expect(database.get('temptx', {})).eventually.length(0)
        throw new Error('oops')
      })).to.be.rejected
      await expect(database.get('temptx', {})).to.eventually.have.shape(barTable)
    })
  }
}

export default TransactionOperations
