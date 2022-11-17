import { $, Database } from '@minatojs/core'
import { expect } from 'chai'
import { setup } from './utils'

interface Foo {
  id: number
  value: number
}

interface Tables {
  foo: Foo
}

function SelectionTests(database: Database<Tables>) {
  database.extend('foo', {
    id: 'unsigned',
    value: 'integer',
  })
}

namespace SelectionTests {
  export function sort(database: Database<Tables>) {
    before(async () => {
      await database.remove('foo', {})
      await setup(database, 'foo', [
        { id: 1, value: 0 },
        { id: 2, value: 2 },
        { id: 3, value: 2 },
      ])
    })

    it('shorthand', async () => {
      await expect(database.get('foo', {}, {
        sort: { id: 'desc', value: 'asc' }
      })).to.eventually.deep.equal([
        { id: 3, value: 2 },
        { id: 2, value: 2 },
        { id: 1, value: 0 },
      ])

      await expect(database.get('foo', {}, {
        sort: { value: 'asc', id: 'desc' }
      })).to.eventually.deep.equal([
        { id: 1, value: 0 },
        { id: 3, value: 2 },
        { id: 2, value: 2 },
      ])
    })

    it('callback', async () => {
      await expect(database
        .select('foo')
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
        .select('foo')
        .orderBy('id', 'desc')
        .limit(1)
        .offset(2)
        .execute()
      ).to.eventually.deep.equal([
        { id: 1, value: 0 },
      ])
    })
  }

  export function project(database: Database<Tables>) {
    before(async () => {
      await database.remove('foo', {})
      await setup(database, 'foo', [
        { id: 1, value: 0 },
        { id: 2, value: 2 },
        { id: 3, value: 2 },
      ])
    })

    it('shorthand', async () => {
      await expect(database.get('foo', {}, ['id'])).to.eventually.deep.equal([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ])
    })

    it('callback', async () => {
      await expect(database
        .select('foo')
        .project({
          id: row => $.add($.multiply(row.id, row.id), 1),
        })
        .execute()
      ).to.eventually.deep.equal([
        { id: 2 },
        { id: 5 },
        { id: 10 },
      ])
    })

    it('chaining', async () => {
      await expect(database
        .select('foo')
        .project({
          id: row => $.multiply(row.id, row.id),
        })
        .project({
          id: row => $.add(row.id, 1),
        })
        .execute()
      ).to.eventually.deep.equal([
        { id: 2 },
        { id: 5 },
        { id: 10 },
      ])
    })
  }

  export function aggregate(database: Database<Tables>) {
    before(async () => {
      await database.remove('foo', {})
      await setup(database, 'foo', [
        { id: 1, value: 0 },
        { id: 2, value: 2 },
        { id: 3, value: 2 },
      ])
    })

    it('shorthand', async () => {
      await expect(database.eval('foo', { $sum: 'id' })).to.eventually.equal(6)
      await expect(database.eval('foo', { $count: 'value' })).to.eventually.equal(2)
    })

    it('inner expressions', async () => {
      await expect(database
        .select('foo')
        .execute(row => $.avg($.multiply($.subtract(row.id, 1), row.value)))
      ).to.eventually.equal(2)
    })

    it('outer expressions', async () => {
      await expect(database
        .select('foo')
        .execute(row => $.subtract($.sum(row.id), $.count(row.value)))
      ).to.eventually.equal(4)
    })

    it('chaining', async () => {
      await expect(database
        .select('foo')
        .project({
          value: row => $.multiply($.subtract(row.id, 1), row.value),
        })
        .execute(row => $.avg(row.value))
      ).to.eventually.equal(2)
    })
  }
}

export default SelectionTests
