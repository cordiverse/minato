import { $, Database } from '@minatojs/core'
import { expect } from 'chai'
import { setup } from './utils'

interface Foo {
  id: number
  value: number
  deprecated: number
}

interface Bar {
  id: number
  uid: number
  pid: number
  value: number
}

interface Tables {
  foo: Foo
  bar: Bar
}

function SelectionTests(database: Database<Tables>) {
  database.extend('foo', {
    id: 'unsigned',
    value: 'integer',
  })

  database.migrate('foo', { deprecated: 'unsigned' }, async () => { })

  database.extend('bar', {
    id: 'unsigned',
    uid: 'unsigned',
    pid: 'unsigned',
    value: 'integer',
  }, {
    autoInc: true,
  })

  before(async () => {
    await setup(database, 'foo', [
      { id: 1, value: 0 },
      { id: 2, value: 2 },
      { id: 3, value: 2 },
    ])

    await setup(database, 'bar', [
      { uid: 1, pid: 1, value: 0 },
      { uid: 1, pid: 1, value: 1 },
      { uid: 1, pid: 2, value: 0 },
      { uid: 1, pid: 3, value: 1 },
      { uid: 2, pid: 1, value: 1 },
      { uid: 2, pid: 1, value: 1 },
    ])
  })
}

namespace SelectionTests {
  export function sort(database: Database<Tables>) {
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

    it('random', async () => {
      await expect(database.select('foo').orderBy(row => $.random()).execute(['id'])).to.eventually.have.deep.members([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ])
    })
  }

  export function project(database: Database<Tables>) {
    it('shorthand', async () => {
      await expect(database.get('foo', {}, ['id'])).to.eventually.deep.equal([
        { id: 1 },
        { id: 2 },
        { id: 3 },
      ])

      await expect(database.select('foo', row => $.eq(row.id, 1)).orderBy('id').execute(['id'])).to.eventually.deep.equal([
        { id: 1 },
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

    it('aggregate', async () => {
      await expect(database.select('foo')
        .groupBy({}, {
          count: row => $.count(row.id),
          size: row => $.length(row.id),
          max: row => $.max(row.id),
          min: row => $.min(row.id),
          avg: row => $.avg(row.id),
        })
        .execute()
      ).to.eventually.deep.equal([
        { avg: 2, count: 3, max: 3, min: 1, size: 3 },
      ])
    })
  }

  export function aggregate(database: Database<Tables>) {
    it('shorthand', async () => {
      await expect(database.eval('foo', row => $.sum(row.id))).to.eventually.equal(6)
      await expect(database.eval('foo', row => $.count(row.value))).to.eventually.equal(2)
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

  export function group(database: Database<Tables>) {
    it('multiple', async () => {
      await expect(database
        .select('foo')
        .groupBy(['id', 'value'])
        .orderBy('id')
        .execute()
      ).to.eventually.deep.equal([
        { id: 1, value: 0 },
        { id: 2, value: 2 },
        { id: 3, value: 2 },
      ])
    })

    it('callback', async () => {
      await expect(database
        .select('foo')
        .groupBy({
          key: row => $.subtract(row.id, row.value),
        })
        .orderBy('key')
        .execute()
      ).to.eventually.deep.equal([
        { key: 0 },
        { key: 1 },
      ])
    })

    it('extra', async () => {
      await expect(database
        .select('foo')
        .groupBy('value', {
          sum: row => $.sum(row.id),
          count: row => $.count(row.id),
        })
        .orderBy('value')
        .execute()
      ).to.eventually.deep.equal([
        { value: 0, sum: 1, count: 1 },
        { value: 2, sum: 5, count: 2 },
      ])
    })

    it('having', async () => {
      await expect(database
        .select('foo')
        .having(row => $.gt($.sum(row.id), 1))
        .groupBy('value')
        .execute()
      ).to.eventually.deep.equal([
        { value: 2 },
      ])
    })

    it('chaining', async () => {
      await expect(database
        .select('bar')
        .groupBy(['uid', 'pid'], {
          submit: row => $.sum(1),
          accept: row => $.sum(row.value),
        })
        .groupBy(['uid'], {
          submit: row => $.sum(row.submit),
          accept: row => $.sum($.if($.gt(row.accept, 0), 1, 0)),
        })
        .orderBy('uid')
        .execute()
      ).to.eventually.deep.equal([
        { uid: 1, submit: 4, accept: 2 },
        { uid: 2, submit: 2, accept: 1 },
      ])
    })
  }

  export function join(database: Database<Tables>) {
    it('inner join', async () => {
      await expect(database
        .join(['foo', 'bar'] as const)
        .execute()
      ).to.eventually.have.length(18)

      await expect(database
        .join(['foo', 'bar'] as const, (foo, bar) => $.eq(foo.value, bar.value))
        .execute()
      ).to.eventually.have.length(2)
    })

    it('group', async () => {
      await expect(database.join(['foo', 'bar'] as const, (foo, bar) => $.eq(foo.id, bar.pid))
        .groupBy('foo', { count: row => $.sum(row.bar.uid) })
        .orderBy(row => row.foo.id)
        .execute()).to.eventually.deep.equal([
          { foo: { id: 1, value: 0 }, count: 6 },
          { foo: { id: 2, value: 2 }, count: 1 },
          { foo: { id: 3, value: 2 }, count: 1 },
        ])
    })

    it('aggregate', async () => {
      await expect(database
        .join(['foo', 'bar'] as const)
        .execute(row => $.count(row.bar.id))
      ).to.eventually.equal(6)
    })
  }
}

export default SelectionTests
