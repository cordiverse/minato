import { $, Database } from '@minatojs/core'
import { expect } from 'chai'
import { setup } from './utils'

interface Foo {
  id: number
  value: number
  bars: Bar[]
}

interface Bar {
  id: number
  uid: number
  pid: number
  value: number
  obj: {
    x: string
    y: string
  }
  l: string[]
}

interface Tables {
  foo: Foo
  bar: Bar
}

function ExperimentalTests(database: Database<Tables>) {
  database.extend('foo', {
    id: 'unsigned',
    value: 'integer',
  })

  database.extend('bar', {
    id: 'unsigned',
    uid: 'unsigned',
    pid: 'unsigned',
    value: 'integer',
    obj: 'json',
    l: 'list',
  }, {
    autoInc: true,
  })

  before(async () => {

    database.extend('foo', {
      id: 'unsigned',
      value: 'integer',
      // bars: row => database.select('bar').where(r => $.eq(r.pid, row.id)).evaluate(r => r.id)
    })

    await setup(database, 'foo', [
      { id: 1, value: 0 },
      { id: 2, value: 2 },
      { id: 3, value: 2 },
    ])

    await setup(database, 'bar', [
      { uid: 1, pid: 1, value: 0, obj: { x: '1', y: 'a' }, l: ['a,b', 'c'] },
      { uid: 1, pid: 1, value: 1, obj: { x: '2', y: 'b' }, },
      { uid: 1, pid: 2, value: 0, obj: { x: '3', y: 'c' }, },
    ])
  })
}

namespace ExperimentalTests {
  export function computed(database: Database<Tables>) {
    // it('strlist', async () => {
    //   const res = await database.get('bar', {})
    //   console.log('res', res)
    // })

    // it('get', async () => {
    //   await expect(database.get('foo', {})).to.eventually.deep.equal([
    //     { id: 2, pid: 1, uid: 1, value: 1, id2: 2 },
    //   ])
    // })

    it('project', async () => {
      const res = await database.select('bar')
        .project({
          count: row => (row.obj),
          count2: row => (row.obj.x)
        })
        // .orderBy(row => row.foo.id)
        .execute()
      console.log('res', res)
    })

    it('group', async () => {
      const res = await database.join(['foo', 'bar'] as const, (foo, bar) => $.eq(foo.id, bar.pid))
        .groupBy('foo', {
          // count: row => $.aggr(row.bar.obj),
          count2: row => $.aggr(row.bar.obj.y)
        })
        // .orderBy(row => row.foo.id)
        .execute()
      console.log('res', res)
    })

    // it('raw', async () => {
    //   const driver = Object.values(database.drivers)[0]
    //   const res = await driver.query(
    //     "SELECT `foo.id`, `foo.value`, `count` FROM (SELECT `foo`.`id` AS `foo.id`, `foo`.`value` AS `foo.value`, concat('[', group_concat(json_unquote(json_extract(`niormjql`. `bar.obj`, '$.x'))), ']') AS `count` FROM `foo` JOIN `bar` ON (`foo`.`id` = `bar`.`pid`) GROUP BY `foo.id`, `foo.value`) wvmceoou"
    //     )


    //   console.log('res', res)
    // })

    // it('raw2', async () => {
    //   const driver = Object.values(database.drivers)[0]
    //   const res = await driver.query("SELECT `foo.id`, `foo.value`, `count` FROM (SELECT `foo`.`id` AS `foo.id`, `foo`.`value` AS `foo.value`, group_concat(distinct `bar`.`id`) AS `count` FROM `foo` JOIN `bar` ON (`foo`.`id` = `bar`.`pid`) GROUP BY `foo.id`, `foo.value`) xlziynlx")

    //   console.log('res', res)
    // })
  }
}

export default ExperimentalTests
