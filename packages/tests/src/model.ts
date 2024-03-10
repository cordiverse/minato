import { $, Database, Typed } from 'minato'
import { expect } from 'chai'

declare module 'minato' {
  interface Database {
    bigint: Field.NewType<bigint>
  }
}

interface Bar {
  id: number
  text?: string
  num?: number
  double?: number
  decimal?: number
  bool?: boolean
  list?: string[]
  timestamp?: Date
  date?: Date
  time?: Date
  binary?: Buffer
  bigint?: bigint
}

interface Tables {
  dtypes: Bar
}

function ModelOperations(database: Database<Tables>) {
  database.bigint = database.define({
    type: 'string',
    dump: value => value ? value.toString() : value as any,
    load: value => value ? BigInt(value) : value as any,
    initial: 123n
  })

  database.extend('dtypes', {
    id: 'unsigned',
    text: {
      type: 'string',
      initial: 'he`l"\'\\lo',
    },
    num: {
      type: 'integer',
      initial: 233,
    },
    double: {
      type: 'double',
      initial: 3.14,
    },
    decimal: {
      type: 'decimal',
      scale: 3,
      initial: 12413
    },
    bool: {
      type: 'boolean',
      initial: true,
    },
    list: {
      type: 'list',
      initial: ['a`a', 'b"b', 'c\'c', 'd\\d'],
    },
    timestamp: {
      type: 'timestamp',
      initial: new Date('1970-01-01 00:00:00'),
    },
    date: {
      type: 'date',
      initial: new Date('1970-01-01'),
    },
    time: {
      type: 'time',
      initial: new Date('1970-01-01 12:00:00'),
    },
    binary: {
      type: 'blob',
      initial: Buffer.from('initial buffer')
    },
    bigint: database.bigint,
  }, {
    autoInc: true,
  })
}

namespace ModelOperations {
  const magicBorn = new Date('1970/08/17')

  const barTable: Bar[] = [
    { id: 1, bool: true },
    { id: 2, text: 'pku' },
    { id: 3, num: 1989 },
    { id: 4, list: ['1', '1', '4'] },
    { id: 5, timestamp: magicBorn },
    { id: 6, date: magicBorn },
    { id: 7, time: new Date('1970-01-01 12:00:00') },
    { id: 8, binary: Buffer.from('hello') },
    { id: 9, bigint: BigInt(1e63) },
    { id: 10, decimal: 2.432 },
  ]

  async function setup<K extends keyof Tables>(database: Database<Tables>, name: K, table: Tables[K][]) {
    await database.remove(name, {})
    const result: Tables[K][] = []
    for (const item of table) {
      result.push(await database.create(name, item as any))
    }
    return result
  }

  interface FieldsOptions {
    cast?: boolean
  }

  export const fields = function Fields(database: Database<Tables>, options: FieldsOptions = {}) {
    const { cast = true } = options

    it('basic', async () => {
      const table = await setup(database, 'dtypes', barTable)
      await expect(database.get('dtypes', {})).to.eventually.have.shape(table)

      await database.remove('dtypes', {})
      await database.upsert('dtypes', barTable)
      await expect(database.get('dtypes', {})).to.eventually.have.shape(table)
    })

    it('primitive', async () => {
      expect(Typed.transform($.literal(123)).type).to.equal(Typed.Number.type)
      expect(Typed.transform($.literal('abc')).type).to.equal(Typed.String.type)
      expect(Typed.transform($.literal(true)).type).to.equal(Typed.Boolean.type)
      expect(Typed.transform($.literal(new Date('1970-01-01'))).type).to.equal('timestamp')
      expect(Typed.transform($.literal(Buffer.from('hello'))).type).to.equal('blob')
      expect(Typed.transform($.literal([1, 2, 3])).type).to.equal('json')
      expect(Typed.transform($.literal({ a: 1 })).inner?.a.type).to.equal(Typed.Number.type)
    })

    cast && it('cast newtype', async () => {
      await setup(database, 'dtypes', barTable)
      await expect(database.get('dtypes', row => $.eq(row.bigint as any, $.literal(234n, database.bigint)))).to.eventually.have.length(0)
      await expect(database.get('dtypes', row => $.eq(row.bigint as any, $.literal(BigInt(1e63), database.bigint)))).to.eventually.have.length(1)
    })
  }
}

export default ModelOperations
