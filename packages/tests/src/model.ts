import { valueMap } from 'cosmokit'
import { $, clone, Database, Field, Type } from 'minato'
import { expect } from 'chai'

interface DType {
  id: number
  text?: string
  num?: number
  double?: number
  decimal?: number
  bool?: boolean
  list?: string[]
  array?: number[]
  object?: {
    text?: string
    num?: number
    embed?: {
      bool?: boolean
      bigint?: bigint
    }
  }
  object2?: {
    text?: string
    num?: number
    embed?: {
      bool?: boolean
      bigint?: bigint
    }
  }
  timestamp?: Date
  date?: Date
  time?: Date
  binary?: Buffer
  bigint?: bigint
  bnum?: number
  bnum2?: number
}

interface DObject {
  id: number
  foo?: {
    nested: DType
  }
  bar?: {
    nested: DType
  }
  baz?: {
    nested?: DType
  }[]
}

interface Tables {
  dtypes: DType
  dobjects: DObject
}

interface Types {
  bigint: bigint
}

function flatten<T, N, P extends string>(type: Field.Definition<T, N>, prefix: P): Field.Extension<{ [K in P]: T }, N> {
  if (typeof type.type === 'object') {
    const result = {}
    for (const key in type.type) {
      Object.assign(result, flatten(type.type[key], `${prefix}.${key}`))
    }
    return result
  } else {
    return { [prefix]: type } as any
  }
}

function ModelOperations(database: Database<Tables, Types>) {
  database.define('bigint', {
    type: 'string',
    dump: value => value ? value.toString() : value as any,
    load: value => value ? BigInt(value) : value as any,
    initial: 123n
  })

  const bnum = database.define({
    type: 'binary',
    dump: value => value === undefined ? value : Buffer.from(String(value)),
    load: value => value ? +value : value,
    initial: 0,
  })

  const baseFields: Field.Extension<DType, Types> = {
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
    array: {
      type: 'json',
      initial: [1, 2, 3],
    },
    object: {
      type: {
        num: 'unsigned',
        text: 'string',
        embed: {
          type: {
            bool: {
              type: 'boolean',
              initial: false,
            },
            bigint: 'bigint',
          }
        }
      },
      initial: {
        num: 1,
        text: '2',
        embed: {
          bool: true,
          bigint: 123n,
        }
      }
    },
    // dot defined object
    'object2.num': {
      type: 'unsigned',
      initial: 1,
    },
    'object2.text': {
      type: 'string',
      initial: '2'
    },
    'object2.embed.bool': {
      type: 'boolean',
      initial: true,
    },
    'object2.embed.bigint': 'bigint',
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
      type: 'binary',
      initial: Buffer.from('initial buffer')
    },
    bigint: 'bigint',
    bnum,
    bnum2: {
      type: 'binary',
      dump: value => value === undefined ? value : Buffer.from(String(value)),
      load: value => value ? +value : value,
      initial: 0,
    }
  }

  const baseObject = {
    type: { nested: { type: baseFields } },
    initial: { nested: { id: 1 } }
  }

  database.extend('dtypes', {
    ...baseFields
  }, { autoInc: true })

  database.extend('dobjects', {
    id: 'unsigned',
    foo: baseObject,
    ...flatten(baseObject, 'bar'),
    baz: { type: [baseObject] },
  }, { autoInc: true })
}

function getValue(obj: any, path: string) {
  if (path.includes('.')) {
    const index = path.indexOf('.')
    return getValue(obj[path.slice(0, index)] ??= {}, path.slice(index + 1))
  } else {
    return obj[path]
  }
}

namespace ModelOperations {
  const magicBorn = new Date('1970/08/17')

  const dtypeTable: DType[] = [
    { id: 1, bool: false },
    { id: 2, text: 'pku' },
    { id: 3, num: 1989 },
    { id: 4, list: ['1', '1', '4'], array: [1, 1, 4] },
    { id: 5, object: { num: 10, text: 'ab', embed: { bool: false, bigint: 90n } } },
    { id: 6, object2: { num: 10, text: 'ab', embed: { bool: false, bigint: 90n } } },
    { id: 7, timestamp: magicBorn },
    { id: 8, date: magicBorn },
    { id: 9, time: new Date('1999-10-01 15:40:00') },
    { id: 10, binary: Buffer.from('hello') },
    { id: 11, bigint: BigInt(1e63) },
    { id: 12, decimal: 2.432 },
    { id: 13, bnum: 114514, bnum2: 12345 },
  ]

  const dobjectTable: DObject[] = [
    { id: 1 },
    { id: 2, foo: { nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } } },
    { id: 3, bar: { nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } } },
    { id: 4, baz: [{ nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } }, { nested: { id: 2 } }] },
    { id: 5, foo: { nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object2: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } } },
    { id: 6, bar: { nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object2: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } } },
    { id: 7, baz: [{ nested: { id: 1, list: ['1', '1', '4'], array: [1, 1, 4], object2: { num: 10, text: 'ab', embed: { bool: false, bigint: BigInt(1e163) } }, bigint: BigInt(1e63), bnum: 114514, bnum2: 12345 } }, { nested: { id: 2 } }] },
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
    typeModel?: boolean
  }

  export const fields = function Fields(database: Database<Tables, Types>, options: FieldsOptions = {}) {
    const { cast = true, typeModel = true } = options

    it('basic', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await expect(database.get('dtypes', {})).to.eventually.have.shape(table)

      await database.remove('dtypes', {})
      await database.upsert('dtypes', dtypeTable)
      await expect(database.get('dtypes', {})).to.eventually.have.shape(table)
    })

    it('modifier', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await database.remove('dtypes', {})
      await database.upsert('dtypes', dtypeTable.map(({ id }) => ({ id })))

      await Promise.all(table.map(({ id, ...x }) => database.set('dtypes', id, x)))
      await expect(database.get('dtypes', {})).to.eventually.have.shape(table)
    })

    it('primitive', async () => {
      expect(Type.fromTerm($.literal(123)).type).to.equal(Type.Number.type)
      expect(Type.fromTerm($.literal('abc')).type).to.equal(Type.String.type)
      expect(Type.fromTerm($.literal(true)).type).to.equal(Type.Boolean.type)
      expect(Type.fromTerm($.literal(new Date('1970-01-01'))).type).to.equal('timestamp')
      expect(Type.fromTerm($.literal(Buffer.from('hello'))).type).to.equal('binary')
      expect(Type.fromTerm($.literal([1, 2, 3])).type).to.equal('json')
      expect(Type.fromTerm($.literal({ a: 1 })).type).to.equal('json')
    })

    cast && it('cast newtype', async () => {
      await setup(database, 'dtypes', dtypeTable)
      await expect(database.get('dtypes', row => $.eq(row.bigint as any, $.literal(234n, 'bigint')))).to.eventually.have.length(0)
      await expect(database.get('dtypes', row => $.eq(row.bigint as any, $.literal(BigInt(1e63), 'bigint')))).to.eventually.have.length(1)
    })

    typeModel && it('$.object encoding', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await expect(database.eval('dtypes', row => $.array($.object(row)))).to.eventually.have.shape(table)
    })

    typeModel && it('$.object decoding', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await expect(database.select('dtypes')
        .project({
          obj: row => $.object(row)
        })
        .project(valueMap(database.tables['dtypes'].fields as any, (field, key) => row => row.obj[key]))
        .execute()
      ).to.eventually.have.shape(table)
    })

    typeModel && it('$.array encoding on cell', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await expect(database.eval('dtypes', row => $.array(row.object))).to.eventually.have.shape(table.map(x => x.object))
      await expect(database.eval('dtypes', row => $.array($.object(row.object2)))).to.eventually.have.shape(table.map(x => x.object2))
    })

    it('$.array encoding', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await Promise.all(Object.keys(database.tables['dtypes'].fields).map(
        key => expect(database.eval('dtypes', row => $.array(row[key]))).to.eventually.have.shape(table.map(x => getValue(x, key)))
      ))
    })

    it('subquery encoding', async () => {
      const table = await setup(database, 'dtypes', dtypeTable)
      await Promise.all(Object.keys(database.tables['dtypes'].fields).map(
        key => expect(database.select('dtypes', 1)
          .project({
            x: row => database.select('dtypes').evaluate(key as any)
          })
          .execute()
        ).to.eventually.have.shape([{ x: table.map(x => getValue(x, key)) }])
      ))
    })
  }

  export const object = function ObjectFields(database: Database<Tables, Types>, options: FieldsOptions = {}) {
    const { typeModel = true } = options

    it('basic', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await expect(database.get('dobjects', {})).to.eventually.have.shape(table)

      await database.remove('dobjects', {})
      await database.upsert('dobjects', dobjectTable)
      await expect(database.get('dobjects', {})).to.eventually.have.shape(table)
    })

    it('modifier', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await database.remove('dobjects', {})
      await database.upsert('dobjects', dobjectTable.map(({ id }) => ({ id })))

      await Promise.all(table.map(({ id, ...x }) => database.set('dobjects', id, x)))
      await expect(database.get('dobjects', {})).to.eventually.have.shape(table)
    })

    typeModel && it('$.object decoding', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await expect(database.select('dobjects')
        .project({
          obj: row => $.object(row)
        })
        .project(valueMap(database.tables['dobjects'].fields as any, (field, key) => row => row.obj[key]))
        .execute()
      ).to.eventually.have.shape(table)
    })

    typeModel && it('$.object decoding', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await expect(database.select('dobjects')
        .project({
          obj: row => $.object(row)
        })
        .project(valueMap(database.tables['dobjects'].fields as any, (field, key) => row => row.obj[key]))
        .execute()
      ).to.eventually.have.shape(table)
    })

    it('$.array encoding', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await Promise.all(Object.keys(database.tables['dobjects'].fields).map(
        key => expect(database.eval('dobjects', row => $.array(row[key]))).to.eventually.have.shape(table.map(x => getValue(x, key)))
      ))
    })

    it('subquery encoding', async () => {
      const table = await setup(database, 'dobjects', dobjectTable)
      await Promise.all(Object.keys(database.tables['dobjects'].fields).map(
        key => expect(database.select('dobjects', 1)
          .project({
            x: row => database.select('dobjects').evaluate(key as any)
          })
          .execute()
        ).to.eventually.have.shape([{ x: table.map(x => getValue(x, key)) }])
      ))
    })
  }
}

export default ModelOperations
