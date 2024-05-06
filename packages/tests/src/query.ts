import { $, Database } from 'minato'
import { expect } from 'chai'

interface Foo {
  id?: number
  text?: string
  value?: number
  bool?: boolean
  list?: number[]
  timestamp?: Date
  date?: Date
  time?: Date
  regex?: string
}

interface Tables {
  temp1: Foo
}

function QueryOperators(database: Database<Tables>) {
  database.extend('temp1', {
    id: 'unsigned',
    text: 'string',
    value: 'integer',
    bool: 'boolean',
    list: 'list',
    timestamp: 'timestamp',
    date: 'date',
    time: 'time',
    regex: 'string',
  }, {
    autoInc: true,
  })
}

namespace QueryOperators {
  interface QueryOptions {
    nullableComparator?: boolean
  }

  export const comparison = function Comparison(database: Database<Tables>, options: QueryOptions = {}) {
    const { nullableComparator = true } = options

    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', {
        text: 'awesome foo',
        timestamp: new Date('2000-01-01'),
        date: new Date('2020-01-01'),
        time: new Date('2020-01-01 12:00:00'),
      })
      await database.create('temp1', { text: 'awesome bar' })
      await database.create('temp1', { text: 'awesome baz' })
    })

    it('basic support', async () => {
      await expect(database.get('temp1', {
        id: { $eq: 2 },
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome bar')

      await expect(database.get('temp1', {
        id: { $ne: 3 },
      })).eventually.to.have.length(2).with.nested.property('0.text').equal('awesome foo')

      await expect(database.get('temp1', {
        id: { $gt: 1 },
      })).eventually.to.have.length(2).with.nested.property('1.text').equal('awesome baz')

      await expect(database.get('temp1', {
        id: { $gte: 3 },
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome baz')

      await expect(database.get('temp1', {
        id: { $lt: 1 },
      })).eventually.to.have.length(0)

      await expect(database.get('temp1', {
        id: { $lte: 2 },
      })).eventually.to.have.length(2).with.nested.property('0.text').equal('awesome foo')
    })

    it('timestamp comparisons', async () => {
      await expect(database.get('temp1', {
        timestamp: { $gt: new Date('1999-01-01') },
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome foo')

      await expect(database.get('temp1', {
        timestamp: { $lte: new Date('1999-01-01') },
      })).eventually.to.have.length(0)

      nullableComparator && await expect(database.get('temp1',
        row => $.gt(row.timestamp, new Date('1999-01-01'))
      )).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome foo')
    })

    it('date comparisons', async () => {
      await expect(database.get('temp1', {
        date: { $gt: new Date('1999-01-01') },
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome foo')

      await expect(database.get('temp1', {
        date: { $lte: new Date('1999-01-01') },
      })).eventually.to.have.length(0)
    })

    it('time comparisons', async () => {
      await expect(database.get('temp1', {
        // date should not matter
        time: { $gt: new Date('1970-01-01 11:00:00') },
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome foo')

      await expect(database.get('temp1', {
        time: { $lte: new Date('1970-01-01 11:00:00') },
      })).eventually.to.have.length(0)
    })

    it('shorthand syntax', async () => {
      await expect(database.get('temp1', {
        id: 2,
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome bar')

      await expect(database.get('temp1', {
        timestamp: new Date('2000-01-01'),
      })).eventually.to.have.length(1).with.nested.property('0.text').equal('awesome foo')
    })
  }

  export const existence = function Existence(database: Database<Tables>) {
    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { date: new Date('2010-01-01') })
      await database.create('temp1', { date: new Date('2020-01-01') })
      await database.create('temp1', {})
    })

    it('basic support', async () => {
      await expect(database.get('temp1', {
        date: { $exists: true },
      })).eventually.to.have.length(2)

      await expect(database.get('temp1', {
        date: { $exists: false },
      })).eventually.to.have.length(1)
    })

    it('shorthand syntax', async () => {
      await expect(database.get('temp1', {
        date: null,
      })).eventually.to.have.length(1)
    })
  }

  export const membership = function Membership(database: Database<Tables>) {
    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { value: 3 })
      await database.create('temp1', { value: 4 })
      await database.create('temp1', { value: 7 })
    })

    it('edge cases', async () => {
      await expect(database.get('temp1', {
        value: { $in: [] },
      })).eventually.to.have.length(0)

      await expect(database.get('temp1', {
        value: { $nin: [] },
      })).eventually.to.have.length(3)
    })

    it('basic support', async () => {
      await expect(database.get('temp1', {
        value: { $in: [3, 4, 5] },
      })).eventually.to.have.length(2)

      await expect(database.get('temp1', (row) => {
        return $.in(row.value, [3, 4, 5])
      })).eventually.to.have.length(2)

      await expect(database.get('temp1', {
        value: { $nin: [4, 5, 6] },
      })).eventually.to.have.length(2)
    })

    it('shorthand syntax', async () => {
      await expect(database.get('temp1', {
        value: [],
      })).eventually.to.have.length(0)

      await expect(database.get('temp1', {
        value: [3, 4, 5],
      })).eventually.to.have.length(2)
    })
  }

  interface RegExpOptions {
    regexBy?: boolean
    regexFor?: boolean
  }

  export const regexp = function RegularExpression(database: Database<Tables>, options: RegExpOptions = {}) {
    const { regexBy = true, regexFor = true } = options

    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { text: 'awesome foo', regex: 'foo' })
      await database.create('temp1', { text: 'awesome bar', regex: 'bar' })
      await database.create('temp1', { text: 'awesome foo bar', regex: 'baz' })
    })

    regexFor && it('$regexFor', async () => {
      await expect(database.get('temp1', {
        regex: { $regexFor: 'foo bar' },
      })).eventually.to.have.length(2)

      await expect(database.get('temp1', {
        regex: { $regexFor: 'baz' },
      })).eventually.to.have.length(1)
    })

    regexBy && it('$regexBy', async () => {
      await expect(database.get('temp1', {
        text: { $regex: /^.*foo.*$/ },
      })).eventually.to.have.length(2)

      await expect(database.get('temp1', {
        text: { $regex: /^.*bar$/ },
      })).eventually.to.have.length(2)
    })

    regexBy && it('shorthand syntax', async () => {
      await expect(database.get('temp1', {
        text: /^.*foo$/,
      })).eventually.to.have.length(1).with.nested.property('[0].text').equal('awesome foo')
    })

    regexBy && regexFor && it('$.regex', async () => {
      await expect(database.get('temp1', row => $.regex('foo bar', row.regex))).eventually.to.have.length(2)
      await expect(database.get('temp1', row => $.regex('baz', row.regex))).eventually.to.have.length(1)
      await expect(database.get('temp1', row => $.regex(row.text, /^.*foo.*$/))).eventually.to.have.length(2)
      await expect(database.get('temp1', row => $.regex(row.text, /^.*bar.*$/))).eventually.to.have.length(2)
      await expect(database.get('temp1', row => $.regex(row.text, row.regex))).eventually.to.have.length(2)
    })
  }

  export const bitwise = function Bitwise(database: Database<Tables>) {
    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { value: 3 })
      await database.create('temp1', { value: 4 })
      await database.create('temp1', { value: 7 })
    })

    it('basic support', async () => {
      await expect(database.get('temp1', {
        value: { $bitsAllSet: 3 },
      })).eventually.to.have.shape([{ value: 3 }, { value: 7 }])

      await expect(database.get('temp1', {
        value: { $bitsAllClear: 9 },
      })).eventually.to.have.shape([{ value: 4 }])

      await expect(database.get('temp1', {
        value: { $bitsAnySet: 4 },
      })).eventually.to.have.shape([{ value: 4 }, { value: 7 }])

      await expect(database.get('temp1', {
        value: { $bitsAnyClear: 6 },
      })).eventually.to.have.shape([{ value: 3 }, { value: 4 }])
    })

    it('using expressions', async () => {
      await expect(database.get('temp1',
        row => $.eq($.bitAnd(row.value, 1, 1), 1),
      )).eventually.to.have.shape([{ value: 3 }, { value: 7 }])

      await expect(database.get('temp1',
        row => $.eq($.bitOr(row.value, 3, 3), 7),
      )).eventually.to.have.shape([{ value: 4 }, { value: 7 }])

      await expect(database.get('temp1',
        row => $.eq($.bitAnd(row.value, $.bitNot(4)), 3),
      )).eventually.to.have.shape([{ value: 3 }, { value: 7 }])

      await expect(database.get('temp1',
        row => $.eq($.bitXor(row.value, 3), 7),
      )).eventually.to.have.shape([{ value: 4 }])
    })
  }

  interface ListOptions {
    size?: boolean
    element?: boolean
    elementQuery?: boolean
  }

  export const list = function List(database: Database<Tables>, options: ListOptions = {}) {
    const { size = true, element = true, elementQuery = element } = options

    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { id: 1, list: [] })
      await database.create('temp1', { id: 2, list: [23] })
      await database.create('temp1', { id: 3, list: [233] })
      await database.create('temp1', { id: 4, list: [233, 332] })
    })

    size && it('$size', async () => {
      await expect(database.get('temp1', {
        list: { $size: 1 },
      })).eventually.to.have.length(2).with.shape([{ id: 2 }, { id: 3 }])
    })

    size && it('$.length', async () => {
      await expect(database.select('temp1')
        .project({ x: row => $.length(row.list) })
        .orderBy(row => row.x)
        .execute()
      ).eventually.to.deep.equal([
        { x: 0 },
        { x: 1 },
        { x: 1 },
        { x: 2 },
      ])
    })

    element && it('$el shorthand', async () => {
      await expect(database.get('temp1', {
        list: { $el: 233 },
      })).eventually.to.have.length(2).with.shape([{ id: 3 }, { id: 4 }])
    })

    elementQuery && it('$el with field temp1', async () => {
      await expect(database.get('temp1', {
        list: { $el: { $lt: 50 } },
      })).eventually.to.have.shape([{ id: 2 }])
    })
  }

  export const evaluation = function Evaluation(database: Database<Tables>) {
    before(async () => {
      await database.remove('temp1', {})
      await database.create('temp1', { id: 1, value: 8 })
      await database.create('temp1', { id: 2, value: 7 })
      await database.create('temp1', { id: 3, value: 9 })
    })

    it('arithmetic operators', async () => {
      await expect(database.get('temp1', (row) => {
        return $.eq(9, $.add(row.id, row.value))
      })).eventually.to.have.length(2).with.shape([{ id: 1 }, { id: 2 }])
    })
  }

  namespace Logical {
    export const queryLevel = function LogicalQueryLevel(database: Database<Tables>) {
      before(async () => {
        await database.remove('temp1', {})
        await database.create('temp1', { id: 1 })
        await database.create('temp1', { id: 2 })
        await database.create('temp1', { id: 3 })
      })

      it('edge cases', async () => {
        await expect(database.get('temp1', {})).eventually.to.have.length(3)
        await expect(database.get('temp1', { $and: [] })).eventually.to.have.length(3)
        await expect(database.get('temp1', { $or: [] })).eventually.to.have.length(0)
        await expect(database.get('temp1', { $not: {} })).eventually.to.have.length(0)
        await expect(database.get('temp1', { $not: { $and: [] } })).eventually.to.have.length(0)
        await expect(database.get('temp1', { $not: { $or: [] } })).eventually.to.have.length(3)
      })

      it('$or', async () => {
        await expect(database.get('temp1', {
          $or: [{ id: 1 }, { id: { $ne: 2 } }],
        })).eventually.to.have.length(2).with.shape([{ id: 1 }, { id: 3 }])

        await expect(database.get('temp1', {
          $or: [{ id: 1 }, { id: { $eq: 2 } }],
        })).eventually.to.have.length(2).with.shape([{ id: 1 }, { id: 2 }])

        await expect(database.get('temp1', {
          $or: [{ id: { $ne: 1 } }, { id: { $ne: 2 } }],
        })).eventually.to.have.length(3).with.shape([{ id: 1 }, { id: 2 }, { id: 3 }])
      })

      it('$and', async () => {
        await expect(database.get('temp1', {
          $and: [{ id: 1 }, { id: { $ne: 2 } }],
        })).eventually.to.have.length(1).with.shape([{ id: 1 }])

        await expect(database.get('temp1', {
          $and: [{ id: 1 }, { id: { $eq: 2 } }],
        })).eventually.to.have.length(0)

        await expect(database.get('temp1', {
          $and: [{ id: { $ne: 1 } }, { id: { $ne: 2 } }],
        })).eventually.to.have.length(1).with.shape([{ id: 3 }])
      })

      it('$not', async () => {
        await expect(database.get('temp1', {
          $not: { id: 1 },
        })).eventually.to.have.length(2).with.shape([{ id: 2 }, { id: 3 }])

        await expect(database.get('temp1', {
          $not: { id: { $ne: 1 } },
        })).eventually.to.have.length(1).with.shape([{ id: 1 }])
      })
    }

    export const fieldLevel = function LogicalFieldLevel(database: Database<Tables>) {
      before(async () => {
        await database.remove('temp1', {})
        await database.create('temp1', { id: 1 })
        await database.create('temp1', { id: 2 })
        await database.create('temp1', { id: 3 })
      })

      it('edge cases', async () => {
        await expect(database.get('temp1', { id: {} })).eventually.to.have.length(3)
        await expect(database.get('temp1', { id: { $and: [] } })).eventually.to.have.length(3)
        await expect(database.get('temp1', { id: { $or: [] } })).eventually.to.have.length(0)
        await expect(database.get('temp1', { id: { $not: {} } })).eventually.to.have.length(0)
        await expect(database.get('temp1', { id: { $not: { $and: [] } } })).eventually.to.have.length(0)
        await expect(database.get('temp1', { id: { $not: { $or: [] } } })).eventually.to.have.length(3)
      })

      it('$or', async () => {
        await expect(database.get('temp1', {
          id: { $or: [1, { $gt: 2 }] },
        })).eventually.to.have.length(2).with.shape([{ id: 1 }, { id: 3 }])

        await expect(database.get('temp1', {
          id: { $or: [1, { $gt: 2 }], $ne: 3 },
        })).eventually.to.have.length(1).with.shape([{ id: 1 }])
      })

      it('$and', async () => {
        await expect(database.get('temp1', {
          id: { $and: [[1, 2], { $lt: 2 }] },
        })).eventually.to.have.length(1).with.shape([{ id: 1 }])

        await expect(database.get('temp1', {
          id: { $and: [[1, 2], { $lt: 2 }], $eq: 2 },
        })).eventually.to.have.length(0)
      })

      it('$not', async () => {
        await expect(database.get('temp1', {
          id: { $not: 1 },
        })).eventually.to.have.length(2).with.shape([{ id: 2 }, { id: 3 }])

        await expect(database.get('temp1', {
          id: { $not: 1, $lt: 3 },
        })).eventually.to.have.length(1).with.shape([{ id: 2 }])
      })
    }
  }

  export const logical = Logical
}

export default QueryOperators
