import { $, Database } from 'minato'
import { expect } from 'chai'
import { setup } from './utils'

interface Perf {
  id: number
  text: string
  number: number
}

interface PerfNested {
  id: number
  meta: {
    label: string
    score: number
  }
  items: {
    value: string
    index: number
  }[]
}

interface Tables {
  perf: Perf
  perfNested: PerfNested
}

function PerformanceTests(database: Database<Tables>) {
  const benchmark = async <T>(label: string = '', repeat: number, task: (index: number) => Promise<T>) => {
    await task(0)
    const start = performance.now()
    let result: T
    for (let i = 0; i < repeat; i++) {
      result = await task(i)
    }
    const elapsed = performance.now() - start
    console.log(`[perf] ${label}: total=${elapsed.toFixed(2)}ms avg=${(elapsed / repeat).toFixed(2)}ms`)
    return result!
  }

  const cycleId = (index: number, size: number) => (index % size) + 1

  before(async () => {
    database.extend('perf', {
      id: 'unsigned',
      text: 'text',
      number: 'integer',
    }, {
      autoInc: true,
    })

    database.extend('perfNested', {
      id: 'unsigned',
      meta: {
        type: 'object',
        inner: {
          label: 'string',
          score: 'integer',
        },
      },
      items: {
        type: 'array',
        inner: {
          type: 'object',
          inner: {
            value: 'string',
            index: 'integer',
          },
        },
      },
    })

    await database.upsert('perf', new Array(2000).fill(0).map((_, i) => ({ text: 'hello', number: i })))

    await setup(database, 'perfNested', new Array(500).fill(0).map((_, i) => ({
      id: i + 1,
      meta: {
        label: `label-${i}`,
        score: i,
      },
      items: new Array(3).fill(0).map((_, j) => ({
        value: `value-${i}-${j}`,
        index: j,
      })),
    })))
  })

  it('bulk get flat rows', async function () {
    const rows = await benchmark(this.test?.title, 10, () => database.get('perf', {}))
    expect(rows).to.have.length(2000)
    expect(rows[1999]).to.include({ text: 'hello', number: 1999 })
  })

  it('bulk get nested rows', async function () {
    const rows = await benchmark(this.test?.title, 8, () => database.get('perfNested', {}))
    expect(rows).to.have.length(500)
    expect(rows[0].meta.label).to.equal('label-0')
    expect(rows[0].items).to.have.length(3)
  })

  it('bulk upsert nested rows', async function () {
    const payload = new Array(200).fill(0).map((_, i) => ({
      id: i + 1,
      meta: {
        label: `updated-${i}`,
        score: i * 2,
      },
      items: new Array(4).fill(0).map((_, j) => ({
        value: `updated-${i}-${j}`,
        index: j,
      })),
    }))

    await benchmark(this.test?.title, 5, () => database.upsert('perfNested', payload, ['id']))

    const rows = await database.get('perfNested', { id: { $in: [1, 50, 200] } })
    expect(rows).to.have.length(3)
    expect(rows[0].items).to.have.length(4)
  })

  it('bulk upsert direct fields', async function () {
    const payload = new Array(400).fill(0).map((_, i) => ({
      id: i + 1,
      text: `direct-${i}`,
      number: i * 3,
    }))

    await benchmark(this.test?.title, 8, () => database.upsert('perf', payload, ['id']))

    const rows = await database.get('perf', { id: { $in: [1, 200, 400] } })
    expect(rows).to.have.length(3)
    expect(rows[0].text).to.match(/^direct-/)
  })

  it('bulk upsert expression fields', async function () {
    await benchmark(this.test?.title, 8, () => database.upsert('perf', row => {
      return new Array(400).fill(0).map((_, i) => ({
        id: i + 1,
        number: $.multiply(row.id, 2),
      }))
    }))

    const rows = await database.get('perf', { id: { $in: [1, 200, 400] } })
    expect(rows).to.have.length(3)
    expect(rows[0].number).to.equal(rows[0].id * 2)
  })

  it('bulk upsert dotted fields', async function () {
    const payload = new Array(200).fill(0).map((_, i) => ({
      id: i + 1,
      'meta.label': `dotted-${i}`,
      'meta.score': i * 5,
    }))

    await benchmark(this.test?.title, 6, () => database.upsert('perfNested', payload, ['id']))

    const rows = await database.get('perfNested', { id: { $in: [1, 50, 200] } })
    expect(rows).to.have.length(3)
    expect(rows[0].meta.label).to.match(/^dotted-/)
  })

  it('repeated get single flat row', async function () {
    const row = await benchmark(this.test?.title, 400, (index) => {
      return database.get('perf', { id: cycleId(index, 40) })
    })

    expect(row).to.have.length(1)
    expect(row[0].text).to.be.a('string')
  })

  it('repeated get single nested row', async function () {
    const row = await benchmark(this.test?.title, 300, (index) => {
      return database.get('perfNested', { id: 300 + cycleId(index, 30) })
    })

    expect(row).to.have.length(1)
    expect(row[0].meta.label).to.be.a('string')
    expect(row[0].items).to.have.length(3)
  })

  it('repeated set single flat row', async function () {
    await benchmark(this.test?.title, 300, (index) => {
      return database.set('perf', { id: 1 }, {
        text: `single-set-${index}`,
        number: index,
      })
    })

    const rows = await database.get('perf', { id: 1 })
    expect(rows).to.have.length(1)
    expect(rows[0]).to.include({ text: 'single-set-299', number: 299 })
  })

  it('repeated set single nested row', async function () {
    await benchmark(this.test?.title, 200, (index) => {
      return database.set('perfNested', { id: 1 }, {
        'meta.label': `single-nested-set-${index}`,
        'meta.score': index * 3,
      } as any)
    })

    const rows = await database.get('perfNested', { id: 1 })
    expect(rows).to.have.length(1)
    expect(rows[0].meta).to.include({
      label: 'single-nested-set-199',
      score: 597,
    })
  })

  it('repeated upsert single flat row', async function () {
    await benchmark(this.test?.title, 300, (index) => {
      return database.upsert('perf', [{
        id: 2,
        text: `single-upsert-${index}`,
        number: index * 2,
      }], ['id'])
    })

    const rows = await database.get('perf', { id: 2 })
    expect(rows).to.have.length(1)
    expect(rows[0]).to.include({ text: 'single-upsert-299', number: 598 })
  })

  it('repeated upsert single nested dotted row', async function () {
    await benchmark(this.test?.title, 200, (index) => {
      return database.upsert('perfNested', [{
        id: 1,
        'meta.label': `single-dotted-${index}`,
        'meta.score': index,
      }], ['id'])
    })

    const rows = await database.get('perfNested', { id: 1 })
    expect(rows).to.have.length(1)
    expect(rows[0].meta).to.include({ label: 'single-dotted-199', score: 199 })
  })

  it('repeated create and remove single flat row', async function () {
    const created = await benchmark(this.test?.title, 150, async (index) => {
      const row = await database.create('perf', {
        text: `single-create-${index}`,
        number: -index,
      })
      await database.remove('perf', { id: row.id })
      return row
    })

    expect(created.id).to.be.a('number')
    const rows = await database.get('perf', { text: created.text })
    expect(rows).to.have.length(0)
  })
}

export default PerformanceTests
