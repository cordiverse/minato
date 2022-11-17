import { clone, Dict, makeArray, noop, pick } from 'cosmokit'
import { Database, Driver, Eval, executeEval, executeQuery, executeSort, executeUpdate, Modifier, RuntimeError, Selection } from '@minatojs/core'

namespace MemoryDriver {
  export interface Config {}
}

class MemoryDriver extends Driver {
  #store: Dict<any[]> = {
    _fields: [],
  }

  constructor(public database: Database, public config: MemoryDriver.Config) {
    super(database)
  }

  async prepare(name: string) {}

  async start() {
    // await this.#loader?.start(this.#store)
  }

  async $save(name: string) {
    // await this.#loader?.save(name, this.#store[name])
  }

  async stop() {
    // await this.#loader?.stop(this.#store)
  }

  $table(sel: string | Selection) {
    if (typeof sel === 'string') {
      return this.#store[sel] ||= []
    }

    const { ref, query, table, args } = sel
    const data = this.$table(table).filter(row => executeQuery(row, query, ref))
    return executeSort(data, args[0], ref).map(row => sel.resolveData(row, args[0].fields))
  }

  async drop() {
    this.#store = {}
    // await this.#loader?.drop()
  }

  async stats() {
    return {}
  }

  async get(sel: Selection.Immutable, modifier: Modifier) {
    const { ref, query, table, args } = sel
    const data = this.$table(table).filter(row => executeQuery(row, query, ref))
    return executeSort(data, modifier, ref).map(row => sel.resolveData(row, args[0].fields))
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const { query, table } = sel
    const ref = typeof table === 'string' ? sel.ref : table.ref
    const data = this.$table(table).filter(row => executeQuery(row, query, ref))
    return executeEval(data.map(row => ({ [ref]: row, _: row })), expr)
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { table, ref, query } = sel
    this.$table(table)
      .filter(row => executeQuery(row, query, ref))
      .forEach(row => executeUpdate(row, data, ref))
    this.$save(table)
  }

  async remove(sel: Selection.Mutable) {
    const { ref, query, table } = sel
    this.#store[table] = this.$table(table).filter(row => !executeQuery(row, query, ref))
    this.$save(table)
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table, model } = sel
    const { primary, autoInc } = model
    const store = this.$table(table)
    if (!Array.isArray(primary) && autoInc && !(primary in data)) {
      let meta = this.#store._fields.find(row => row.table === table && row.field === primary)
      if (!meta) {
        meta = { table, field: primary, autoInc: 0 }
        this.#store._fields.push(meta)
      }
      meta.autoInc += 1
      data[primary] = meta.autoInc
    } else {
      const duplicated = await this.database.get(table, pick(data, makeArray(primary)))
      if (duplicated.length) {
        throw new RuntimeError('duplicate-entry')
      }
    }
    const copy = model.create(data)
    store.push(copy)
    this.$save(table)
    return clone(copy)
  }

  async upsert(sel: Selection.Mutable, data: any, keys: string[]) {
    const { table, model, ref } = sel
    for (const update of data) {
      const row = this.$table(table).find(row => {
        return keys.every(key => row[key] === update[key])
      })
      if (row) {
        executeUpdate(row, update, ref)
      } else {
        const data = executeUpdate(model.create(), update, ref)
        await this.database.create(table, data).catch(noop)
      }
    }
    this.$save(table)
  }
}

export default MemoryDriver
