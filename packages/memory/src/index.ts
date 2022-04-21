import { clone, Dict, makeArray, noop, pick } from 'cosmokit'
import { Database, Driver, Eval, Executable, executeEval, executeQuery, executeSort, executeUpdate, Field, Modifier, RuntimeError } from 'cosmotype'

namespace MemoryDriver {
  export interface Config {}
}

class MemoryDriver extends Driver {
  #store: Dict<any[]> = {}

  constructor(public database: Database, public config: MemoryDriver.Config) {
    super(database, 'memory')
  }

  async prepare(name: string) {}

  async start() {
    // await this.#loader?.start(this.#store)
    super.start()
  }

  async $save(name: string) {
    // await this.#loader?.save(name, this.#store[name])
  }

  async stop() {
    super.stop()
  }

  $table(table: string) {
    return this.#store[table] ||= []
  }

  async drop() {
    this.#store = {}
    // await this.#loader?.drop()
  }

  async stats() {
    return {}
  }

  async get(sel: Executable, modifier: Modifier) {
    const { ref, query, fields, table } = sel
    const data = this.$table(table).filter(row => executeQuery(row, query, ref))
    return executeSort(data, modifier, ref).map(row => sel.resolveData(row, fields))
  }

  async eval(sel: Executable, expr: Eval.Expr) {
    const { ref, query, table } = sel
    const data = this.$table(table).filter(row => executeQuery(row, query, ref))
    return executeEval(data.map(row => ({ [ref]: row, _: row })), expr)
  }

  async set(sel: Executable, data: {}) {
    const { table, ref, query } = sel
    this.$table(table)
      .filter(row => executeQuery(row, query, ref))
      .forEach(row => executeUpdate(row, data, ref))
    this.$save(table)
  }

  async remove(sel: Executable) {
    const { ref, query, table } = sel
    this.#store[table] = this.$table(table).filter(row => !executeQuery(row, query, ref))
    this.$save(table)
  }

  async create(sel: Executable, data: any) {
    const { table, model } = sel
    const { primary, fields, autoInc } = model
    const store = this.$table(table)
    if (!Array.isArray(primary) && autoInc && !(primary in data)) {
      const max = store.length ? Math.max(...store.map(row => +row[primary])) : 0
      data[primary] = max + 1
      if (Field.string.includes(fields[primary].type)) {
        data[primary] += ''
      }
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

  async upsert(sel: Executable, data: any, keys: string[]) {
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
