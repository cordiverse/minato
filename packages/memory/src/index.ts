import { clone, Dict, makeArray, noop, omit, pick, valueMap } from 'cosmokit'
import { Database, Driver, Eval, executeEval, executeQuery, executeSort, executeUpdate, RuntimeError, Selection } from '@minatojs/core'

export namespace MemoryDriver {
  export interface Config {}
}

export class MemoryDriver extends Driver {
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

  table(sel: string | Selection.Immutable | Dict<string | Selection.Immutable>, expr?: any): any[] {
    if (typeof sel === 'string') {
      return this.#store[sel] ||= []
    }

    if (!(sel instanceof Selection)) {
      const entries = Object.entries(sel).map(([name, sel]) => [name, this.table(sel)] as const)
      const catesian = (entries: (readonly [string, any[]])[]): any[] => {
        if (!entries.length) return []
        const [[name, rows], ...tail] = entries
        if (!tail.length) return rows.map(row => ({ [name]: row }))
        return rows.flatMap(row => catesian(tail).map(tail => ({ ...tail, [name]: row })))
      }
      return catesian(entries).filter(data => executeEval(data, expr))
    }

    const { ref, query, table, args, model } = sel
    const { fields, group, having } = sel.args[0]
    const data = this.table(table, having).filter(row => executeQuery(row, query, ref))

    const branches: { index: Dict; table: any[] }[] = []
    const groupFields = group ? pick(fields!, group) : fields
    for (let row of executeSort(data, args[0], ref)) {
      row = model.format(row, false)
      for (const key in model.fields) {
        if (model.fields[key]!.deprecated) continue
        row[key] ??= null
      }
      let index = row
      if (fields) {
        index = valueMap(groupFields!, (expr) => executeEval({ [ref]: row }, expr))
      }
      let branch = branches.find((branch) => {
        if (!group || !groupFields) return false
        for (const key in groupFields) {
          if (branch.index[key] !== index[key]) return false
        }
        return true
      })
      if (!branch) {
        branch = { index, table: [] }
        branches.push(branch)
      }
      branch.table.push(row)
    }
    return branches.map(({ index, table }) => {
      if (group) {
        if (having) {
          const value = executeEval(table.map(row => ({ [ref]: row, _: row })), having)
          if (!value) return
        }
        for (const key in omit(fields!, group)) {
          index[key] = executeEval(table.map(row => ({ [ref]: row, _: row })), fields![key])
        }
      }
      return model.parse(index, false)
    }).filter(Boolean)
  }

  async drop(table?: string) {
    if (table) {
      delete this.#store[table]
    } else {
      this.#store = { _fields: [] }
    }
  }

  async stats() {
    return {}
  }

  async get(sel: Selection.Immutable) {
    return this.table(sel as Selection)
  }

  async eval(sel: Selection.Immutable, expr: Eval.Expr) {
    const { query, table } = sel
    const ref = typeof table === 'string' ? sel.ref : table.ref as string
    const data = this.table(table).filter(row => executeQuery(row, query, ref))
    return executeEval(data.map(row => ({ [ref]: row, _: row })), expr)
  }

  async set(sel: Selection.Mutable, data: {}) {
    const { table, ref, query } = sel
    const matched = this.table(table)
      .filter(row => executeQuery(row, query, ref))
      .map(row => executeUpdate(row, data, ref))
      .length
    this.$save(table)
    return { matched }
  }

  async remove(sel: Selection.Mutable) {
    const { ref, query, table } = sel
    const data = this.table(table)
    this.#store[table] = data.filter(row => !executeQuery(row, query, ref))
    this.$save(table)
    return { removed: data.length - this.#store[table].length }
  }

  async create(sel: Selection.Mutable, data: any) {
    const { table, model } = sel
    const { primary, autoInc } = model
    const store = this.table(table)
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
    const result = { inserted: 0, matched: 0 }
    for (const update of data) {
      const row = this.table(table).find(row => {
        return keys.every(key => row[key] === update[key])
      })
      if (row) {
        executeUpdate(row, update, ref)
        result.matched++
      } else {
        const data = executeUpdate(model.create(), update, ref)
        await this.database.create(table, data).catch(noop)
        result.inserted++
      }
    }
    this.$save(table)
    return result
  }
}

export default MemoryDriver
