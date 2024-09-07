import { Driver, Schema, Selection } from 'minato'
import { mapValues } from 'cosmokit'
import { deserialize, serialize } from './utils'
export { serialize, deserialize }

// @ts-expect-error
export class ConsoleDriver extends Driver<ConsoleDriver.Config> {
  static name = 'console'
  session?: string
  send!: (action: string, session: string, table: string, ...args: any[]) => Promise<any>

  _counter: number = 0

  async prepare(name: string) {}

  async start() {
    this.send = this.config.send!

    const methods = ['create', 'eval', 'get', 'remove', 'set', 'upsert', 'drop', 'dropAll', 'stats', 'getIndexes', 'createIndex', 'dropIndex'] as const
    for (const method of methods) {
      this[method] = async function (...args: any[]) {
        const arg = args.shift() ?? ''
        const table = typeof arg === 'string' ? arg : getTable(arg)
        if (Selection.is(arg)) arg.tables = mapValues(arg.tables, _ => ({} as any))
        const result = await this.send(method, this.session, table, serialize(arg), ...Selection.is(arg) ? [] : args.map(serialize))
        return result && deserialize(result)
      }
    }
  }

  async stop() {}

  async withTransaction(callback: (session?: any) => Promise<void>): Promise<void> {
    const session = `_tx_${this._counter++}`
    await this.send('transaction/begin', session, '')
    try {
      await callback(session)
      await this.send('transaction/commit', session, '')
    } catch (e) {
      await this.send('transaction/rollback', session, '')
    }
  }

  async prepareIndexes(table: string) {}
}

const getTable = (sel: Selection.Immutable | Selection.Mutable) => {
  return typeof sel.table === 'string' ? sel.table : sel.table.table ? getTable(sel.table as Selection) : getTable(Object.values(sel.table)[0])
}

export namespace ConsoleDriver {
  export interface Config {
    send?: (action: string, session: string, table: string, ...args: any[]) => Promise<any>
  }

  export const Config: Schema<Config> = Schema.object({})
}

export default ConsoleDriver
