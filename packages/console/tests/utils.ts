import { Context } from 'cordis'
import { Database, Driver, Model, Selection } from 'minato'
import ConsoleDriver, { serialize, deserialize } from '@minatojs/console'
import { defineProperty, mapValues } from 'cosmokit'

export function setup() {
  const ctx = new Context()
  const ctx2 = new Context()
  ctx.plugin(Database)
  ctx2.plugin(Database)
  const database = ctx.model
  const database2 = ctx2.model
  // @ts-expect-error
  database2.prepareTasks = database.prepareTasks

  const database3 = new Proxy(database2, {
    get(target, key, receiver) {
      if (key === 'extend') {
        return (...args: any[]) => {
          // @ts-ignore
          database.extend(...args)
          database2.tables = mapValues(database.tables, (table) => defineProperty(Object.assign(new Model(table.name), table), 'ctx', ctx2)) as any
        }
      } else if (key === 'define') {
        return database.define.bind(database)
      }
      return Reflect.get(target, key, receiver)
    },
  })

  return [database, database3]
}

export async function prepare(database: Database, database3: Database) {
  const sessions: Record<string, [Database, () => void, () => void, Promise<void>]> = {}

  await database3.connect(ConsoleDriver, {
    send: async (action, session, table, ...args) => {
      if (action === 'stats') {
        return serialize(await database.stats())
      } else if (action === 'dropAll') {
        return await database.dropAll()
      } else if (action === 'transaction/begin') {
        const task = database.withTransaction(async (db) => {
          return new Promise((resolve, reject) => {
            sessions[session] = [db, resolve, reject, task]
          })
        })
        return
      } else if (action === 'transaction/commit') {
        const [_, resolve, reject, task] = sessions[session]
        resolve()
        await task
        delete sessions[session]
        return
      } else if (action === 'transaction/rollback') {
        const [_, resolve, reject, task] = sessions[session]
        reject()
        await task
        delete sessions[session]
        return
      }
      let callargs: any[] = args.map(deserialize)
      const db = (session && sessions[session]?.[0]) ?? database
      // @ts-expect-error
      const driver: Driver = db.getDriver(table)
      await driver.database.prepared()
      await driver._ensureSession()

      if (callargs[0] && Selection.is(callargs[0])) {
        callargs[0] = Selection.retrieve(callargs[0], driver)
        callargs = [callargs[0], ...callargs[0].args]
      }
      const result = await (driver[action as any] as any)(...callargs)
      return result && serialize(result)
    }
  })
}
