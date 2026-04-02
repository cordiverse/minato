import { Database } from 'minato'
import ModelOperations from './model'
import QueryOperators from './query'
import UpdateOperators from './update'
import ObjectOperations from './object'
import Migration from './migration'
import Selection from './selection'
import Json from './json'
import Transaction from './transaction'
import Relation from './relation'
import Performance from './performance'
import './setup'

export { expect } from 'chai'

const Keywords = ['name']
type Keywords = 'name'

type UnitOptions<T> = (T extends (database: Database, options?: infer R) => any ? R : {}) & {
  [K in keyof T as Exclude<K, Keywords>]?: false | UnitOptions<T[K]>
}

type DatabaseLike = Database | (() => Database) | { model: Database }

type Unit<T> = ((source: DatabaseLike, options?: UnitOptions<T>, overrideOptions?: UnitOptions<T>) => void) & {
  [K in keyof T as Exclude<K, Keywords>]: Unit<T[K]>
}

function setValue(obj: any, path: string, value: any) {
  if (path.includes('.')) {
    const index = path.indexOf('.')
    setValue(obj[path.slice(0, index)] ??= {}, path.slice(index + 1), value)
  } else {
    obj[path] = value
  }
}

function resolveGetDb(source: DatabaseLike): () => Database {
  if (typeof source === 'function') return source
  if (source instanceof Database) return () => source
  return () => source.model
}

function createLazyDatabase(getDb: () => Database): Database {
  return new Proxy(Object.create(null), {
    get(_, prop) {
      const db = getDb()
      const value = (db as any)[prop]
      return typeof value === 'function' ? value.bind(db) : value
    },
  })
}

function createUnit<T>(target: T, root = false): Unit<T> {
  const test: any = (source: DatabaseLike, options: any = {}, overrideOptions?: any) => {
    const getDb = resolveGetDb(source)

    function callback() {
      if (typeof target === 'function') {
        target(createLazyDatabase(getDb), options)
      }

      for (const key in target) {
        if (overrideOptions && !overrideOptions[key]) continue
        if (options[key] === false || Keywords.includes(key)) continue
        test[key](getDb, options[key], overrideOptions?.[key] === true ? undefined : overrideOptions?.[key])
      }
    }

    if (root) {
      process.argv.filter(x => x.startsWith('--+')).forEach(x => setValue(options, x.slice(3), true))
      process.argv.filter(x => x.startsWith('---')).forEach(x => setValue(options, x.slice(3), false))
      if (process.argv.some(x => x.startsWith('--!'))) {
        overrideOptions = {}
        process.argv.filter(x => x.startsWith('--!')).forEach(x => setValue(overrideOptions, x.slice(3), true))
      }
    }

    const title = target['name']
    if (!root && title) {
      describe(title.replace(/(?=[A-Z])/g, ' ').trimStart(), callback)
    } else {
      callback()
    }
  }

  for (const key in target) {
    if (Keywords.includes(key)) continue
    test[key] = createUnit(target[key])
  }

  return test
}

namespace Tests {
  export const model = ModelOperations
  export const query = QueryOperators
  export const update = UpdateOperators
  export const object = ObjectOperations
  export const selection = Selection
  export const migration = Migration
  export const json = Json
  export const transaction = Transaction
  export const relation = Relation
  export const performance = Performance
}

export default createUnit(Tests, true)
