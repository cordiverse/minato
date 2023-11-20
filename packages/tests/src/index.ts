import { Database } from '@minatojs/core'
import QueryOperators from './query'
import UpdateOperators from './update'
import ObjectOperations from './object'
import Migration from './migration'
import Selection from './selection'
import Json from './json'
import Transaction from './transaction'
import './setup'

const Keywords = ['name']
type Keywords = 'name'

type UnitOptions<T> = (T extends (database: Database, options?: infer R) => any ? R : {}) & {
  [K in keyof T as Exclude<K, Keywords>]?: false | UnitOptions<T[K]>
}

type Unit<T> = ((database: Database, options?: UnitOptions<T>) => void) & {
  [K in keyof T as Exclude<K, Keywords>]: Unit<T[K]>
}

function createUnit<T>(target: T, root = false): Unit<T> {
  const test: any = (database: Database, options: any = {}) => {
    function callback() {
      if (typeof target === 'function') {
        target(database, options)
      }

      for (const key in target) {
        if (options[key] === false || Keywords.includes(key)) continue
        test[key](database, options[key])
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
  export const query = QueryOperators
  export const update = UpdateOperators
  export const object = ObjectOperations
  export const selection = Selection
  export const migration = Migration
  export const json = Json
  export const transaction = Transaction
}

export default createUnit(Tests, true)
