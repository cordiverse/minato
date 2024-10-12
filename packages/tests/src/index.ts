import { Context, ForkScope } from 'cordis'
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
import './setup'

export { expect } from 'chai'

const Keywords = ['name']
type Keywords = 'name'

type UnitOptions<T> = (T extends (database: Database, options?: infer R) => any ? R : {}) & {
  [K in keyof T as Exclude<K, Keywords>]?: false | UnitOptions<T[K]>
}

type Unit<T> = ((database: Database | ((arg: T) => Database), options: UnitOptions<T>, fork?: boolean) => void) & {
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

function createUnit<T>(target: T, level = 0): Unit<T> {
  const title = target['name']
  const test: any = (database: Database, options: any = {}) => {
    function callback() {
      let fork: ForkScope<Context> | undefined
      if (level === 1) {
        fork = database['ctx'].plugin({
          inject: ['model'],
          name: title,
          apply: () => {},
        })
        database = fork.ctx.model
      }

      if (typeof target === 'function') {
        target(database, options)
      }

      for (const key in target) {
        if (options[key] === false || Keywords.includes(key)) continue
        test[key](database, options[key])
      }

      if (fork) {
        after(async () => {
          await database.dropAll()
          fork.dispose()
        })
      }
    }

    process.argv.filter(x => x.startsWith('--+')).forEach(x => setValue(options, x.slice(3), true))
    process.argv.filter(x => x.startsWith('---')).forEach(x => setValue(options, x.slice(3), false))

    if (level && title) {
      describe(title.replace(/(?=[A-Z])/g, ' ').trimStart(), callback)
    } else {
      callback()
    }
  }

  for (const key in target) {
    if (Keywords.includes(key)) continue
    test[key] = createUnit(target[key], level + 1)
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
}

export default createUnit(Tests)
