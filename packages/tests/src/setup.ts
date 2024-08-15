import { config, use, util } from 'chai'
import promised from 'chai-as-promised'
import shape from './shape'
import { isNullable } from 'cosmokit'

use(shape)
use(promised)

function type(obj) {
  if (typeof obj === 'undefined') {
    return 'undefined'
  }

  if (obj === null) {
    return 'null'
  }

  const stringTag = obj[Symbol.toStringTag]
  if (typeof stringTag === 'string') {
    return stringTag
  }
  const sliceStart = 8
  const sliceEnd = -1
  return Object.prototype.toString.call(obj).slice(sliceStart, sliceEnd)
}

function getEnumerableKeys(target) {
  const keys: string[] = []
  for (const key in target) {
    keys.push(key)
  }
  return keys
}

function getEnumerableSymbols(target) {
  const keys: symbol[] = []
  const allKeys = Object.getOwnPropertySymbols(target)
  for (let i = 0; i < allKeys.length; i += 1) {
    const key = allKeys[i]
    if (Object.getOwnPropertyDescriptor(target, key)?.enumerable) {
      keys.push(key)
    }
  }
  return keys
}

config.deepEqual = (expected, actual, options) => {
  return util.eql(expected, actual, {
    comparator: (expected, actual) => {
      if (isNullable(expected) && isNullable(actual)) return true
      if (type(expected) === 'Object' && type(actual) === 'Object') {
        const keys = new Set([
          ...getEnumerableKeys(expected),
          ...getEnumerableKeys(actual),
          ...getEnumerableSymbols(expected),
          ...getEnumerableSymbols(actual),
        ])
        return [...keys].every(key => config.deepEqual!(expected[key], actual[key], options))
      }
      return null
    },
  })
}
