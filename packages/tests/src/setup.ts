import chai, { use } from 'chai'
import promised from 'chai-as-promised'
import shape from './shape'
import { isNullable } from 'cosmokit'

use(shape)
use(promised)

function type(obj) {
  if (typeof obj === 'undefined') {
    return 'undefined';
  }

  if (obj === null) {
    return 'null';
  }

  const stringTag = obj[Symbol.toStringTag];
  if (typeof stringTag === 'string') {
    return stringTag;
  }
  const sliceStart = 8;
  const sliceEnd = -1;
  return Object.prototype.toString.call(obj).slice(sliceStart, sliceEnd);
}

function getEnumerableKeys(target) {
  var keys: string[] = [];
  for (var key in target) {
    keys.push(key);
  }
  return keys;
}

function getEnumerableSymbols(target) {
  var keys: symbol[] = [];
  var allKeys = Object.getOwnPropertySymbols(target);
  for (var i = 0; i < allKeys.length; i += 1) {
    var key = allKeys[i];
    if (Object.getOwnPropertyDescriptor(target, key)?.enumerable) {
      keys.push(key);
    }
  }
  return keys;
}

chai.config.deepEqual = (expected, actual, options) => {
  return chai.util.eql(expected, actual, {
    comparator: (expected, actual) => {
      if (isNullable(expected) && isNullable(actual)) return true
      if (type(expected) === 'Object' && type(actual) === 'Object') {
        const keys = new Set([
          ...getEnumerableKeys(expected),
          ...getEnumerableKeys(actual),
          ...getEnumerableSymbols(expected),
          ...getEnumerableSymbols(actual),
        ])
        return [...keys].every(key => chai.config.deepEqual!(expected[key], actual[key], options))
      }
      return null
    }
  })
}
