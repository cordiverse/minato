// modified from sqlstring
// https://github.com/mysqljs/sqlstring/blob/master/lib/SqlString.js

import { Field } from '@minatojs/core'
import { isNullable, Time } from 'cosmokit'

const ESCAPE_CHARS_MAP = {
  '\0' : '\\0',
  '\b' : '\\b',
  '\t' : '\\t',
  '\n' : '\\n',
  '\r' : '\\r',
  '\x1a' : '\\Z',
  '"' : '\\"',
  '\'' : '\\\'',
  '\\' : '\\\\',
}

const ESCAPE_CHARS_REGEXP = new RegExp(`[${Object.values(ESCAPE_CHARS_MAP).join('')}]`, 'g')

export function escapeId(value: string) {
  return '`' + value + '`'
}

export function stringify(value: any, field?: Field) {
  if (isNullable(value)) return value
  if (typeof value !== 'object') return value
  if (Object.prototype.toString.call(value) === '[object Date]') {
    return Time.template('yyyy-MM-dd hh:mm:ss', value)
  } else if (Array.isArray(value) && field?.type === 'list') {
    return value.join(',')
  } else {
    return JSON.stringify(value)
  }
}

export function escape(value: any, field?: Field) {
  if (isNullable(value)) return 'NULL'

  switch (typeof value) {
    case 'boolean':
    case 'number':
      return value + ''
    case 'object':
      return escape(stringify(value, field))
    default:
      return escapeString(value)
  }
}

export function escapeString(value: string) {
  let chunkIndex = ESCAPE_CHARS_REGEXP.lastIndex = 0
  let escapedVal = ''
  let match: RegExpExecArray

  while ((match = ESCAPE_CHARS_REGEXP.exec(value))) {
    escapedVal += value.slice(chunkIndex, match.index) + ESCAPE_CHARS_MAP[match[0]]
    chunkIndex = ESCAPE_CHARS_REGEXP.lastIndex
  }

  if (chunkIndex === 0) {
    return "'" + value + "'"
  }

  if (chunkIndex < value.length) {
    return "'" + escapedVal + value.slice(chunkIndex) + "'"
  }

  return "'" + escapedVal + "'"
}
