// modified from sqlstring
// https://github.com/mysqljs/sqlstring/blob/master/lib/SqlString.js

import { isNullable } from 'cosmokit'

const ESCAPE_CHARS_MAP = {
  '\0' : '\\0',
  '\b' : '\\b',
  '\t' : '\\t',
  '\n' : '\\n',
  '\r' : '\\r',
  '\x1a' : '\\Z',
  '\'' : '\\\'',
  '\\' : '\\\\',
}

const ESCAPE_CHARS_REGEXP = new RegExp(`[${Object.values(ESCAPE_CHARS_MAP).join('')}]`, 'g')

export function escapeId(value: string) {
  return '`' + value + '`'
}

export function escape(value: any) {
  if (isNullable(value)) return 'NULL'

  switch (typeof value) {
    case 'boolean':
    case 'number':
      return value + ''
    case 'object':
      return quote(JSON.stringify(value))
    default:
      return quote(value)
  }
}

export function quote(value: string) {
  let chunkIndex = ESCAPE_CHARS_REGEXP.lastIndex = 0
  let escapedVal = ''
  let match: RegExpExecArray | null

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
