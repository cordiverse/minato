import { Builder, escapeId } from '@minatojs/sql-utils'
import { Binary, Dict, isNullable } from 'cosmokit'
import { Driver, Field, Model, randomId, Type } from 'minato'

export class SQLiteBuilder extends Builder {
  protected escapeMap = {
    "'": "''",
  }

  constructor(protected driver: Driver, tables?: Dict<Model>) {
    super(driver, tables)

    this.evalOperators.$if = (args) => `iif(${args.map(arg => this.parseEval(arg)).join(', ')})`
    this.evalOperators.$concat = (args) => `(${args.map(arg => this.parseEval(arg)).join('||')})`
    this.evalOperators.$modulo = ([left, right]) => `modulo(${this.parseEval(left)}, ${this.parseEval(right)})`
    this.evalOperators.$log = ([left, right]) => isNullable(right)
      ? `log(${this.parseEval(left)})`
      : `log(${this.parseEval(left)}) / log(${this.parseEval(right)})`
    this.evalOperators.$length = (expr) => this.createAggr(expr, value => `count(${value})`, value => this.isEncoded() ? this.jsonLength(value)
      : this.asEncoded(`iif(${value}, LENGTH(${value}) - LENGTH(REPLACE(${value}, ${this.escape(',')}, ${this.escape('')})) + 1, 0)`, false))
    this.evalOperators.$number = (arg) => {
      const type = Type.fromTerm(arg)
      const value = this.parseEval(arg)
      const res = Field.date.includes(type.type as any) ? `cast(${value} / 1000 as integer)` : `cast(${this.parseEval(arg)} as double)`
      return this.asEncoded(`ifnull(${res}, 0)`, false)
    }
    this.evalOperators.$bitXor = ([left, right]) => {
      const leftTerm = this.parseEval(left), rightTerm = this.parseEval(right)
      return `((${leftTerm} & ~${rightTerm}) | (~${leftTerm} & ${rightTerm}))`
    }

    this.transformers['bigint'] = {
      encode: value => `cast(${value} as text)`,
      decode: value => `cast(${value} as integer)`,
      load: value => isNullable(value) ? value : BigInt(value),
      dump: value => isNullable(value) ? value : `${value}`,
    }

    this.transformers['binary'] = {
      encode: value => `hex(${value})`,
      decode: value => `unhex(${value})`,
      load: value => isNullable(value) || typeof value === 'object' ? value : Binary.fromHex(value),
      dump: value => isNullable(value) || typeof value === 'string' ? value : Binary.toHex(value),
    }
  }

  escapePrimitive(value: any, type?: Type) {
    if (value instanceof Date) value = +value
    else if (value instanceof RegExp) value = value.source
    else if (Binary.is(value)) return `X'${Binary.toHex(value)}'`
    else if (Binary.isSource(value)) return `X'${Binary.toHex(Binary.fromSource(value))}'`
    return super.escapePrimitive(value, type)
  }

  protected createElementQuery(key: string, value: any) {
    if (this.isJsonQuery(key)) {
      return this.jsonContains(key, this.escape(value, 'json'))
    } else {
      return `(',' || ${key} || ',') LIKE ${this.escape('%,' + value + ',%')}`
    }
  }

  protected jsonLength(value: string) {
    return this.asEncoded(`json_array_length(${value})`, false)
  }

  protected jsonContains(obj: string, value: string) {
    return this.asEncoded(`json_array_contains(${obj}, ${value})`, false)
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false, type?: Type) {
    return encoded ? super.encode(value, encoded, pure, type) : value
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string) {
    if (!this.state.group && !nonaggr) {
      const value = this.parseEval(expr, false)
      return `(select ${aggr(escapeId('value'))} from json_each(${value}) ${randomId()})`
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected groupArray(value: string) {
    const res = this.isEncoded() ? `('[' || group_concat(${value}) || ']')` : `('[' || group_concat(json_quote(${value})) || ']')`
    return this.asEncoded(`ifnull(${res}, json_array())`, true)
  }

  protected transformJsonField(obj: string, path: string) {
    return this.asEncoded(`json_extract(${obj}, '$${path}')`, false)
  }
}
