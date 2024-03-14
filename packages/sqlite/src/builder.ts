import { Builder, escapeId } from '@minatojs/sql-utils'
import { Dict, isNullable } from 'cosmokit'
import { Driver, Field, Model, randomId, Typed } from 'minato'

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
      const typed = Typed.transform(arg)
      const value = this.parseEval(arg)
      const res = Field.date.includes(typed.type!) ? `cast(${value} / 1000 as integer)` : `cast(${this.parseEval(arg)} as double)`
      return this.asEncoded(`ifnull(${res}, 0)`, false)
    }
  }

  protected escapePrimitive(value: any) {
    if (value instanceof Date) value = +value
    else if (value instanceof RegExp) value = value.source
    else if (value instanceof Buffer) return `X'${value.toString('hex')}'`
    return super.escapePrimitive(value)
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

  protected encode(value: string, encoded: boolean, pure: boolean = false) {
    return encoded ? super.encode(value, encoded, pure) : value
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
