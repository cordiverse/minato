import { Builder, escapeId } from '@minatojs/sql-utils'
import { Dict, isNullable } from 'cosmokit'
import { Field, Model, randomId, Typed } from 'minato'

export class SQLiteBuilder extends Builder {
  protected escapeMap = {
    "'": "''",
  }

  constructor(tables?: Dict<Model>) {
    super(tables)

    this.evalOperators.$if = (args) => `iif(${args.map(arg => this.parseEval(arg)).join(', ')})`
    this.evalOperators.$concat = (args) => `(${args.map(arg => this.parseEval(arg)).join('||')})`
    this.evalOperators.$modulo = ([left, right]) => `modulo(${this.parseEval(left)}, ${this.parseEval(right)})`
    this.evalOperators.$log = ([left, right]) => isNullable(right)
      ? `log(${this.parseEval(left)})`
      : `log(${this.parseEval(left)}) / log(${this.parseEval(right)})`
    this.evalOperators.$length = (expr) => this.createAggr(expr, value => `count(${value})`, value => {
      if (this.state.sqlType === 'json') {
        this.state.sqlType = 'raw'
        return `${this.jsonLength(value)}`
      } else {
        this.state.sqlType = 'raw'
        return `iif(${value}, LENGTH(${value}) - LENGTH(REPLACE(${value}, ${this.escape(',')}, ${this.escape('')})) + 1, 0)`
      }
    })
    this.evalOperators.$number = (arg) => {
      const typed = Typed.transform(arg)
      const value = this.parseEval(arg)
      const res = Field.date.includes(typed.field!) ? `cast(${value} / 1000 as integer)` : `cast(${this.parseEval(arg)} as double)`
      this.state.sqlType = 'raw'
      return `ifnull(${res}, 0)`
    }

    this.define<boolean, number>({
      types: ['boolean'],
      dump: value => +value,
      load: (value) => !!value,
    })

    this.define<object, string>({
      types: ['json'],
      dump: value => JSON.stringify(value),
      load: (value, initial) => value ? JSON.parse(value) : initial,
    })

    this.define<string[], string>({
      types: ['list'],
      dump: value => Array.isArray(value) ? value.join(',') : value,
      load: (value) => value ? value.split(',') : [],
    })

    this.define<Date, number>({
      types: ['date', 'time', 'timestamp'],
      dump: value => value === null ? null : +new Date(value),
      load: (value) => value === null ? null : new Date(value),
    })
  }

  escape(value: any, field?: Field) {
    if (value instanceof Date) value = +value
    else if (value instanceof RegExp) value = value.source
    return super.escape(value, field)
  }

  protected createElementQuery(key: string, value: any) {
    if (this.isJsonQuery(key)) {
      return this.jsonContains(key, this.quote(JSON.stringify(value)))
    } else {
      return `(',' || ${key} || ',') LIKE ${this.escape('%,' + value + ',%')}`
    }
  }

  protected jsonLength(value: string) {
    return `json_array_length(${value})`
  }

  protected jsonContains(obj: string, value: string) {
    return `json_array_contains(${obj}, ${value})`
  }

  protected jsonUnquote(value: string, pure: boolean = false) {
    return value
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
    const res = this.state.sqlType === 'json' ? `('[' || group_concat(${value}) || ']')` : `('[' || group_concat(json_quote(${value})) || ']')`
    this.state.sqlType = 'json'
    return `ifnull(${res}, json_array())`
  }

  protected transformJsonField(obj: string, path: string) {
    this.state.sqlType = 'raw'
    return `json_extract(${obj}, '$${path}')`
  }
}
