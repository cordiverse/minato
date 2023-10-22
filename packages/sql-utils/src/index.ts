import { Dict, isNullable } from 'cosmokit'
import { Eval, Field, isComparable, Model, Modifier, Query, Selection } from '@minatojs/core'

export function escapeId(value: string) {
  return '`' + value + '`'
}

export type QueryOperators = {
  [K in keyof Query.FieldExpr]?: (key: string, value: NonNullable<Query.FieldExpr[K]>) => string
}

export type ExtractUnary<T> = T extends [infer U] ? U : T

export type EvalOperators = {
  [K in keyof Eval.Static as `$${K}`]?: (expr: ExtractUnary<Parameters<Eval.Static[K]>>) => string
} & { $: (expr: any) => string }

export interface Transformer<S = any, T = any> {
  types: Field.Type<S>[]
  dump: (value: S) => T | null
  load: (value: T, initial?: S) => S | null
}

export class Builder {
  protected escapeMap = {}
  protected escapeRegExp?: RegExp
  protected types: Dict<Transformer> = {}
  protected createEqualQuery = this.comparator('=')
  protected queryOperators: QueryOperators
  protected evalOperators: EvalOperators

  constructor(public tables?: Dict<Model>) {
    this.queryOperators = {
      // logical
      $or: (key, value) => this.logicalOr(value.map(value => this.parseFieldQuery(key, value))),
      $and: (key, value) => this.logicalAnd(value.map(value => this.parseFieldQuery(key, value))),
      $not: (key, value) => this.logicalNot(this.parseFieldQuery(key, value)),

      // existence
      $exists: (key, value) => this.createNullQuery(key, value),

      // comparison
      $eq: this.createEqualQuery,
      $ne: this.comparator('!='),
      $gt: this.comparator('>'),
      $gte: this.comparator('>='),
      $lt: this.comparator('<'),
      $lte: this.comparator('<='),

      // membership
      $in: (key, value) => this.createMemberQuery(key, value, ''),
      $nin: (key, value) => this.createMemberQuery(key, value, ' NOT'),

      // regexp
      $regex: (key, value) => this.createRegExpQuery(key, value),
      $regexFor: (key, value) => `${this.escape(value)} regexp ${key}`,

      // bitwise
      $bitsAllSet: (key, value) => `${key} & ${this.escape(value)} = ${this.escape(value)}`,
      $bitsAllClear: (key, value) => `${key} & ${this.escape(value)} = 0`,
      $bitsAnySet: (key, value) => `${key} & ${this.escape(value)} != 0`,
      $bitsAnyClear: (key, value) => `${key} & ${this.escape(value)} != ${this.escape(value)}`,

      // list
      $el: (key, value) => {
        if (Array.isArray(value)) {
          return this.logicalOr(value.map(value => this.createElementQuery(key, value)))
        } else if (typeof value !== 'number' && typeof value !== 'string') {
          throw new TypeError('query expr under $el is not supported')
        } else {
          return this.createElementQuery(key, value)
        }
      },
      $size: (key, value) => {
        if (!value) return this.logicalNot(key)
        return `${key} AND LENGTH(${key}) - LENGTH(REPLACE(${key}, ${this.escape(',')}, ${this.escape('')})) = ${this.escape(value)} - 1`
      },
    }

    this.evalOperators = {
      // universal
      $: (key) => this.getRecursive(key),
      $if: (args) => `if(${args.map(arg => this.parseEval(arg)).join(', ')})`,
      $ifNull: (args) => `ifnull(${args.map(arg => this.parseEval(arg)).join(', ')})`,

      // number
      $add: (args) => `(${args.map(arg => this.parseEval(arg)).join(' + ')})`,
      $multiply: (args) => `(${args.map(arg => this.parseEval(arg)).join(' * ')})`,
      $subtract: this.binary('-'),
      $divide: this.binary('/'),

      // string
      $concat: (args) => `concat(${args.map(arg => this.parseEval(arg)).join(', ')})`,

      // logical
      $or: (args) => this.logicalOr(args.map(arg => this.parseEval(arg))),
      $and: (args) => this.logicalAnd(args.map(arg => this.parseEval(arg))),
      $not: (arg) => this.logicalNot(this.parseEval(arg)),

      // boolean
      $eq: this.binary('='),
      $ne: this.binary('!='),
      $gt: this.binary('>'),
      $gte: this.binary('>='),
      $lt: this.binary('<'),
      $lte: this.binary('<='),

      // aggregation
      $sum: (expr) => `ifnull(sum(${this.parseAggr(expr)}), 0)`,
      $avg: (expr) => `avg(${this.parseAggr(expr)})`,
      $min: (expr) => `min(${this.parseAggr(expr)})`,
      $max: (expr) => `max(${this.parseAggr(expr)})`,
      $count: (expr) => `count(distinct ${this.parseAggr(expr)})`,
    }
  }

  protected createNullQuery(key: string, value: boolean) {
    return `${key} is ${value ? 'not ' : ''}null`
  }

  protected createMemberQuery(key: string, value: any[], notStr = '') {
    if (!value.length) return notStr ? '1' : '0'
    return `${key}${notStr} in (${value.map(val => this.escape(val)).join(', ')})`
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} regexp ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    return `find_in_set(${this.escape(value)}, ${key})`
  }

  protected comparator(operator: string) {
    return (key: string, value: any) => {
      return `${key} ${operator} ${this.escape(value)}`
    }
  }

  protected binary(operator: string) {
    return ([left, right]) => {
      return `(${this.parseEval(left)} ${operator} ${this.parseEval(right)})`
    }
  }

  protected logicalAnd(conditions: string[]) {
    if (!conditions.length) return '1'
    if (conditions.includes('0')) return '0'
    return conditions.join(' AND ')
  }

  protected logicalOr(conditions: string[]) {
    if (!conditions.length) return '0'
    if (conditions.includes('1')) return '1'
    return `(${conditions.join(' OR ')})`
  }

  protected logicalNot(condition: string) {
    return `NOT(${condition})`
  }

  protected parseFieldQuery(key: string, query: Query.FieldExpr) {
    const conditions: string[] = []

    // query shorthand
    if (Array.isArray(query)) {
      conditions.push(this.createMemberQuery(key, query))
    } else if (query instanceof RegExp) {
      conditions.push(this.createRegExpQuery(key, query))
    } else if (isComparable(query)) {
      conditions.push(this.createEqualQuery(key, query))
    } else if (isNullable(query)) {
      conditions.push(this.createNullQuery(key, false))
    } else {
      // query expression
      for (const prop in query) {
        if (prop in this.queryOperators) {
          conditions.push(this.queryOperators[prop](key, query[prop]))
        }
      }
    }

    return this.logicalAnd(conditions)
  }

  parseQuery(query: Query.Expr) {
    const conditions: string[] = []
    for (const key in query) {
      // logical expression
      if (key === '$not') {
        conditions.push(this.logicalNot(this.parseQuery(query.$not!)))
      } else if (key === '$and') {
        conditions.push(this.logicalAnd(query.$and!.map(this.parseQuery.bind(this))))
      } else if (key === '$or') {
        conditions.push(this.logicalOr(query.$or!.map(this.parseQuery.bind(this))))
      } else if (key === '$expr') {
        conditions.push(this.parseEval(query.$expr))
      } else {
        conditions.push(this.parseFieldQuery(this.escapeId(key), query[key]))
      }
    }

    return this.logicalAnd(conditions)
  }

  private parseEvalExpr(expr: any) {
    for (const key in expr) {
      if (key in this.evalOperators) {
        return this.evalOperators[key](expr[key])
      }
    }
    return this.escape(expr)
  }

  private parseAggr(expr: any) {
    if (typeof expr === 'string') {
      return this.getRecursive(expr)
    }
    return this.parseEvalExpr(expr)
  }

  private transformKey(key: string, fields: {}, prefix: string) {
    if (key in fields || !key.includes('.')) return prefix + this.escapeId(key)
    const field = Object.keys(fields).find(k => key.startsWith(k + '.')) || key.split('.')[0]
    const rest = key.slice(field.length + 1).split('.')
    return `json_unquote(json_extract(${prefix} ${this.escapeId(field)}, '$${rest.map(key => `."${key}"`).join('')}'))`
  }

  private getRecursive(args: string | string[]) {
    if (typeof args === 'string') {
      return this.getRecursive(['_', args])
    }
    const [table, key] = args
    const fields = this.tables?.[table]?.fields || {}
    if (fields[key]?.expr) {
      return this.parseEvalExpr(fields[key]?.expr)
    }
    const prefix = !this.tables || table === '_' || key in fields
    // the only table must be the main table
    || (Object.keys(this.tables).length === 1 && table in this.tables) ? '' : `${this.escapeId(table)}.`
    return this.transformKey(key, fields, prefix)
  }

  escapeId(value: string) {
    return escapeId(value)
  }

  parseEval(expr: any): string {
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date) {
      return this.escape(expr)
    }
    return this.parseEvalExpr(expr)
  }

  suffix(modifier: Modifier) {
    const { limit, offset, sort, group, having } = modifier
    let sql = ''
    if (group.length) {
      sql += ` GROUP BY ${group.map(this.escapeId).join(', ')}`
      const filter = this.parseEval(having)
      if (filter !== '1') sql += ` HAVING ${filter}`
    }
    if (sort.length) {
      sql += ' ORDER BY ' + sort.map(([expr, dir]) => {
        return `${this.parseEval(expr)} ${dir.toUpperCase()}`
      }).join(', ')
    }
    if (limit < Infinity) sql += ' LIMIT ' + limit
    if (offset > 0) sql += ' OFFSET ' + offset
    return sql
  }

  get(sel: Selection.Immutable, inline = false) {
    const { args, table, query, ref, model } = sel
    const filter = this.parseQuery(query)
    if (filter === '0') return

    // get prefix
    const fields = args[0].fields ?? Object.fromEntries(Object
      .entries(model.fields)
      .filter(([, field]) => !field!.deprecated)
      .map(([key]) => [key, { $: [ref, key] }]))
    const keys = Object.entries(fields).map(([key, value]) => {
      key = this.escapeId(key)
      value = this.parseEval(value)
      return key === value ? key : `${value} AS ${key}`
    }).join(', ')
    let prefix: string | undefined
    if (typeof table === 'string') {
      prefix = this.escapeId(table)
    } else if (table instanceof Selection) {
      prefix = this.get(table, true)
      if (!prefix) return
    } else {
      prefix = Object.entries(table).map(([key, table]) => {
        if (typeof table !== 'string') {
          return `${this.get(table, true)} AS ${this.escapeId(key)}`
        } else {
          return key === table ? this.escapeId(table) : `${this.escapeId(table)} AS ${this.escapeId(key)}`
        }
      }).join(' JOIN ')
      const filter = this.parseEval(args[0].having)
      if (filter !== '1') prefix += ` ON ${filter}`
    }

    // get suffix
    let suffix = this.suffix(args[0])
    if (filter !== '1') {
      suffix = ` WHERE ${filter}` + suffix
    }
    if (!prefix.includes(' ') || prefix.startsWith('(')) {
      suffix = ` ${ref}` + suffix
    }

    if (inline && !args[0].fields && !suffix) return prefix
    const result = `SELECT ${keys} FROM ${prefix}${suffix}`
    return inline ? `(${result})` : result
  }

  define<S, T>(converter: Transformer<S, T>) {
    converter.types.forEach(type => this.types[type] = converter)
  }

  dump(model: Model, obj: any): any {
    obj = model.format(obj)
    const result = {}
    for (const key in obj) {
      result[key] = this.stringify(obj[key], model.fields[key])
    }
    return result
  }

  load(model: Model, obj: any): any {
    const result = {}
    for (const key in obj) {
      if (!(key in model.fields)) continue
      const { type, initial } = model.fields[key]!
      const converter = this.types[type]
      result[key] = converter ? converter.load(obj[key], initial) : obj[key]
    }
    return model.parse(result)
  }

  escape(value: any, field?: Field) {
    value = this.stringify(value, field)
    if (isNullable(value)) return 'NULL'

    switch (typeof value) {
      case 'boolean':
      case 'number':
        return value + ''
      case 'object':
        return this.quote(JSON.stringify(value))
      default:
        return this.quote(value)
    }
  }

  stringify(value: any, field?: Field) {
    const converter = this.types[field!?.type]
    return converter ? converter.dump(value) : value
  }

  quote(value: string) {
    this.escapeRegExp ??= new RegExp(`[${Object.values(this.escapeMap).join('')}]`, 'g')
    let chunkIndex = this.escapeRegExp.lastIndex = 0
    let escapedVal = ''
    let match: RegExpExecArray | null

    while ((match = this.escapeRegExp.exec(value))) {
      escapedVal += value.slice(chunkIndex, match.index) + this.escapeMap[match[0]]
      chunkIndex = this.escapeRegExp.lastIndex
    }

    if (chunkIndex === 0) {
      return "'" + value + "'"
    }

    if (chunkIndex < value.length) {
      return "'" + escapedVal + value.slice(chunkIndex) + "'"
    }

    return "'" + escapedVal + "'"
  }
}
