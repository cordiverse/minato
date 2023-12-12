import { Dict, isNullable } from 'cosmokit'
import { Eval, Field, isComparable, Model, Modifier, Query, randomId, Selection } from '@minatojs/core'

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

type SQLType = 'raw' | 'json' | 'list' | 'date' | 'time' | 'timestamp'

interface State {
  sqlType?: SQLType
  sqlTypes?: Dict<SQLType>
  group?: boolean
}

export class Builder {
  protected escapeMap = {}
  protected escapeRegExp?: RegExp
  protected types: Dict<Transformer> = {}
  protected createEqualQuery = this.comparator('=')
  protected queryOperators: QueryOperators
  protected evalOperators: EvalOperators
  protected state: State = {}
  protected $true = '1'
  protected $false = '0'
  protected modifiedTable?: string

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
        if (this.state.sqlTypes?.[this.unescapeId(key)] === 'json') {
          return `${this.jsonLength(key)} = ${this.escape(value)}`
        } else {
          return `${key} AND LENGTH(${key}) - LENGTH(REPLACE(${key}, ${this.escape(',')}, ${this.escape('')})) = ${this.escape(value)} - 1`
        }
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

      // membership
      $in: ([key, value]) => this.createMemberQuery(this.parseEval(key), value, ''),
      $nin: ([key, value]) => this.createMemberQuery(this.parseEval(key), value, ' NOT'),

      // aggregation
      $sum: (expr) => this.createAggr(expr, value => `ifnull(sum(${value}), 0)`),
      $avg: (expr) => this.createAggr(expr, value => `avg(${value})`),
      $min: (expr) => this.createAggr(expr, value => `min(${value})`),
      $max: (expr) => this.createAggr(expr, value => `max(${value})`),
      $count: (expr) => this.createAggr(expr, value => `count(distinct ${value})`),
      $length: (expr) => this.createAggr(expr, value => `count(${value})`, value => {
        if (this.state.sqlType === 'json') {
          this.state.sqlType = 'raw'
          return `${this.jsonLength(value)}`
        } else {
          this.state.sqlType = 'raw'
          return `if(${value}, LENGTH(${value}) - LENGTH(REPLACE(${value}, ${this.escape(',')}, ${this.escape('')})) + 1, 0)`
        }
      }),

      $object: (fields) => this.groupObject(fields),
      $array: (expr) => this.groupArray(this.parseEval(expr, false)),
    }
  }

  protected unescapeId(value: string) {
    return value.slice(1, value.length - 1)
  }

  protected createNullQuery(key: string, value: boolean) {
    return `${key} is ${value ? 'not ' : ''}null`
  }

  protected createMemberQuery(key: string, value: any, notStr = '') {
    if (Array.isArray(value)) {
      if (!value.length) return notStr ? this.$true : this.$false
      return `${key}${notStr} in (${value.map(val => this.escape(val)).join(', ')})`
    } else {
      const res = this.jsonContains(this.parseEval(value, false), this.jsonQuote(key, true))
      this.state.sqlType = 'raw'
      return notStr ? this.logicalNot(res) : res
    }
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} regexp ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    if (this.state.sqlTypes?.[this.unescapeId(key)] === 'json') {
      return this.jsonContains(key, this.quote(JSON.stringify(value)))
    } else {
      return `find_in_set(${this.escape(value)}, ${key})`
    }
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
    if (!conditions.length) return this.$true
    if (conditions.includes(this.$false)) return this.$false
    return conditions.join(' AND ')
  }

  protected logicalOr(conditions: string[]) {
    if (!conditions.length) return this.$false
    if (conditions.includes(this.$true)) return this.$true
    return `(${conditions.join(' OR ')})`
  }

  protected logicalNot(condition: string) {
    return `NOT(${condition})`
  }

  protected jsonLength(value: string) {
    return `json_length(${value})`
  }

  protected jsonContains(obj: string, value: string) {
    return `json_contains(${obj}, ${value})`
  }

  protected jsonUnquote(value: string, pure: boolean = false) {
    if (pure) return `json_unquote(${value})`
    const res = this.state.sqlType === 'json' ? `json_unquote(${value})` : value
    this.state.sqlType = 'raw'
    return res
  }

  protected jsonQuote(value: string, pure: boolean = false) {
    if (pure) return `cast(${value} as json)`
    const res = this.state.sqlType === 'raw' ? `cast(${value} as json)` : value
    this.state.sqlType = 'json'
    return res
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string) {
    if (this.state.group) {
      this.state.group = false
      const value = aggr(this.parseEval(expr, false))
      this.state.group = true
      // pass through sqlType of elements for variant types
      // ok to pass json on raw since mysql can treat them properly
      return value
    } else {
      const value = this.parseEval(expr, false)
      const res = nonaggr ? nonaggr(value)
        : `(select ${aggr(`json_unquote(${this.escapeId('value')})`)} from json_table(${value}, '$[*]' columns (value json path '$')) ${randomId()})`
      return res
    }
  }

  protected groupObject(fields: any) {
    const parse = (expr) => {
      const value = this.parseEval(expr, false)
      return this.state.sqlType === 'json' ? `json_extract(${value}, '$')` : `${value}`
    }
    const res = `json_object(` + Object.entries(fields).map(([key, expr]) => `'${key}', ${parse(expr)}`).join(',') + `)`
    this.state.sqlType = 'json'
    return res
  }

  protected groupArray(value: string) {
    this.state.sqlType = 'json'
    return `ifnull(json_arrayagg(${value}), json_array())`
  }

  protected parseFieldQuery(key: string, query: Query.FieldExpr) {
    const conditions: string[] = []
    if (this.modifiedTable) key = `${this.escapeId(this.modifiedTable)}.${key}`

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

  protected parseEvalExpr(expr: any) {
    this.state.sqlType = 'raw'
    for (const key in expr) {
      if (key in this.evalOperators) {
        return this.evalOperators[key](expr[key])
      }
    }
    return this.escape(expr)
  }

  protected transformJsonField(obj: string, path: string) {
    this.state.sqlType = 'json'
    return `json_extract(${obj}, '$${path}')`
  }

  protected transformKey(key: string, fields: {}, prefix: string, fullKey: string) {
    if (key in fields || !key.includes('.')) {
      if (this.state.sqlTypes?.[key] || this.state.sqlTypes?.[fullKey]) {
        this.state.sqlType = this.state.sqlTypes[key] || this.state.sqlTypes[fullKey]
      }
      return prefix + this.escapeId(key)
    }
    const field = Object.keys(fields).find(k => key.startsWith(k + '.')) || key.split('.')[0]
    const rest = key.slice(field.length + 1).split('.')
    return this.transformJsonField(`${prefix}${this.escapeId(field)}`, rest.map(key => `.${this.escapeKey(key)}`).join(''))
  }

  protected getRecursive(args: string | string[]) {
    if (typeof args === 'string') {
      return this.getRecursive(['_', args])
    }
    const [table, key] = args
    const fields = this.tables?.[table]?.fields || {}
    const fkey = Object.keys(fields).find(field => key === field || key.startsWith(field + '.'))
    if (fkey && fields[fkey]?.expr) {
      if (key === fkey) {
        return this.parseEvalExpr(fields[fkey]?.expr)
      } else {
        const field = this.parseEvalExpr(fields[fkey]?.expr)
        const rest = key.slice(fkey.length + 1).split('.')
        return this.transformJsonField(`${field}`, rest.map(key => `.${this.escapeKey(key)}`).join(''))
      }
    }
    const prefix = this.modifiedTable ? `${this.escapeId(this.tables?.[table]?.name ?? this.modifiedTable)}.` : (!this.tables || table === '_' || key in fields
    // the only table must be the main table
    || (Object.keys(this.tables).length === 1 && table in this.tables) ? '' : `${this.escapeId(table)}.`)
    return this.transformKey(key, fields, prefix, `${table}.${key}`)
  }

  parseEval(expr: any, unquote: boolean = true): string {
    this.state.sqlType = 'raw'
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date) {
      return this.escape(expr)
    }
    return unquote ? this.jsonUnquote(this.parseEvalExpr(expr)) : this.parseEvalExpr(expr)
  }

  suffix(modifier: Modifier) {
    const { limit, offset, sort, group, having } = modifier
    let sql = ''
    if (group?.length) {
      sql += ` GROUP BY ${group.map(this.escapeId).join(', ')}`
      const filter = this.parseEval(having)
      if (filter !== this.$true) sql += ` HAVING ${filter}`
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

  get(sel: Selection.Immutable, inline = false, group = false) {
    const { args, table, query, ref, model } = sel

    // get prefix
    let prefix: string | undefined
    if (typeof table === 'string') {
      prefix = this.escapeId(table)
      this.state.sqlTypes = Object.fromEntries(Object.entries(model.fields).map(([key, field]) => {
        let sqlType: SQLType = 'raw'
        if (field!.type === 'json') sqlType = 'json'
        else if (field!.type === 'list') sqlType = 'list'
        else if (Field.date.includes(field!.type)) sqlType = field!.type as SQLType
        return [key, sqlType]
      }))
    } else if (table instanceof Selection) {
      prefix = this.get(table, true)
      if (!prefix) return
    } else {
      const sqlTypes: Dict<SQLType> = {}
      prefix = Object.entries(table).map(([key, table]) => {
        const t = `${this.get(table, true)} AS ${this.escapeId(key)}`
        for (const [fieldKey, fieldType] of Object.entries(this.state.sqlTypes!)) {
          sqlTypes[`${key}.${fieldKey}`] = fieldType
        }
        return t
      }).join(' JOIN ')
      this.state.sqlTypes = sqlTypes
      const filter = this.parseEval(args[0].having)
      prefix += ` ON ${filter}`
    }

    const filter = this.parseQuery(query)
    if (filter === this.$false) return

    this.state.group = group || !!args[0].group
    const sqlTypes: Dict<SQLType> = {}
    const fields = args[0].fields ?? Object.fromEntries(Object
      .entries(model.fields)
      .filter(([, field]) => !field!.deprecated)
      .map(([key]) => [key, { $: [ref, key] }]))
    const keys = Object.entries(fields).map(([key, value]) => {
      value = this.parseEval(value, false)
      sqlTypes[key] = this.state.sqlType!
      return this.escapeId(key) === value ? this.escapeId(key) : `${value} AS ${this.escapeId(key)}`
    }).join(', ')

    // get suffix
    let suffix = this.suffix(args[0])
    this.state.sqlTypes = sqlTypes

    if (filter !== this.$true) {
      suffix = ` WHERE ${filter}` + suffix
    }

    if (inline && !args[0].fields && !suffix) {
      return (prefix.startsWith('(') && prefix.endsWith(')')) ? `${prefix} ${ref}` : prefix
    }

    if (!prefix.includes(' ') || prefix.startsWith('(')) {
      suffix = ` ${ref}` + suffix
    }

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

  load(obj: any): any
  load(model: Model, obj: any): any
  load(model: any, obj?: any) {
    if (!obj) {
      const converter = this.types[this.state.sqlType!]
      return converter ? converter.load(model) : model
    }

    const result = {}
    for (const key in obj) {
      if (!(key in model.fields)) continue
      const { type, initial } = model.fields[key]!
      const converter = (this.state.sqlTypes?.[key] ?? 'raw') === 'raw' ? this.types[type] : this.types[this.state.sqlTypes![key]]
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

  escapeId(value: string) {
    return escapeId(value)
  }

  escapeKey(value: string) {
    return `"${value}"`
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
