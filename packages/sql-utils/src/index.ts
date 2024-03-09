import { Dict, isNullable } from 'cosmokit'
import { Driver, Eval, Field, isComparable, isEvalExpr, Model, Modifier, Query, randomId, Selection, Typed } from 'minato'

export function escapeId(value: string) {
  return '`' + value + '`'
}

export function isBracketed(value: string) {
  return value.startsWith('(') && value.endsWith(')')
}

export function isSqlJson(typed?: Typed) {
  return typed ? (typed.field === 'json' || !!typed.inner) : false
}

export type QueryOperators = {
  [K in keyof Query.FieldExpr]?: (key: string, value: NonNullable<Query.FieldExpr[K]>) => string
}

export type ExtractUnary<T> = T extends [infer U] ? U : T

export type EvalOperators = {
  [K in keyof Eval.Static as `$${K}`]?: (expr: ExtractUnary<Parameters<Eval.Static[K]>>) => string
} & { $: (expr: any) => string }

interface State {
  // current table ref in get()
  table?: string

  // encode format of last evaluation
  encoded?: boolean
  encodedMap?: Dict<boolean>

  group?: boolean
  tables?: Dict<Model>

  // joined tables
  innerTables?: Dict<Model>

  // outter tables and fields within subquery
  refFields?: Dict<string>
  refTables?: Dict<Model>
  wrappedSubquery?: boolean
}

export class Builder {
  protected escapeMap = {}
  protected escapeRegExp?: RegExp
  protected createEqualQuery = this.comparator('=')
  protected queryOperators: QueryOperators
  protected evalOperators: EvalOperators
  protected state: State = {}
  protected $true = '1'
  protected $false = '0'
  protected modifiedTable?: string

  private readonly _timezone = `+${(new Date()).getTimezoneOffset() / -60}:00`.replace('+-', '-')

  constructor(protected driver: Driver, tables?: Dict<Model>) {
    this.state.tables = tables

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
        if (this.isJsonQuery(key)) {
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
      $modulo: this.binary('%'),

      // mathemetic
      $abs: (arg) => `abs(${this.parseEval(arg)})`,
      $floor: (arg) => `floor(${this.parseEval(arg)})`,
      $ceil: (arg) => `ceil(${this.parseEval(arg)})`,
      $round: (arg) => `round(${this.parseEval(arg)})`,
      $exp: (arg) => `exp(${this.parseEval(arg)})`,
      $log: (args) => `log(${args.filter(x => !isNullable(x)).map(arg => this.parseEval(arg)).reverse().join(', ')})`,
      $power: (args) => `power(${args.map(arg => this.parseEval(arg)).join(', ')})`,
      $random: () => `rand()`,

      // string
      $concat: (args) => `concat(${args.map(arg => this.parseEval(arg)).join(', ')})`,
      $regex: ([key, value]) => `${this.parseEval(key)} regexp ${this.parseEval(value)}`,

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
      $in: ([key, value]) => this.asEncoded(this.createMemberQuery(this.parseEval(key), value, ''), false),
      $nin: ([key, value]) => this.asEncoded(this.createMemberQuery(this.parseEval(key), value, ' NOT'), false),

      // typecast
      $cast: ([value, type]) => this.escape(value, type as any),
      $number: (arg) => {
        const value = this.parseEval(arg)
        const typed = Typed.transform(arg)
        const res = typed.field === 'time' ? `unix_timestamp(convert_tz(addtime('1970-01-01 00:00:00', ${value}), '${this._timezone}', '+0:00'))`
          : ['timestamp', 'date'].includes(typed.field!) ? `unix_timestamp(convert_tz(${value}, '${this._timezone}', '+0:00'))` : `(0+${value})`
        return this.asEncoded(`ifnull(${res}, 0)`, false)
      },

      // aggregation
      $sum: (expr) => this.createAggr(expr, value => `ifnull(sum(${value}), 0)`),
      $avg: (expr) => this.createAggr(expr, value => `avg(${value})`),
      $min: (expr) => this.createAggr(expr, value => `min(${value})`),
      $max: (expr) => this.createAggr(expr, value => `max(${value})`),
      $count: (expr) => this.createAggr(expr, value => `count(distinct ${value})`),
      $length: (expr) => this.createAggr(expr, value => `count(${value})`, value => this.isEncoded() ? this.jsonLength(value)
        : this.asEncoded(`if(${value}, LENGTH(${value}) - LENGTH(REPLACE(${value}, ${this.escape(',')}, ${this.escape('')})) + 1, 0)`, false)),

      $object: (fields) => this.groupObject(fields),
      $array: (expr) => this.groupArray(this.parseEval(expr, false)),

      $exec: (sel) => this.parseSelection(sel as Selection),
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
      const res = this.jsonContains(this.parseEval(value, false), this.encode(key, true, true))
      return notStr ? this.logicalNot(res) : res
    }
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} regexp ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    if (this.isJsonQuery(key)) {
      return this.jsonContains(key, this.escape(value, 'json'))
    } else {
      return `find_in_set(${this.escape(value)}, ${key})`
    }
  }

  protected isJsonQuery(key: string) {
    return isSqlJson(this.state.tables![this.state.table!].fields![this.unescapeId(key)]?.typed)
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

  protected parseSelection(sel: Selection) {
    const { args: [expr], ref, table, tables } = sel
    const restore = this.saveState({ tables })
    const inner = this.get(table as Selection, true, true) as string
    const output = this.parseEval(expr, false)
    restore()
    if (!(sel.args[0] as any).$) {
      return `(SELECT ${output} AS value FROM ${inner} ${isBracketed(inner) ? ref : ''})`
    } else {
      return `(ifnull((SELECT ${this.groupArray(output)} AS value FROM ${inner} ${isBracketed(inner) ? ref : ''}), json_array()))`
    }
  }

  protected jsonLength(value: string) {
    return this.asEncoded(`json_length(${value})`, false)
  }

  protected jsonContains(obj: string, value: string) {
    return this.asEncoded(`json_contains(${obj}, ${value})`, false)
  }

  protected asEncoded(value: string, encoded: boolean | undefined) {
    if (encoded !== undefined) this.state.encoded = encoded
    return value
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false) {
    return this.asEncoded((encoded === this.isEncoded() && !pure) ? value : encoded
      ? `cast(${value} as json)` : `json_unquote(${value})`, pure ? undefined : encoded)
  }

  protected isEncoded(key?: string) {
    return key ? this.state.encodedMap?.[key] : this.state.encoded
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
      return this.isEncoded() ? `json_extract(${value}, '$')` : `${value}`
    }
    const res = `json_object(` + Object.entries(fields).map(([key, expr]) => `'${key}', ${parse(expr)}`).join(',') + `)`
    return this.asEncoded(res, true)
  }

  protected groupArray(value: string) {
    return this.asEncoded(`ifnull(json_arrayagg(${value}), json_array())`, true)
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
    this.state.encoded = false
    for (const key in expr) {
      if (key in this.evalOperators) {
        return this.evalOperators[key](expr[key])
      }
    }
    return this.escape(expr)
  }

  protected transformJsonField(obj: string, path: string) {
    return this.asEncoded(`json_extract(${obj}, '$${path}')`, true)
  }

  protected transformKey(key: string, fields: Field.Config, prefix: string) {
    if (key in fields || !key.includes('.')) {
      return this.asEncoded(prefix + this.escapeId(key), this.isEncoded(key) ?? isSqlJson(fields[key]?.typed))
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
    const fields = this.state.tables?.[table]?.fields || {}
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
    const prefix = this.modifiedTable ? `${this.escapeId(this.state.tables?.[table]?.name ?? this.modifiedTable)}.`
      : (!this.state.tables || table === '_' || key in fields
    // the only table must be the main table
    || (Object.keys(this.state.tables).length === 1 && table in this.state.tables) ? '' : `${this.escapeId(table)}.`)

    if (!(table in (this.state.tables || {})) && (table in (this.state.innerTables || {}))) {
      const fields = this.state.innerTables?.[table]?.fields || {}
      const res = (fields[key]?.expr) ? this.parseEvalExpr(fields[key]?.expr)
        : this.transformKey(key, fields, `${this.escapeId(table)}.`)
      return res
    }

    // field from outer selection
    if (!(table in (this.state.tables || {})) && (table in (this.state.refTables || {}))) {
      const fields = this.state.refTables?.[table]?.fields || {}
      const res = (fields[key]?.expr) ? this.parseEvalExpr(fields[key]?.expr)
        : this.transformKey(key, fields, `${this.escapeId(table)}.`)
      if (this.state.wrappedSubquery) {
        if (res in (this.state.refFields ?? {})) return this.state.refFields![res]
        const key = `minato_tvar_${randomId()}`
        ;(this.state.refFields ??= {})[res] = key
        return this.asEncoded(this.escapeId(key), true)
      } else return res
    }
    return this.transformKey(key, fields, prefix)
  }

  parseEval(expr: any, unquote: boolean = true): string {
    this.state.encoded = false
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date || expr instanceof RegExp) {
      return this.escape(expr)
    }
    return unquote ? this.encode(this.parseEvalExpr(expr), false) : this.parseEvalExpr(expr)
  }

  protected saveState(extra: Partial<State> = {}) {
    const thisState = this.state
    this.state = { refTables: { ...(this.state.refTables || {}), ...(this.state.tables || {}) }, ...extra }
    return () => {
      thisState.encoded = this.state.encoded
      this.state = thisState
    }
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

  get(sel: Selection.Immutable, inline = false, group = false, addref = true) {
    const { args, table, query, ref, model } = sel
    this.state.table = ref

    // get prefix
    let prefix: string | undefined
    if (typeof table === 'string') {
      prefix = this.escapeId(table)
    } else if (table instanceof Selection) {
      prefix = this.get(table, true)
      if (!prefix) return
    } else {
      this.state.innerTables = Object.fromEntries(Object.values(table).map(t => [t.ref, t.model]))
      const joins: string[] = Object.entries(table).map(([key, table]) => {
        const restore = this.saveState({ tables: { ...table.tables } })
        const t = `${this.get(table, true, false, false)} AS ${this.escapeId(table.ref)}`
        restore()
        return t
      })

      // the leading space is to prevent from being parsed as bracketed and added ref
      prefix = ' ' + joins[0] + joins.slice(1, -1).map(join => ` JOIN ${join} ON ${this.$true}`).join(' ') + ` JOIN ` + joins.at(-1)
      const filter = this.parseEval(args[0].having)
      prefix += ` ON ${filter}`
    }

    const filter = this.parseQuery(query)
    if (filter === this.$false) return

    this.state.group = group || !!args[0].group
    const encodedMap: Dict<boolean> = {}
    const fields = args[0].fields ?? Object.fromEntries(Object
      .entries(model.fields)
      .filter(([, field]) => !field!.deprecated)
      .map(([key, field]) => [key, field!.expr ? field!.expr : Eval('', [ref, key], Typed.fromField(field!))]))
    const keys = Object.entries(fields).map(([key, value]) => {
      value = this.parseEval(value, false)
      encodedMap![key] = this.state.encoded!
      return this.escapeId(key) === value ? this.escapeId(key) : `${value} AS ${this.escapeId(key)}`
    }).join(', ')

    // get suffix
    let suffix = this.suffix(args[0])
    this.state.encodedMap = encodedMap

    if (filter !== this.$true) {
      suffix = ` WHERE ${filter}` + suffix
    }

    if (inline && !args[0].fields && !suffix) {
      return (addref && isBracketed(prefix)) ? `${prefix} ${ref}` : prefix
    }

    if (!prefix.includes(' ') || isBracketed(prefix)) {
      suffix = ` ${ref}` + suffix
    }

    const result = `SELECT ${keys} FROM ${prefix}${suffix}`
    return inline ? `(${result})` : result
  }

  dump(model: Model, obj: any): any {
    obj = model.format(obj)
    const result = {}
    for (const key in obj) {
      const { type, typed } = model.fields[key] ?? {}
      const converter = typed?.field ? this.driver.types[typed.field] : type && this.driver.types[type]
      result[key] = converter ? converter.dump(obj[key]) : obj[key]
    }
    return result
  }

  load(model: Model, obj: any): any
  load(typed: Typed | Eval.Expr, obj: any): any
  load(model: Model | Typed | Eval.Expr, obj?: any) {
    if (Typed.isTyped(model) || isEvalExpr(model)) {
      const typed = Typed.transform(model)
      const converter = this.driver.types[typed?.field ?? (typed?.inner ? 'json' : 'raw')]
      return converter ? converter.load(obj) : obj
    }

    const result = {}
    for (const key in obj) {
      if (!(key in model.fields)) continue
      const { type, initial, typed } = model.fields[key]!
      const converter = this.isEncoded(key) ? this.driver.types['json']
        : typed?.field ? this.driver.types[typed.field] : typed?.inner ? this.driver.types['json'] : this.driver.types[type]
      result[key] = converter ? converter.load(obj[key], initial) : obj[key]
    }
    return model.parse(result)
  }

  escape(value: any, field?: Field | Field.Type) {
    let converter: Driver.Transformer | undefined
    if (typeof field === 'string') converter = this.driver.types[field]
    else {
      const { type, typed } = field ?? {}
      converter = typed?.field ? this.driver.types[typed.field] : type && this.driver.types[type]
    }

    return this.escapePrimitive(converter ? converter.dump(value) : value)
  }

  protected escapePrimitive(value: any) {
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
