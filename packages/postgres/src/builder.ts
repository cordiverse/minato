import { Builder, isBracketed } from '@minatojs/sql-utils'
import { Dict, isNullable, Time } from 'cosmokit'
import { Field, isEvalExpr, Model, randomId, Selection, Typed } from 'minato'

const timeRegex = /(\d+):(\d+):(\d+)(\.(\d+))?/

export function escapeId(value: string) {
  return '"' + value.replace(/"/g, '""') + '"'
}

export function formatTime(time: Date) {
  const year = time.getFullYear().toString()
  const month = Time.toDigits(time.getMonth() + 1)
  const date = Time.toDigits(time.getDate())
  const hour = Time.toDigits(time.getHours())
  const min = Time.toDigits(time.getMinutes())
  const sec = Time.toDigits(time.getSeconds())
  const ms = Time.toDigits(time.getMilliseconds(), 3)
  let timezone = Time.toDigits(time.getTimezoneOffset() / -60)
  if (!timezone.startsWith('-')) timezone = `+${timezone}`
  return `${year}-${month}-${date} ${hour}:${min}:${sec}.${ms}${timezone}`
}

export class PostgresBuilder extends Builder {
  // eslint-disable-next-line no-control-regex
  protected escapeRegExp = /[\0\b\t\n\r\x1a'\\]/g
  protected escapeMap = {
    '\0': '\\0',
    '\b': '\\b',
    '\t': '\\t',
    '\n': '\\n',
    '\r': '\\r',
    '\x1a': '\\Z',
    '\'': '\'\'',
    '\\': '\\\\',
  }

  protected $true = 'TRUE'
  protected $false = 'FALSE'

  constructor(public tables?: Dict<Model>) {
    super(tables)

    this.queryOperators = {
      ...this.queryOperators,
      $regex: (key, value) => this.createRegExpQuery(key, value),
      $regexFor: (key, value) => `${this.escape(value)} ~ ${key}`,
      $size: (key, value) => {
        if (!value) return this.logicalNot(key)
        if (this.isJsonQuery(key)) {
          return `${this.jsonLength(key)} = ${this.escape(value)}`
        } else {
          return `${key} IS NOT NULL AND ARRAY_LENGTH(${key}, 1) = ${value}`
        }
      },
    }

    this.evalOperators = {
      ...this.evalOperators,
      $if: (args) => {
        const type = this.getLiteralType(args[1]) ?? this.getLiteralType(args[2]) ?? 'text'
        return `(SELECT CASE WHEN ${this.parseEval(args[0], 'boolean')} THEN ${this.parseEval(args[1], type)} ELSE ${this.parseEval(args[2], type)} END)`
      },
      $ifNull: (args) => {
        const type = args.map(this.getLiteralType).find(x => x) ?? 'text'
        return `coalesce(${args.map(arg => this.parseEval(arg, type)).join(', ')})`
      },

      $regex: ([key, value]) => `${this.parseEval(key)} ~ ${this.parseEval(value)}`,

      // number
      $add: (args) => `(${args.map(arg => this.parseEval(arg, 'double precision')).join(' + ')})`,
      $multiply: (args) => `(${args.map(arg => this.parseEval(arg, 'double precision')).join(' * ')})`,
      $modulo: ([left, right]) => {
        const dividend = this.parseEval(left, 'double precision'), divisor = this.parseEval(right, 'double precision')
        return `${dividend} - (${divisor} * floor(${dividend} / ${divisor}))`
      },
      $log: ([left, right]) => isNullable(right)
        ? `ln(${this.parseEval(left, 'double precision')})`
        : `ln(${this.parseEval(left, 'double precision')}) / ln(${this.parseEval(right, 'double precision')})`,
      $random: () => `random()`,

      $eq: this.binary('=', 'text'),

      $number: (arg) => {
        const value = this.parseEval(arg)
        const typed = Typed.transform(arg)
        const res = Field.date.includes(typed.field!) ? `extract(epoch from ${value})::bigint` : `${value}::double precision`
        return this.asEncoded(`coalesce(${res}, 0)`, false)
      },

      $sum: (expr) => this.createAggr(expr, value => `coalesce(sum(${value})::double precision, 0)`, undefined, 'double precision'),
      $avg: (expr) => this.createAggr(expr, value => `avg(${value})::double precision`, undefined, 'double precision'),
      $min: (expr) => this.createAggr(expr, value => `min(${value})`, undefined, 'double precision'),
      $max: (expr) => this.createAggr(expr, value => `max(${value})`, undefined, 'double precision'),
      $count: (expr) => this.createAggr(expr, value => `count(distinct ${value})::integer`),
      $length: (expr) => this.createAggr(expr, value => `count(${value})::integer`,
        value => this.isEncoded() ? this.jsonLength(value) : this.asEncoded(`COALESCE(ARRAY_LENGTH(${value}, 1), 0)`, false),
      ),

      $concat: (args) => `${args.map(arg => this.parseEval(arg, 'text')).join('||')}`,
    }

    this.define<Date, string>({
      types: ['time'],
      dump: date => date ? (typeof date === 'string' ? date : formatTime(date)) : null,
      load: str => {
        if (isNullable(str)) return str
        const date = new Date(0)
        const parsed = timeRegex.exec(str)
        if (!parsed) throw Error(`unexpected time value: ${str}`)
        date.setHours(+parsed[1], +parsed[2], +parsed[3], +(parsed[5] ?? 0))
        return date
      },
    })

    this.define<string[], any>({
      types: ['list'],
      dump: value => '{' + value.join(',') + '}',
      load: value => value,
    })
  }

  upsert(table: string) {
    this.modifiedTable = table
  }

  protected binary(operator: string, eltype: string = 'double precision') {
    return ([left, right]) => {
      const type = this.getLiteralType(left) ?? this.getLiteralType(right) ?? eltype
      return `(${this.parseEval(left, type)} ${operator} ${this.parseEval(right, type)})`
    }
  }

  private getLiteralType(expr: any) {
    if (typeof expr === 'string') return 'text'
    else if (typeof expr === 'number') return 'double precision'
    else if (typeof expr === 'string') return 'boolean'
  }

  parseEval(expr: any, outtype: boolean | string = false): string {
    this.state.encoded = false
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date || expr instanceof RegExp) {
      return this.escape(expr)
    }
    return outtype ? this.encode(this.parseEvalExpr(expr), false, false, typeof outtype === 'string' ? outtype : undefined) : this.parseEvalExpr(expr)
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} ~ ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    if (this.isJsonQuery(key)) {
      return this.jsonContains(key, this.escape(value, 'json'))
    } else {
      return `${key} && ARRAY['${value}']::TEXT[]`
    }
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string, eltype?: string) {
    if (!this.state.group && !nonaggr) {
      const value = this.parseEval(expr, false)
      return `(select ${aggr(this.encode(this.escapeId('value'), false, true, eltype))} from jsonb_array_elements(${value}) ${randomId()})`
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected transformJsonField(obj: string, path: string) {
    return this.asEncoded(`jsonb_extract_path(${obj}, ${path.slice(1).replace('.', ',')})`, true)
  }

  protected jsonLength(value: string) {
    return this.asEncoded(`jsonb_array_length(${value})`, false)
  }

  protected jsonContains(obj: string, value: string) {
    return this.asEncoded(`(${obj} @> ${value})`, false)
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false, type?: string) {
    return this.asEncoded((encoded === this.isEncoded() && !pure) ? value : encoded
      ? `to_jsonb(${value})` : `(jsonb_build_object('v', ${value})->>'v')::${type}`, pure ? undefined : encoded)
  }

  protected groupObject(fields: any) {
    const parse = (expr) => {
      const value = this.parseEval(expr, false)
      return this.encode(`to_jsonb(${value})`, true)
    }
    const res = `jsonb_build_object(` + Object.entries(fields).map(([key, expr]) => `'${key}', ${parse(expr)}`).join(',') + `)`
    return this.asEncoded(res, true)
  }

  protected groupArray(value: string) {
    return this.asEncoded(`coalesce(jsonb_agg(${value}), '[]'::jsonb)`, true)
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
      return `(coalesce((SELECT ${this.groupArray(output)} AS value FROM ${inner} ${isBracketed(inner) ? ref : ''}), '[]'::jsonb))`
    }
  }

  escapeId = escapeId

  escapeKey(value: string) {
    return `'${value}'`
  }

  protected escapePrimitive(value: any) {
    if (value instanceof Date) {
      value = formatTime(value)
    } else if (value instanceof RegExp) {
      value = value.source
    } else if (value instanceof Buffer) {
      return `'\\x${value.toString('hex')}'::bytea`
    } else if (!!value && typeof value === 'object') {
      return `${this.quote(JSON.stringify(value))}::jsonb`
    }
    return super.escapePrimitive(value)
  }

  toUpdateExpr(item: any, key: string, field?: Field, upsert?: boolean) {
    const escaped = this.escapeId(key)
    // update directly
    if (key in item) {
      if (!isEvalExpr(item[key]) && upsert) {
        return `excluded.${escaped}`
      } else if (isEvalExpr(item[key])) {
        return this.parseEval(item[key])
      } else {
        return this.escape(item[key], field)
      }
    }

    // prepare nested layout
    const jsonInit = {}
    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      if (rest.length === 1) continue
      rest.reduce((obj, k) => obj[k] ??= {}, jsonInit)
    }

    // update with json_set
    const valueInit = this.modifiedTable ? `coalesce(${this.escapeId(this.modifiedTable)}.${escaped}, '{}')::jsonb` : `coalesce(${escaped}, '{}')::jsonb`
    let value = valueInit

    // json_set cannot create deeply nested property when non-exist
    // therefore we merge a layout to it
    if (Object.keys(jsonInit).length !== 0) {
      value = `(${value} || jsonb ${this.escape(jsonInit, 'json')})`
    }

    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      value = `jsonb_set(${value}, '{${rest.map(key => `"${key}"`).join(',')}}', ${this.encode(this.parseEval(item[prop]), true, true)}, true)`
    }

    if (value === valueInit) {
      return this.modifiedTable ? `${this.escapeId(this.modifiedTable)}.${escaped}` : escaped
    } else {
      return value
    }
  }
}
