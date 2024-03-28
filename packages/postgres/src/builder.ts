import { Builder, isBracketed } from '@minatojs/sql-utils'
import { Dict, isNullable, Time } from 'cosmokit'
import { Driver, Field, isEvalExpr, isUint8Array, Model, randomId, Selection, Typed, Uint8ArrayFromBase64, Uint8ArrayToHex } from 'minato'

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

  constructor(protected driver: Driver, public tables?: Dict<Model>) {
    super(driver, tables)

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
        const typed = Typed.fromTerm(arg)
        const res = Field.date.includes(typed.type!) ? `extract(epoch from ${value})::bigint` : `${value}::double precision`
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

    this.transformers['boolean'] = {
      encode: value => value,
      decode: value => `(${value})::boolean`,
      load: value => value,
    }

    this.transformers['decimal'] = {
      encode: value => value,
      decode: value => `(${value})::double precision`,
      load: value => isNullable(value) ? value : +value,
    }

    this.transformers['binary'] = {
      encode: value => `encode(${value}, 'base64')`,
      decode: value => `decode(${value}, 'base64')`,
      load: value => isNullable(value) ? value : Uint8ArrayFromBase64(value),
    }

    this.transformers['date'] = {
      encode: value => value,
      decode: value => `cast(${value} as date)`,
      load: value => {
        if (!value || typeof value === 'object') return value
        const parsed = new Date(value), date = new Date()
        date.setFullYear(parsed.getFullYear(), parsed.getMonth(), parsed.getDate())
        date.setHours(0, 0, 0, 0)
        return date
      },
    }

    this.transformers['time'] = {
      encode: value => value,
      decode: value => `cast(${value} as time)`,
      load: value => this.driver.types['time'].load(value),
    }

    this.transformers['timestamp'] = {
      encode: value => value,
      decode: value => `cast(${value} as datetime)`,
      load: value => {
        if (!value || typeof value === 'object') return value
        return new Date(value)
      },
    }
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

  parseEval(expr: any, outtype: boolean | string = true): string {
    this.state.encoded = false
    if (typeof expr === 'string' || typeof expr === 'number' || typeof expr === 'boolean' || expr instanceof Date || expr instanceof RegExp) {
      return this.escape(expr)
    }
    return outtype ? `(${this.encode(this.parseEvalExpr(expr), false, false, Typed.fromTerm(expr))})${typeof outtype === 'string' ? `::${outtype}` : ''}`
      : this.parseEvalExpr(expr)
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
      return `(select ${aggr(`(${this.encode(this.escapeId('value'), false, true, undefined)})${eltype ? `::${eltype}` : ''}`)}`
        + ` from jsonb_array_elements(${value}) ${randomId()})`
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

  protected encode(value: string, encoded: boolean, pure: boolean = false, typed?: Typed) {
    const transformer = this.getTransformer(typed)
    return this.asEncoded((encoded === this.isEncoded() && !pure) ? value
      : encoded ? `to_jsonb(${transformer ? transformer.encode(value) : value})`
        : transformer ? transformer.decode(`(jsonb_build_object('v', ${value})->>'v')`)
          : `(jsonb_build_object('v', ${value})->>'v')`
    , pure ? undefined : encoded)
  }

  protected groupObject(fields: any) {
    const parse = (expr) => {
      const value = this.parseEval(expr, false)
      const transformer = this.getTransformer(expr)
      return this.isEncoded() ? this.encode(`to_jsonb(${value})`, true) : transformer ? transformer.encode(value)
        : `${value}`
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
    } else if (isUint8Array(value)) {
      return `'\\x${Uint8ArrayToHex(value)}'::bytea`
    } else if (Array.isArray(value)) {
      return `ARRAY[${value.map(x => this.escape(x)).join(', ')}]::TEXT[]`
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
