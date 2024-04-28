import { Builder, isBracketed } from '@minatojs/sql-utils'
import { Binary, Dict, isNullable, Time } from 'cosmokit'
import { Driver, Field, isAggrExpr, isEvalExpr, Model, randomId, Selection, Type, unravel } from 'minato'

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
  protected escapeMap = {
    "'": "''",
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
        const type = Type.fromTerm(arg)
        const res = Field.date.includes(type.type as any) ? `extract(epoch from ${value})::bigint` : `${value}::double precision`
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
      decode: value => `(${value})::boolean`,
    }

    this.transformers['decimal'] = {
      decode: value => `(${value})::double precision`,
      load: value => isNullable(value) ? value : +value,
    }

    this.transformers['bigint'] = {
      encode: value => `cast(${value} as text)`,
      decode: value => `cast(${value} as bigint)`,
      load: value => isNullable(value) ? value : BigInt(value),
      dump: value => isNullable(value) ? value : `${value}`,
    }

    this.transformers['binary'] = {
      encode: value => `encode(${value}, 'base64')`,
      decode: value => `decode(${value}, 'base64')`,
      load: value => isNullable(value) || typeof value === 'object' ? value : Binary.fromBase64(value),
      dump: value => isNullable(value) || typeof value === 'string' ? value : Binary.toBase64(value),
    }

    this.transformers['date'] = {
      decode: value => `cast(${value} as date)`,
      load: value => {
        if (isNullable(value) || typeof value === 'object') return value
        const parsed = new Date(value), date = new Date()
        date.setFullYear(parsed.getFullYear(), parsed.getMonth(), parsed.getDate())
        date.setHours(0, 0, 0, 0)
        return date
      },
      dump: value => isNullable(value) ? value : formatTime(value),
    }

    this.transformers['time'] = {
      decode: value => `cast(${value} as time)`,
      load: value => this.driver.types['time'].load(value),
      dump: value => this.driver.types['time'].dump(value),
    }

    this.transformers['timestamp'] = {
      decode: value => `cast(${value} as timestamp)`,
      load: value => {
        if (isNullable(value) || typeof value === 'object') return value
        return new Date(value)
      },
      dump: value => isNullable(value) ? value : formatTime(value),
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
    return outtype ? this.encode(this.parseEvalExpr(expr), false, false, Type.fromTerm(expr), typeof outtype === 'string' ? outtype : undefined)
      : this.parseEvalExpr(expr)
  }

  protected createRegExpQuery(key: string, value: string | RegExp) {
    return `${key} ~ ${this.escape(typeof value === 'string' ? value : value.source)}`
  }

  protected createElementQuery(key: string, value: any) {
    if (this.isJsonQuery(key)) {
      return this.jsonContains(key, this.encode(value, true, true))
    } else {
      return `${key} && ARRAY['${value}']::TEXT[]`
    }
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string, eltype?: string) {
    if (!this.state.group && !nonaggr) {
      const value = this.parseEval(expr, false)
      return `(select ${aggr(`(${this.encode(this.escapeId('value'), false, true, undefined)})${eltype ? `::${eltype}` : ''}`)}
        from jsonb_array_elements(${value}) ${randomId()})`
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected transformJsonField(obj: string, path: string) {
    return this.asEncoded(`jsonb_extract_path(${obj}, ${path.slice(1).replaceAll('.', ',')})`, true)
  }

  protected jsonLength(value: string) {
    return this.asEncoded(`jsonb_array_length(${value})`, false)
  }

  protected jsonContains(obj: string, value: string) {
    return this.asEncoded(`(${obj} @> ${value})`, false)
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false, type?: Type, outtype?: string) {
    return this.asEncoded((encoded === this.isEncoded() && !pure) ? value
      : encoded ? `to_jsonb(${this.transform(value, type, 'encode')})`
        : this.transform(`(jsonb_build_object('v', ${value})->>'v')`, type, 'decode') + `${typeof outtype === 'string' ? `::${outtype}` : ''}`
    , pure ? undefined : encoded)
  }

  protected groupObject(_fields: any) {
    const _groupObject = (fields: any, type?: Type, prefix: string = '') => {
      const parse = (expr, key) => {
        const value = (!_fields[`${prefix}${key}`] && type && Type.getInner(type, key)?.inner)
          ? _groupObject(expr, Type.getInner(type, key), `${prefix}${key}.`)
          : this.parseEval(expr, false)
        return this.isEncoded() ? this.encode(`to_jsonb(${value})`, true) : this.transform(value, expr, 'encode')
      }
      return `jsonb_build_object(` + Object.entries(fields).map(([key, expr]) => `'${key}', ${parse(expr, key)}`).join(',') + `)`
    }
    return this.asEncoded(_groupObject(unravel(_fields), this.state.type, ''), true)
  }

  protected groupArray(value: string) {
    return this.asEncoded(`coalesce(jsonb_agg(${value}), '[]'::jsonb)`, true)
  }

  protected parseSelection(sel: Selection, inline: boolean = false) {
    const { args: [expr], ref, table, tables } = sel
    const restore = this.saveState({ tables })
    const inner = this.get(table as Selection, true, true) as string
    const output = this.parseEval(expr, false)
    restore()
    if (inline || !isAggrExpr(expr as any)) {
      return `(SELECT ${output} FROM ${inner} ${isBracketed(inner) ? ref : ''})`
    } else {
      return `(coalesce((SELECT ${this.groupArray(this.transform(output, Type.getInner(Type.fromTerm(expr)), 'encode'))}
        FROM ${inner} ${isBracketed(inner) ? ref : ''}), '[]'::jsonb))`
    }
  }

  escapeId = escapeId

  escapeKey(value: string) {
    return `'${value}'`
  }

  escapePrimitive(value: any, type?: Type) {
    if (value instanceof Date) {
      value = formatTime(value)
    } else if (value instanceof RegExp) {
      value = value.source
    } else if (Binary.is(value)) {
      return `'\\x${Binary.toHex(value)}'::bytea`
    } else if (Binary.isSource(value)) {
      return `'\\x${Binary.toHex(Binary.fromSource(value))}'::bytea`
    } else if (type?.type === 'list' && Array.isArray(value)) {
      return `ARRAY[${value.map(x => this.escape(x)).join(', ')}]::TEXT[]`
    } else if (!!value && typeof value === 'object') {
      return `${this.quote(JSON.stringify(value))}::jsonb`
    }
    return super.escapePrimitive(value, type)
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
      value = `(jsonb ${this.escape(jsonInit, 'json')} || ${value})`
    }

    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      const type = Type.getInner(field?.type, prop.slice(key.length + 1))
      let escaped: string

      const v = isEvalExpr(item[prop]) ? this.encode(this.parseEval(item[prop]), true, true, Type.fromTerm(item[prop]))
        : (escaped = this.transform(this.escape(item[prop], type), type, 'encode'), escaped.endsWith('::jsonb') ? escaped
          : escaped.startsWith(`'`) ? this.encode(`(${escaped})::text`, true, true) // not passing type to prevent duplicated transform
            : this.encode(escaped, true, true))
      value = `jsonb_set(${value}, '{${rest.map(key => `"${key}"`).join(',')}}', ${v}, true)`
    }

    if (value === valueInit) {
      return this.modifiedTable ? `${this.escapeId(this.modifiedTable)}.${escaped}` : escaped
    } else {
      return value
    }
  }
}
