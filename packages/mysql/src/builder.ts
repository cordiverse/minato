import { Builder, escapeId, isBracketed } from '@minatojs/sql-utils'
import { Binary, Dict, isNullable, Time } from 'cosmokit'
import { Driver, Field, isAggrExpr, isEvalExpr, Model, randomId, Selection, Type } from 'minato'

export interface Compat {
  maria?: boolean
  maria105?: boolean
  mysql57?: boolean
  timezone?: string
}

export class MySQLBuilder extends Builder {
  // eslint-disable-next-line no-control-regex
  protected escapeRegExp = /[\0\b\t\n\r\x1a'"\\]/g
  protected escapeMap = {
    '\0': '\\0',
    '\b': '\\b',
    '\t': '\\t',
    '\n': '\\n',
    '\r': '\\r',
    '\x1a': '\\Z',
    '\"': '\\\"',
    '\'': '\\\'',
    '\\': '\\\\',
  }

  readonly _localTimezone = `+${(new Date()).getTimezoneOffset() / -60}:00`.replace('+-', '-')
  readonly _dbTimezone: string

  prequeries: string[] = []

  constructor(protected driver: Driver, tables?: Dict<Model>, private compat: Compat = {}) {
    super(driver, tables)
    this._dbTimezone = compat.timezone ?? 'SYSTEM'

    this.evalOperators.$select = (args) => {
      if (compat.maria || compat.mysql57) {
        return this.asEncoded(`json_object(${args.map(arg => this.parseEval(arg, false)).flatMap((x, i) => [`${i}`, x]).join(', ')})`, true)
      } else {
        return `${args.map(arg => this.parseEval(arg, false)).join(', ')}`
      }
    }

    this.evalOperators.$sum = (expr) => this.createAggr(expr, value => `ifnull(sum(${value}), 0)`, undefined, value => `ifnull(minato_cfunc_sum(${value}), 0)`)
    this.evalOperators.$avg = (expr) => this.createAggr(expr, value => `avg(${value})`, undefined, value => `minato_cfunc_avg(${value})`)
    this.evalOperators.$min = (expr) => this.createAggr(expr, value => `min(${value})`, undefined, value => `minato_cfunc_min(${value})`)
    this.evalOperators.$max = (expr) => this.createAggr(expr, value => `max(${value})`, undefined, value => `minato_cfunc_max(${value})`)

    this.evalOperators.$number = (arg) => {
      const value = this.parseEval(arg)
      const type = Type.fromTerm(arg)
      const res = type.type === 'time' ? `unix_timestamp(convert_tz(addtime('1970-01-01 00:00:00', ${value}), '${this._localTimezone}', '${this._dbTimezone}'))`
        : ['timestamp', 'date'].includes(type.type!) ? `unix_timestamp(convert_tz(${value}, '${this._localTimezone}', '${this._dbTimezone}'))` : `(0+${value})`
      return this.asEncoded(`ifnull(${res}, 0)`, false)
    }

    this.evalOperators.$or = (args) => {
      const type = Type.fromTerm(this.state.expr, Type.Boolean)
      if (Field.boolean.includes(type.type)) return this.logicalOr(args.map(arg => this.parseEval(arg)))
      else return `cast(${args.map(arg => this.parseEval(arg)).join(' | ')} as signed)`
    }
    this.evalOperators.$and = (args) => {
      const type = Type.fromTerm(this.state.expr, Type.Boolean)
      if (Field.boolean.includes(type.type)) return this.logicalAnd(args.map(arg => this.parseEval(arg)))
      else return `cast(${args.map(arg => this.parseEval(arg)).join(' & ')} as signed)`
    }
    this.evalOperators.$not = (arg) => {
      const type = Type.fromTerm(this.state.expr, Type.Boolean)
      if (Field.boolean.includes(type.type)) return this.logicalNot(this.parseEval(arg))
      else return `cast(~(${this.parseEval(arg)}) as signed)`
    }
    this.evalOperators.$xor = (args) => {
      const type = Type.fromTerm(this.state.expr, Type.Boolean)
      if (Field.boolean.includes(type.type)) return args.map(arg => this.parseEval(arg)).reduce((prev, curr) => `(${prev} != ${curr})`)
      else return `cast(${args.map(arg => this.parseEval(arg)).join(' ^ ')} as signed)`
    }

    this.transformers['boolean'] = {
      encode: value => `if(${value}=true, 1, 0)`,
      decode: value => `if(${value}=1, true, false)`,
      load: value => isNullable(value) ? value : !!value,
      dump: value => isNullable(value) ? value : value ? 1 : 0,
    }

    this.transformers['bigint'] = {
      encode: value => `cast(${value} as char)`,
      decode: value => `cast(${value} as signed)`,
      load: value => isNullable(value) ? value : BigInt(value),
      dump: value => isNullable(value) ? value : `${value}`,
    }

    this.transformers['binary'] = {
      encode: value => `to_base64(${value})`,
      decode: value => `from_base64(${value})`,
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
      dump: value => {
        if (isNullable(value)) return value
        const date = new Date(0)
        date.setFullYear(value.getFullYear(), value.getMonth(), value.getDate())
        date.setHours(0, 0, 0, 0)
        return Time.template('yyyy-MM-dd hh:mm:ss.SSS', date)
      },
    }

    this.transformers['time'] = {
      decode: value => `cast(${value} as time)`,
      load: value => this.driver.types['time'].load(value),
      dump: value => isNullable(value) ? value : Time.template('yyyy-MM-dd hh:mm:ss.SSS', value),
    }

    this.transformers['timestamp'] = {
      decode: value => `cast(${value} as datetime)`,
      load: value => {
        if (isNullable(value) || typeof value === 'object') return value
        return new Date(value)
      },
      dump: value => isNullable(value) ? value : Time.template('yyyy-MM-dd hh:mm:ss.SSS', value),
    }
  }

  protected createMemberQuery(key: string, value: any, notStr = '') {
    if (Array.isArray(value) && Array.isArray(value[0]) && (this.compat.maria || this.compat.mysql57)) {
      const vals = `json_array(${value.map((val: any[]) => `(${this.evalOperators.$select!(val)})`).join(', ')})`
      return this.jsonContains(vals, key)
    }
    if (value.$exec && (this.compat.maria || this.compat.mysql57)) {
      const res = this.jsonContains(this.parseEval(value, false), this.encode(key, true, true))
      return notStr ? this.logicalNot(res) : res
    }
    return super.createMemberQuery(key, value, notStr)
  }

  escapePrimitive(value: any, type?: Type) {
    if (value instanceof Date) {
      value = Time.template('yyyy-MM-dd hh:mm:ss.SSS', value)
    } else if (value instanceof RegExp) {
      value = value.source
    } else if (Binary.is(value)) {
      return `X'${Binary.toHex(value)}'`
    } else if (Binary.isSource(value)) {
      return `X'${Binary.toHex(Binary.fromSource(value))}'`
    } else if (!!value && typeof value === 'object') {
      return `json_extract(${this.quote(JSON.stringify(value))}, '$')`
    }
    return super.escapePrimitive(value, type)
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false, type?: Type) {
    return this.asEncoded(encoded === this.isEncoded() && !pure ? value : encoded
      ? `json_extract(json_object('v', ${this.transform(value, type, 'encode')}), '$.v')`
      : this.transform(`json_unquote(${value})`, type, 'decode'), pure ? undefined : encoded)
  }

  protected createAggr(expr: any, aggr: (value: string) => string, nonaggr?: (value: string) => string, compat?: (value: string) => string) {
    if (!this.state.group && compat && (this.compat.mysql57 || this.compat.maria)) {
      return compat(this.parseEval(expr, false))
    } else {
      return super.createAggr(expr, aggr, nonaggr)
    }
  }

  protected groupArray(value: string) {
    if (!this.compat.maria) return super.groupArray(value)
    const res = this.isEncoded() ? `concat('[', group_concat(${value}), ']')`
      : `concat('[', group_concat(json_extract(json_object('v', ${value}), '$.v')), ']')`
    return this.asEncoded(`ifnull(${res}, json_array())`, true)
  }

  protected parseSelection(sel: Selection, inline: boolean = false) {
    if (!this.compat.maria && !this.compat.mysql57) return super.parseSelection(sel, inline)
    const { args: [expr], ref, table, tables } = sel
    const restore = this.saveState({ wrappedSubquery: true, tables })
    const inner = this.get(table as Selection, true, true) as string
    const output = this.parseEval(expr, false)
    const fields = expr['$select']?.map(x => this.getRecursive(x['$']))
    const where = fields && this.logicalAnd(fields.map(x => `(${x} is not null)`))
    const refFields = this.state.refFields
    restore()
    let query: string
    if (inline || !isAggrExpr(expr as any)) {
      query = `(SELECT ${output} FROM ${inner} ${isBracketed(inner) ? ref : ''}${where ? ` WHERE ${where}` : ''})`
    } else {
      query = [
        `(ifnull((SELECT ${this.groupArray(this.transform(output, Type.getInner(Type.fromTerm(expr)), 'encode'))}`,
        `FROM ${inner} ${isBracketed(inner) ? ref : ''}), json_array()))`,
      ].join(' ')
    }
    if (Object.keys(refFields ?? {}).length) {
      const funcname = `minato_tfunc_${randomId()}`
      const decls = Object.values(refFields ?? {}).map(x => `${x} JSON`).join(',')
      const args = Object.keys(refFields ?? {}).map(x => this.state.refFields?.[x] ?? x).map(x => this.encode(x, true, true)).join(',')
      query = this.isEncoded() ? `ifnull(${query}, json_array())` : this.encode(query, true)
      this.prequeries.push(`DROP FUNCTION IF EXISTS ${funcname}`)
      this.prequeries.push(`CREATE FUNCTION ${funcname} (${decls}) RETURNS JSON DETERMINISTIC RETURN ${query}`)
      return this.asEncoded(`${funcname}(${args})`, true)
    } else return query
  }

  toUpdateExpr(item: any, key: string, field?: Field, upsert?: boolean) {
    const escaped = escapeId(key)

    // update directly
    if (key in item) {
      if (!isEvalExpr(item[key]) && upsert) {
        return `VALUES(${escaped})`
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
      rest.slice(0, -1).reduce((obj, k) => obj[k] ??= {}, jsonInit)
    }

    // update with json_set
    const valueInit = `ifnull(${escaped}, '{}')`
    let value = valueInit

    // json_set cannot create deeply nested property when non-exist
    // therefore we merge a layout to it
    if (Object.keys(jsonInit).length !== 0) {
      value = `json_merge_patch(${this.escape(jsonInit, 'json')}, ${value})`
    }

    for (const prop in item) {
      if (!prop.startsWith(key + '.')) continue
      const rest = prop.slice(key.length + 1).split('.')
      const type = Type.getInner(field?.type, prop.slice(key.length + 1))
      const v = isEvalExpr(item[prop]) ? this.transform(this.parseEval(item[prop]), item[prop], 'encode')
        : this.transform(this.escape(item[prop], type), type, 'encode')
      value = `json_set(${value}, '$${rest.map(key => `."${key}"`).join('')}', ${v})`
    }

    if (value === valueInit) {
      return escaped
    } else {
      return value
    }
  }
}
