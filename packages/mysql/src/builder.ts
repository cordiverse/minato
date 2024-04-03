import { Builder, escapeId, isBracketed } from '@minatojs/sql-utils'
import { Dict, isNullable, Time } from 'cosmokit'
import { Driver, Field, isEvalExpr, isUint8Array, Model, randomId, Selection, Type, Uint8ArrayFromBase64, Uint8ArrayToBase64, Uint8ArrayToHex } from 'minato'

export interface Compat {
  maria?: boolean
  maria105?: boolean
  mysql57?: boolean
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

  prequeries: string[] = []

  constructor(protected driver: Driver, tables?: Dict<Model>, private compat: Compat = {}) {
    super(driver, tables)

    this.evalOperators.$sum = (expr) => this.createAggr(expr, value => `ifnull(sum(${value}), 0)`, undefined, value => `ifnull(minato_cfunc_sum(${value}), 0)`)
    this.evalOperators.$avg = (expr) => this.createAggr(expr, value => `avg(${value})`, undefined, value => `minato_cfunc_avg(${value})`)
    this.evalOperators.$min = (expr) => this.createAggr(expr, value => `min(${value})`, undefined, value => `minato_cfunc_min(${value})`)
    this.evalOperators.$max = (expr) => this.createAggr(expr, value => `max(${value})`, undefined, value => `minato_cfunc_max(${value})`)

    this.transformers['boolean'] = {
      encode: value => `if(${value}=b'1', 1, 0)`,
      decode: value => `if(${value}=1, b'1', b'0')`,
      load: value => isNullable(value) ? value : !!value,
      dump: value => isNullable(value) ? value : value ? 1 : 0,
    }

    this.transformers['binary'] = {
      encode: value => `to_base64(${value})`,
      decode: value => `from_base64(${value})`,
      load: value => isNullable(value) ? value : Uint8ArrayFromBase64(value),
      dump: value => isNullable(value) ? value : Uint8ArrayToBase64(value),
    }

    this.transformers['date'] = {
      encode: value => value,
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
      encode: value => value,
      decode: value => `cast(${value} as time)`,
      load: value => this.driver.types['time'].load(value),
      dump: value => isNullable(value) ? value : Time.template('yyyy-MM-dd hh:mm:ss.SSS', value),
    }

    this.transformers['timestamp'] = {
      encode: value => value,
      decode: value => `cast(${value} as datetime)`,
      load: value => {
        if (isNullable(value) || typeof value === 'object') return value
        return new Date(value)
      },
      dump: value => isNullable(value) ? value : Time.template('yyyy-MM-dd hh:mm:ss.SSS', value),
    }
  }

  escapePrimitive(value: any, type?: Type) {
    if (value instanceof Date) {
      value = Time.template('yyyy-MM-dd hh:mm:ss.SSS', value)
    } else if (value instanceof RegExp) {
      value = value.source
    } else if (isUint8Array(value)) {
      return `X'${Uint8ArrayToHex(value)}'`
    } else if (!!value && typeof value === 'object') {
      return `json_extract(${this.quote(JSON.stringify(value))}, '$')`
    }
    return super.escapePrimitive(value, type)
  }

  protected encode(value: string, encoded: boolean, pure: boolean = false, type?: Type) {
    return this.asEncoded(encoded === this.isEncoded() && !pure ? value : encoded
      ? (this.compat.maria ? `json_extract(json_object('v', ${this.transform(value, type, 'encode')}), '$.v')`
        : `cast(${this.transform(value, type, 'encode')} as json)`)
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

  protected parseSelection(sel: Selection) {
    if (!this.compat.maria && !this.compat.mysql57) return super.parseSelection(sel)
    const { args: [expr], ref, table, tables } = sel
    const restore = this.saveState({ wrappedSubquery: true, tables })
    const inner = this.get(table as Selection, true, true) as string
    const output = this.parseEval(expr, false)
    const refFields = this.state.refFields
    restore()
    let query: string
    if (!(sel.args[0] as any).$) {
      query = `(SELECT ${output} AS value FROM ${inner} ${isBracketed(inner) ? ref : ''})`
    } else {
      query = `(ifnull((SELECT ${this.groupArray(this.transform(output, Type.getInner(Type.fromTerm(expr)), 'encode'))}
        AS value FROM ${inner} ${isBracketed(inner) ? ref : ''}), json_array()))`
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
