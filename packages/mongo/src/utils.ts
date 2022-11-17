import { Dict, isNullable, valueMap } from 'cosmokit'
import { Query, Selection } from '@minatojs/core'
import { Filter, FilterOperators } from 'mongodb'

function createFieldFilter(query: Query.FieldQuery, key: string) {
  const filters: Filter<any>[] = []
  const result: Filter<any> = {}
  const child = transformFieldQuery(query, key, filters)
  if (child === false) return false
  if (child !== true) result[key] = child
  if (filters.length) result.$and = filters
  if (Object.keys(result).length) return result
  return true
}

function transformFieldQuery(query: Query.FieldQuery, key: string, filters: Filter<any>[]) {
  // shorthand syntax
  if (typeof query === 'string' || typeof query === 'number' || query instanceof Date) {
    return { $eq: query }
  } else if (Array.isArray(query)) {
    if (!query.length) return false
    return { $in: query }
  } else if (query instanceof RegExp) {
    return { $regex: query }
  } else if (isNullable(query)) {
    return { $exists: false }
  }

  // query operators
  const result: FilterOperators<any> = {}
  for (const prop in query) {
    if (prop === '$and') {
      for (const item of query[prop]) {
        const child = createFieldFilter(item, key)
        if (child === false) return false
        if (child !== true) filters.push(child)
      }
    } else if (prop === '$or') {
      const $or: Filter<any>[] = []
      if (!query[prop].length) return false
      const always = query[prop].some((item) => {
        const child = createFieldFilter(item, key)
        if (typeof child === 'boolean') return child
        $or.push(child)
      })
      if (!always) filters.push({ $or })
    } else if (prop === '$not') {
      const child = createFieldFilter(query[prop], key)
      if (child === true) return false
      if (child !== false) filters.push({ $nor: [child] })
    } else if (prop === '$el') {
      const child = transformFieldQuery(query[prop], key, filters)
      if (child === false) return false
      if (child !== true) result.$elemMatch = child
    } else if (prop === '$regexFor') {
      filters.push({
        $expr: {
          $function: {
            body: function (data: string, value: string) {
              return new RegExp(data, 'i').test(value)
            }.toString(),
            args: ['$' + key, query.$regexFor],
            lang: 'js',
          },
        },
      })
    } else {
      result[prop] = query[prop]
    }
  }
  if (!Object.keys(result).length) return true
  return result
}

const aggrKeys = ['$sum', '$avg', '$min', '$max', '$count']

export class Transformer {
  private counter = 0

  constructor(public virtualKey?: string) {}

  public createKey() {
    return '_temp_' + ++this.counter
  }

  protected getActualKey(key: string) {
    return key === this.virtualKey ? '_id' : key
  }

  private transformEvalExpr(expr: any, onAggr?: (pipeline: any[]) => void) {
    if (expr.$) {
      if (typeof expr.$ === 'string') {
        return '$' + this.getActualKey(expr.$)
      } else {
        return '$' + this.getActualKey(expr.$[1])
      }
    }

    return valueMap(expr as any, (value) => {
      if (Array.isArray(value)) {
        return value.map(val => this.eval(val, onAggr))
      } else {
        return this.eval(value, onAggr)
      }
    })
  }

  private transformAggr(expr: any) {
    if (typeof expr === 'string') {
      return '$' + expr
    }

    return this.transformEvalExpr(expr)
  }

  public eval(expr: any, onAggr?: (pipeline: any[]) => void) {
    if (typeof expr === 'number' || typeof expr === 'string' || typeof expr === 'boolean') {
      return expr
    }

    for (const key of aggrKeys) {
      if (!expr[key]) continue
      const value = this.transformAggr(expr[key])
      const $ = this.createKey()
      if (key === '$count') {
        onAggr([
          { $group: { _id: value } },
          { $group: { _id: null, [$]: { $count: {} } } },
        ])
      } else {
        onAggr([{ $group: { _id: null, [$]: { [key]: value } } }])
      }
      return { $ }
    }

    return this.transformEvalExpr(expr, onAggr)
  }

  public query(query: Query.Expr) {
    const filter: Filter<any> = {}
    const additional: Filter<any>[] = []
    for (const key in query) {
      const value = query[key]
      if (key === '$and' || key === '$or') {
        // MongoError: $and/$or/$nor must be a nonempty array
        // { $and: [] } matches everything
        // { $or: [] } matches nothing
        if (value.length) {
          filter[key] = value.map(query => this.query(query))
        } else if (key === '$or') {
          return
        }
      } else if (key === '$not') {
        // MongoError: unknown top level operator: $not
        // https://stackoverflow.com/questions/25270396/mongodb-how-to-invert-query-with-not
        // this may solve this problem but lead to performance degradation
        const query = this.query(value)
        if (query) filter.$nor = [query]
      } else if (key === '$expr') {
        additional.push({ $expr: this.eval(value) })
      } else {
        const actualKey = this.getActualKey(key)
        const query = transformFieldQuery(value, actualKey, additional)
        if (query === false) return
        if (query !== true) filter[actualKey] = query
      }
    }
    if (additional.length) {
      (filter.$and ||= []).push(...additional)
    }
    return filter
  }

  modifier(stages: any[], sel: Selection.Immutable) {
    const { args, model } = sel
    const { fields, offset, limit, sort, group, having } = args[0]

    // groupBy, having
    if (group.length) {
      const $group = { _id: {} }
      const $project = { _id: 0 }
      for (const key in fields) {
        if (group.includes(key)) {
          $group._id[key] = this.eval(fields[key])
        } else {
          $group[key] = this.eval(fields[key])
        }
      }
      if (having) {
        const key = this.createKey()
        $group[key] = this.eval(having)
        $project[key] = 0
      }
      for (const key in fields) {
        if (group.includes(key)) {
          $project[key] = '$_id.' + key
        } else {
          $project[key] = 1
        }
      }
      stages.push({ $group }, { $project })
    }

    // orderBy, limit, offset
    const $set = {}
    const $sort = {}
    const $unset = []
    for (const [expr, dir] of sort) {
      const value = this.eval(expr)
      if (typeof value === 'string') {
        $sort[value.slice(1)] = dir === 'desc' ? -1 : 1
      } else {
        const key = this.createKey()
        $set[key] = value
        $sort[key] = dir === 'desc' ? -1 : 1
        $unset.push(key)
      }
    }
    if ($unset.length) stages.push({ $set })
    if (Object.keys($sort).length) stages.push({ $sort })
    if ($unset.length) stages.push({ $unset })
    if (limit < Infinity) {
      stages.push({ $limit: offset + limit })
    }
    if (offset) {
      stages.push({ $skip: offset })
    }

    // project
    if (fields) {
      const $project = valueMap(fields, (expr) => this.eval(expr))
      if (this.virtualKey) $project._id = 0
      stages.push({ $project })
    } else if (this.virtualKey) {
      const $project: Dict = { _id: 0 }
      for (const key in model.fields) {
        $project[key] = key === this.virtualKey ? '$_id' : 1
      }
      stages.push({ $project })
    }
  }
}
