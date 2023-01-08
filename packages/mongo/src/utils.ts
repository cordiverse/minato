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

  private transformEvalExpr(expr: any, group?: Dict) {
    if (expr.$) {
      if (typeof expr.$ === 'string') {
        return '$' + this.getActualKey(expr.$)
      } else {
        return '$' + this.getActualKey(expr.$[1])
      }
    }

    if (expr.$if) {
      return { $cond: expr.$if.map(val => this.eval(val, group)) }
    }

    return valueMap(expr as any, (value) => {
      if (Array.isArray(value)) {
        return value.map(val => this.eval(val, group))
      } else {
        return this.eval(value, group)
      }
    })
  }

  private transformAggr(expr: any) {
    if (typeof expr === 'number' || typeof expr === 'boolean') {
      return expr
    }

    if (typeof expr === 'string') {
      return '$' + expr
    }

    return this.transformEvalExpr(expr)
  }

  public eval(expr: any, group?: Dict) {
    if (typeof expr === 'number' || typeof expr === 'string' || typeof expr === 'boolean') {
      return expr
    }

    for (const type of aggrKeys) {
      if (!expr[type]) continue
      const key = this.createKey()
      const value = this.transformAggr(expr[type])
      if (type !== '$count') {
        group![key] = { [type]: value }
        return '$' + key
      } else {
        group![key] = { $addToSet: value }
        return { $size: '$' + key }
      }
    }

    return this.transformEvalExpr(expr, group)
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

    // orderBy, limit, offset
    const $set = {}
    const $sort = {}
    const $unset: string[] = []
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

    // groupBy, having, fields
    if (group.length) {
      const $group: Dict = { _id: {} }
      const $project: Dict = { _id: 0 }
      stages.push({ $group })

      for (const key in fields) {
        if (group.includes(key)) {
          $group._id[key] = this.eval(fields[key])
          $project[key] = '$_id.' + key
        } else {
          $project[key] = this.eval(fields[key], $group)
        }
      }
      if (having['$and'].length) {
        const $expr = this.eval(having, $group)
        stages.push({ $match: { $expr } })
      }
      stages.push({ $project })
    } else if (fields) {
      const $project = valueMap(fields, (expr) => this.eval(expr))
      $project._id = 0
      stages.push({ $project })
    } else {
      const $project: Dict = { _id: 0 }
      for (const key in model.fields) {
        $project[key] = key === this.virtualKey ? '$_id' : 1
      }
      stages.push({ $project })
    }
  }
}
