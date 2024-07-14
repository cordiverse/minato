import { Extract, isNullable } from 'cosmokit'
import { Eval, executeEval } from './eval.ts'
import { AtomicTypes, Comparable, Flatten, flatten, getCell, Indexable, isComparable, isFlat, makeRegExp, RegExpLike, Values } from './utils.ts'
import { Selection } from './selection.ts'

export type Query<T = any> = Query.Expr<Flatten<T>> | Query.Shorthand<Indexable> | Selection.Callback<T, boolean>

export namespace Query {
  export interface FieldExpr<T = any> {
    // logical
    $or?: Field<T>[]
    $and?: Field<T>[]
    $not?: Field<T>

    // existence
    $exists?: boolean

    // membership
    $in?: Extract<T, Indexable, T[]>
    $nin?: Extract<T, Indexable, T[]>

    // arithmatic
    $eq?: Extract<T, Comparable>
    $ne?: Extract<T, Comparable>
    $gt?: Extract<T, Comparable>
    $gte?: Extract<T, Comparable>
    $lt?: Extract<T, Comparable>
    $lte?: Extract<T, Comparable>

    // list
    $el?: T extends (infer U)[] ? Field<U> : never
    $size?: Extract<T, any[], number>

    // regexp
    $regex?: Extract<T, string, string | RegExpLike>
    $regexFor?: Extract<T, string, string | { input: string; flags?: string }>

    // bitwise
    $bitsAllClear?: Extract<T, number>
    $bitsAllSet?: Extract<T, number>
    $bitsAnyClear?: Extract<T, number>
    $bitsAnySet?: Extract<T, number>

    // relation
    $some?: T extends (infer U)[] ? Query<U> : never
    $none?: T extends (infer U)[] ? Query<U> : never
    $every?: T extends (infer U)[] ? Query<U> : never
  }

  export interface LogicalExpr<T = any> {
    $or?: Expr<T>[]
    $and?: Expr<T>[]
    $not?: Expr<T>
    /** @deprecated use query callback instead */
    $expr?: Eval.Term<boolean>
  }

  export type Shorthand<T = any> =
    | Extract<T, Comparable>
    | Extract<T, Indexable, T[]>
    | Extract<T, string, RegExp>

  export type Field<T = any> = FieldExpr<T> | Shorthand<T>

  type NonNullExpr<T> = T extends Values<AtomicTypes> | any[] ? Field<T> : T extends object
    ? Expr<Flatten<T>> | Selection.Callback<T, boolean>
    : Field<T>

  export type Expr<T = any> = LogicalExpr<T> & {
    [K in keyof T]?: (undefined extends T[K] ? null : never) | NonNullExpr<Exclude<T[K], undefined>>
  }
}

type QueryOperators = {
  [K in keyof Query.FieldExpr]?: (query: NonNullable<Query.FieldExpr[K]>, data: any) => boolean
}

const queryOperators: QueryOperators = {
  // logical
  $or: (query, data) => query.reduce((prev, query) => prev || executeFieldQuery(query, data), false),
  $and: (query, data) => query.reduce((prev, query) => prev && executeFieldQuery(query, data), true),
  $not: (query, data) => !executeFieldQuery(query, data),

  // existence
  $exists: (query, data) => query !== isNullable(data),

  // comparison
  $eq: (query, data) => data.valueOf() === query.valueOf(),
  $ne: (query, data) => data.valueOf() !== query.valueOf(),
  $gt: (query, data) => data.valueOf() > query.valueOf(),
  $gte: (query, data) => data.valueOf() >= query.valueOf(),
  $lt: (query, data) => data.valueOf() < query.valueOf(),
  $lte: (query, data) => data.valueOf() <= query.valueOf(),

  // membership
  $in: (query, data) => query.includes(data),
  $nin: (query, data) => !query.includes(data),

  // regexp
  $regex: (query, data) => makeRegExp(query).test(data),
  $regexFor: (query, data) => typeof query === 'string' ? makeRegExp(data).test(query) : makeRegExp(data, query.flags).test(query.input),

  // bitwise
  $bitsAllSet: (query, data) => (query & data) === query,
  $bitsAllClear: (query, data) => (query & data) === 0,
  $bitsAnySet: (query, data) => (query & data) !== 0,
  $bitsAnyClear: (query, data) => (query & data) !== query,

  // list
  $el: (query, data) => data.some(item => executeFieldQuery(query, item)),
  $size: (query, data) => data.length === query,
}

function executeFieldQuery(query: Query.Field, data: any) {
  // shorthand syntax
  if (Array.isArray(query)) {
    return query.includes(data)
  } else if (query instanceof RegExp) {
    return query.test(data)
  } else if (isComparable(query)) {
    return data.valueOf() === query.valueOf()
  } else if (isNullable(query)) {
    return isNullable(data)
  }

  for (const key in query) {
    if (key in queryOperators) {
      if (!queryOperators[key](query[key], data)) return false
    }
  }

  return true
}

export function executeQuery(data: any, query: Query.Expr, ref: string, env: any = {}): boolean {
  const entries: [string, any][] = Object.entries(query)
  return entries.every(([key, value]) => {
    // execute logical query
    if (key === '$and') {
      return (value as Query.Expr[]).reduce((prev, query) => prev && executeQuery(data, query, ref, env), true)
    } else if (key === '$or') {
      return (value as Query.Expr[]).reduce((prev, query) => prev || executeQuery(data, query, ref, env), false)
    } else if (key === '$not') {
      return !executeQuery(data, value, ref, env)
    } else if (key === '$expr') {
      return executeEval({ ...env, [ref]: data, _: data }, value)
    }

    // execute field query
    try {
      const flattenQuery = isFlat(query[key]) ? { [key]: query[key] } : flatten(query[key], `${key}.`)
      return Object.entries(flattenQuery).every(([key, value]) => executeFieldQuery(value, getCell(data, key)))
    } catch {
      return false
    }
  })
}
