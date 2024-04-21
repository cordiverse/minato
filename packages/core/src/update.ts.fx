import { Extract, isNullable } from 'cosmokit'
import { Eval, executeEval } from './eval.ts'
import { Comparable, Flatten, Indexable, isComparable, makeRegExp, Row } from './utils.ts'
import { Selection } from './selection.ts'

export type Update<T = any> = Update.Expr<Flatten<T>>

export namespace Update {
  export interface Expr<T = any> {
    // logical
    $inc?: UpdateFields<T, number>
    $min?: UpdateFields<T, number>
    $max?: UpdateFields<T, number>

    $push?: UpdateFields<T, any[]>
  }

  export type Shorthand<T = any> =
    | Extract<T, Comparable>
    | Extract<T, Indexable, T[]>
    | Extract<T, string, RegExp>

  export type UpdateFields<T = any, U = any> = {
    [K in keyof T]?: null | Eval.Term<Extract<T[K], U>>
  }
}

type UpdateOperators = {
  [K in keyof Update.Expr]?: (update: NonNullable<Update.Expr[K]>, data: any) => void
}

const updateOperators: UpdateOperators = {
  // logical
  $inc: (update, data) => Object.entries(update).forEach(([key, diff]) => data[key] += diff),
  // $and: (query, data) => query.reduce((prev, query) => prev && executeFieldQuery(query, data), true),
  // $not: (query, data) => !executeFieldQuery(query, data),
}
