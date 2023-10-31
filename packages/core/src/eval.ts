import { defineProperty, Dict, isNullable, valueMap } from 'cosmokit'
import { Comparable, Flatten, isComparable, makeRegExp } from './utils'

export function isEvalExpr(value: any): value is Eval.Expr {
  return value && Object.keys(value).some(key => key.startsWith('$'))
}

type $Date = Date
type $RegExp = RegExp

export type Uneval<U, A extends boolean> =
  | U extends number ? Eval.Number<A>
  : U extends string ? Eval.String<A>
  : U extends boolean ? Eval.Boolean<A>
  : U extends $Date ? Eval.Date<A>
  : U extends $RegExp ? Eval.RegExp<A>
  : any

export type Eval<U> =
  | U extends Comparable ? U
  : U extends Eval.Expr<infer T> ? T
  : never

const kExpr = Symbol('expr')
const kType = Symbol('type')
const kAggr = Symbol('aggr')

export namespace Eval {
  export interface Expr<T = any, A extends boolean = boolean> {
    [kExpr]: true
    [kType]?: T
    [kAggr]?: A
  }

  export type Number<A extends boolean = boolean> = number | Expr<number, A>
  export type String<A extends boolean = boolean> = string | Expr<string, A>
  export type Boolean<A extends boolean = boolean> = boolean | Expr<boolean, A>
  export type Date<A extends boolean = boolean> = $Date | Expr<$Date, A>
  export type RegExp<A extends boolean = boolean> = $RegExp | Expr<$RegExp, A>
  export type Any<A extends boolean = boolean> = Comparable | Expr<any, A>

  export type Binary<S, R> = <T extends S, A extends boolean>(x: T | Expr<T, A>, y: T | Expr<T, A>) => Expr<R, A>
  export type Multi<S, R> = <T extends S, A extends boolean>(...args: (T | Expr<T, A>)[]) => Expr<R, A>

  export interface Branch<T, A extends boolean> {
    case: Boolean
    then: T | Expr<T, A>
  }

  export interface Static {
    <A extends boolean>(key: string, value: any): Eval.Expr<any, A>

    // univeral
    if<T extends Comparable, A extends boolean>(cond: Any<A>, vThen: T | Expr<T, A>, vElse: T | Expr<T, A>): Expr<T, A>
    ifNull<T extends Comparable, A extends boolean>(...args: (T | Expr<T, A>)[]): Expr<T, A>
    switch<T, A extends boolean>(branches: Branch<T, A>[], vDefault: T | Expr<T, A>): Expr<T, A>

    // arithmetic
    add: Multi<number, number>
    mul: Multi<number, number>
    multiply: Multi<number, number>
    sub: Binary<number, number>
    subtract: Binary<number, number>
    div: Binary<number, number>
    divide: Binary<number, number>

    // comparison
    eq: Multi<Comparable, boolean>
    ne: Binary<Comparable, boolean>
    gt: Binary<Comparable, boolean>
    ge: Binary<Comparable, boolean>
    gte: Binary<Comparable, boolean>
    lt: Binary<Comparable, boolean>
    le: Binary<Comparable, boolean>
    lte: Binary<Comparable, boolean>

    // element
    in<T extends Comparable, A extends boolean>(x: T | Expr<T, A>, array: (T | Expr<T, A>)[] | Expr<T[], A>): Expr<boolean, A>
    nin<T extends Comparable, A extends boolean>(x: T | Expr<T, A>, array: (T | Expr<T, A>)[] | Expr<T[], A>): Expr<boolean, A>

    // string
    concat: Multi<string, string>
    regex<A extends boolean>(x: String<A>, y: String<A> | RegExp<A>): Expr<boolean, A>

    // logical
    and: Multi<Boolean, boolean>
    or: Multi<Boolean, boolean>
    not<A extends boolean>(value: Boolean<A>): Expr<boolean, A>

    // aggregation
    sum(value: Number<false>): Expr<number, true>
    avg(value: Number<false>): Expr<number, true>
    max(value: Number<false>): Expr<number, true>
    min(value: Number<false>): Expr<number, true>
    count(value: Any<false>): Expr<number, true>

    // json
    sum<A extends boolean>(value: (Number | Expr<Number, A>)[] | Expr<Number[], A>): Expr<number, A>
    avg<A extends boolean>(value: (Number | Expr<Number, A>)[] | Expr<Number[], A>): Expr<number, A>
    max<A extends boolean>(value: (Number | Expr<Number, A>)[] | Expr<Number[], A>): Expr<number, A>
    min<A extends boolean>(value: (Number | Expr<Number, A>)[] | Expr<Number[], A>): Expr<number, A>
    count<A extends boolean>(value: (Any | Expr<Any, A>)[] | Expr<Any[], A>): Expr<number, A>

    object<T extends Dict<Expr>>(fields: T): Expr<T, false>
    array<T>(value: Expr<T, false>): Expr<T[], true>
  }
}

export const Eval = ((key, value) => defineProperty({ ['$' + key]: value }, kExpr, true)) as Eval.Static

const operators = {} as Record<`$${keyof Eval.Static}`, (args: any, data: any) => any>

operators['$'] = getRecursive

type UnaryCallback<T> = T extends (value: infer R) => Eval.Expr<infer S> ? (value: R, data: any[]) => S : never
function unary<K extends keyof Eval.Static>(key: K, callback: UnaryCallback<Eval.Static[K]>): Eval.Static[K] {
  operators[`$${key}`] = callback
  return (value: any) => Eval(key, value) as any
}

type MultivariateCallback<T> = T extends (...args: infer R) => Eval.Expr<infer S> ? (args: R, data: any) => S : never
function multary<K extends keyof Eval.Static>(key: K, callback: MultivariateCallback<Eval.Static[K]>): Eval.Static[K] {
  operators[`$${key}`] = callback
  return (...args: any) => Eval(key, args) as any
}

type BinaryCallback<T> = T extends (...args: any[]) => Eval.Expr<infer S> ? (...args: any[]) => S : never
function comparator<K extends keyof Eval.Static>(key: K, callback: BinaryCallback<Eval.Static[K]>): Eval.Static[K] {
  operators[`$${key}`] = (args, data) => {
    const left = executeEval(data, args[0])
    const right = executeEval(data, args[1])
    if (isNullable(left) || isNullable(right)) return true
    return callback(left.valueOf(), right.valueOf())
  }
  return (...args: any) => Eval(key, args) as any
}

Eval.switch = (branches, vDefault) => Eval('switch', { branches, default: vDefault })
operators.$switch = (args, data) => {
  for (const branch of args.branches) {
    if (executeEval(data, branch.case)) return executeEval(data, branch.then)
  }
  return executeEval(data, args.default)
}

// univeral
Eval.if = multary('if', ([cond, vThen, vElse], data) => executeEval(data, cond) ? executeEval(data, vThen) : executeEval(data, vElse))
Eval.ifNull = multary('ifNull', ([value, fallback], data) => executeEval(data, value) ?? executeEval(data, fallback))

// arithmetic
Eval.add = multary('add', (args, data) => args.reduce<number>((prev, curr) => prev + executeEval(data, curr), 0))
Eval.mul = Eval.multiply = multary('multiply', (args, data) => args.reduce<number>((prev, curr) => prev * executeEval(data, curr), 1))
Eval.sub = Eval.subtract = multary('subtract', ([left, right], data) => executeEval(data, left) - executeEval(data, right))
Eval.div = Eval.divide = multary('divide', ([left, right], data) => executeEval(data, left) / executeEval(data, right))

// comparison
Eval.eq = comparator('eq', (left, right) => left === right)
Eval.ne = comparator('ne', (left, right) => left !== right)
Eval.gt = comparator('gt', (left, right) => left > right)
Eval.ge = Eval.gte = comparator('gte', (left, right) => left >= right)
Eval.lt = comparator('lt', (left, right) => left < right)
Eval.le = Eval.lte = comparator('lte', (left, right) => left <= right)

// element
Eval.in = multary('in', ([value, array], data) => executeEval(data, array).includes(executeEval(data, value)))
Eval.nin = multary('nin', ([value, array], data) => !executeEval(data, array).includes(executeEval(data, value)))

// string
Eval.concat = multary('concat', (args, data) => args.map(arg => executeEval(data, arg)).join(''))
Eval.regex = multary('regex', ([value, regex], data) => makeRegExp(executeEval(data, regex)).test(executeEval(data, value)))

// logical
Eval.and = multary('and', (args, data) => args.every(arg => executeEval(data, arg)))
Eval.or = multary('or', (args, data) => args.some(arg => executeEval(data, arg)))
Eval.not = unary('not', (value, data) => !executeEval(data, value))

// aggregation
Eval.sum = unary('sum', (expr, table) => Array.isArray(table)
  ? table.reduce<number>((prev, curr) => prev + executeAggr(expr, curr), 0)
  : Array.from<number>(executeEval(table, expr)).reduce((prev, curr) => prev + curr, 0))
Eval.avg = unary('avg', (expr, table) => {
  if (Array.isArray(table)) return table.reduce((prev, curr) => prev + executeAggr(expr, curr), 0) / table.length
  else {
    const array = Array.from<number>(executeEval(table, expr))
    return array.reduce((prev, curr) => prev + curr, 0) / array.length
  }
})
Eval.max = unary('max', (expr, table) => Array.isArray(table)
  ? Math.max(...table.map(data => executeAggr(expr, data)))
  : Math.max(...Array.from<number>(executeEval(table, expr))))
Eval.min = unary('min', (expr, table) => Array.isArray(table)
  ? Math.min(...table.map(data => executeAggr(expr, data)))
  : Math.min(...Array.from<number>(executeEval(table, expr))))
Eval.count = unary('count', (expr, table) => Array.isArray(table)
  ? new Set(table.map(data => executeAggr(expr, data))).size
  : new Set(Array.from(executeEval(table, expr))).size)

Eval.object = unary('object', (field, table) => valueMap(field, value => executeAggr(value, table)))
Eval.array = unary('array', (expr, table) => Array.isArray(table)
  ? table.map(data => executeAggr(expr, data))
  : Array.from(executeEval(table, expr)))
export { Eval as $ }

type MapUneval<S> = {
  [K in keyof S]?: Uneval<S[K], false>
}

export type Update<T = any> = MapUneval<Flatten<T>>

function getRecursive(args: string | string[], data: any): any {
  if (typeof args === 'string') {
    // for backwards compatibility, TODO remove in v2
    return getRecursive(['_', args], data)
  }

  const [ref, path] = args
  let value = data[ref]
  if (!value) return value
  if (path in value) return value[path]
  const prefix = Object.keys(value).find(s => path.startsWith(s + '.')) || path.split('.', 1)[0]
  const rest = path.slice(prefix.length + 1).split('.').filter(Boolean)
  rest.unshift(prefix)
  for (const key of rest) {
    value = value[key]
    if (!value) return value
  }
  return value
}

function executeEvalExpr(expr: any, data: any) {
  for (const key in expr) {
    if (key in operators) {
      return operators[key](expr[key], data)
    }
  }
  return expr
}

function executeAggr(expr: any, data: any) {
  if (typeof expr === 'string') {
    return getRecursive(expr, data)
  }
  return executeEvalExpr(expr, data)
}

export function executeEval(data: any, expr: any) {
  if (isComparable(expr) || isNullable(expr)) {
    return expr
  }
  if (Array.isArray(expr)) {
    return expr.map(item => executeEval(data, item))
  }
  return executeEvalExpr(expr, data)
}

export function executeUpdate(data: any, update: any, ref: string) {
  for (const key in update) {
    let root = data
    const path = key.split('.')
    const last = path.pop()!
    for (const key of path) {
      root = root[key] ||= {}
    }
    root[last] = executeEval({ [ref]: data, _: data }, update[key])
  }
  return data
}
