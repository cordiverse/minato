import { defineProperty, Dict, isNullable, valueMap } from 'cosmokit'
import { RuntimeType } from './runtime'
import { Comparable, Flatten, isComparable, makeRegExp } from './utils'

export function isEvalExpr(value: any): value is Eval.Expr {
  return value && Object.keys(value).some(key => key.startsWith('$'))
}

export function getExprRuntimeType(value: any): RuntimeType {
  if (isNullable(value)) return RuntimeType.any
  if (RuntimeType.test(value)) return value
  if (isEvalExpr(value)) return value[kRuntimeType]
  else if (typeof value === 'string') return RuntimeType.string
  else if (typeof value === 'number') return RuntimeType.number
  else if (typeof value === 'boolean') return RuntimeType.boolean
  else if (value instanceof Date) return RuntimeType.date
  else if (value instanceof RegExp) return RuntimeType.regexp
  else if (Array.isArray(value)) return RuntimeType.list(RuntimeType.merge(...value))
  else return RuntimeType.create(valueMap(value, getExprRuntimeType))
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
const kRuntimeType = Symbol('RuntimeType')

export namespace Eval {
  export interface Expr<T = any, A extends boolean = boolean> {
    [kExpr]: true
    [kType]?: T
    [kAggr]?: A
    [kRuntimeType]: RuntimeType<T>
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
    <A extends boolean>(key: string, value: any, type: RuntimeType): Eval.Expr<any, A>

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
    at<T extends Comparable, A extends boolean>(array: (T | Expr<T, A>)[] | Expr<T[], A>, index: Number): Expr<T, A>

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

    object<T extends Dict<Expr>>(fields: T): Expr<T, false>
    array<T>(value: Expr<T, false>): Expr<T, true>
  }
}

export const Eval = ((key, value, type) => defineProperty({ ['$' + key]: value, [kRuntimeType]: type }, kExpr, true)) as Eval.Static

const operators = {} as Record<`$${keyof Eval.Static}`, (args: any, data: any) => any>

operators['$'] = getRecursive

type UnaryCallback<T> = T extends (value: infer R) => Eval.Expr<infer S> ? (value: R, data: any[]) => S : never
function unary<K extends keyof Eval.Static, T = any>(key: K, callback: UnaryCallback<Eval.Static[K]>,
  type: RuntimeType<T> | ((value) => RuntimeType<T>)): Eval.Static[K] {
  operators[`$${key}`] = callback
  return (value: any) => Eval(key, value, typeof type === 'function' ? type(value) : type) as any
}

type MultivariateCallback<T> = T extends (...args: infer R) => Eval.Expr<infer S> ? (args: R, data: any) => S : never
function multary<K extends keyof Eval.Static>(
  key: K, callback: MultivariateCallback<Eval.Static[K]>, type: RuntimeType | ((...args) => RuntimeType)): Eval.Static[K] {
  operators[`$${key}`] = callback
  return (...args: any[]) => Eval(key, args, typeof type === 'function' ? type(...args) : type) as any
}

type BinaryCallback<T> = T extends (...args: any[]) => Eval.Expr<infer S> ? (...args: any[]) => S : never
function comparator<K extends keyof Eval.Static>(key: K, callback: BinaryCallback<Eval.Static[K]>): Eval.Static[K] {
  operators[`$${key}`] = (args, data) => {
    const left = executeEval(data, args[0])
    const right = executeEval(data, args[1])
    if (isNullable(left) || isNullable(right)) return true
    return callback(left.valueOf(), right.valueOf())
  }
  return (...args: any[]) => Eval(key, args, RuntimeType.create('boolean')) as any
}

Eval.switch = (branches, vDefault) => Eval('switch', { branches, default: vDefault }, getExprRuntimeType(vDefault))
operators.$switch = (args, data) => {
  for (const branch of args.branches) {
    if (executeEval(data, branch.case)) return executeEval(data, branch.then)
  }
  return executeEval(data, args.default)
}

// univeral
Eval.if = multary('if', ([cond, vThen, vElse], data) => executeEval(data, cond) ? executeEval(data, vThen) : executeEval(data, vElse),
  (_, vThen, vElse) => RuntimeType.merge(vThen, vElse))
Eval.ifNull = multary('ifNull', ([value, fallback], data) => executeEval(data, value) ?? executeEval(data, fallback),
  (value, fallback) => RuntimeType.merge(value, fallback))

// arithmetic
Eval.add = multary('add', (args, data) => args.reduce<number>((prev, curr) => prev + executeEval(data, curr), 0), RuntimeType.number)
Eval.mul = Eval.multiply = multary(
  'multiply', (args, data) => args.reduce<number>((prev, curr) => prev * executeEval(data, curr), 1), RuntimeType.number)
Eval.sub = Eval.subtract = multary('subtract', ([left, right], data) => executeEval(data, left) - executeEval(data, right), RuntimeType.number)
Eval.div = Eval.divide = multary('divide', ([left, right], data) => executeEval(data, left) / executeEval(data, right), RuntimeType.number)

// comparison
Eval.eq = comparator('eq', (left, right) => left === right)
Eval.ne = comparator('ne', (left, right) => left !== right)
Eval.gt = comparator('gt', (left, right) => left > right)
Eval.ge = Eval.gte = comparator('gte', (left, right) => left >= right)
Eval.lt = comparator('lt', (left, right) => left < right)
Eval.le = Eval.lte = comparator('lte', (left, right) => left <= right)

// element
Eval.in = multary('in', ([value, array], data) => executeEval(data, array).includes(executeEval(data, value)), RuntimeType.boolean)
Eval.nin = multary('nin', ([value, array], data) => !executeEval(data, array).includes(executeEval(data, value)), RuntimeType.boolean)

// string
Eval.concat = multary('concat', (args, data) => args.map(arg => executeEval(data, arg)).join(''), RuntimeType.string)
Eval.regex = multary('regex', ([value, regex], data) => makeRegExp(executeEval(data, regex)).test(executeEval(data, value)), RuntimeType.boolean)

// logical
Eval.and = multary('and', (args, data) => args.every(arg => executeEval(data, arg)), RuntimeType.boolean)
Eval.or = multary('or', (args, data) => args.some(arg => executeEval(data, arg)), RuntimeType.boolean)
Eval.not = unary('not', (value, data) => !executeEval(data, value), RuntimeType.boolean)

// aggregation
Eval.sum = unary('sum', (expr, table) => table.reduce<number>((prev, curr) => prev + executeAggr(expr, curr), 0), RuntimeType.number)
Eval.avg = unary('avg', (expr, table) => table.reduce((prev, curr) => prev + executeAggr(expr, curr), 0) / table.length, RuntimeType.number)
Eval.max = unary('max', (expr, table) => Math.max(...table.map(data => executeAggr(expr, data))), RuntimeType.number)
Eval.min = unary('min', (expr, table) => Math.min(...table.map(data => executeAggr(expr, data))), RuntimeType.number)
Eval.count = unary('count', (expr, table) => new Set(table.map(data => executeAggr(expr, data))).size, RuntimeType.number)

Eval.object = unary('object', (field, table) => valueMap(field, value => executeAggr(value, table)),
  fields => RuntimeType.json(RuntimeType.create(valueMap(fields, getExprRuntimeType))))
Eval.array = unary('array', (expr, table) => table.map(data => executeAggr(expr, data)), expr => RuntimeType.json(RuntimeType.list(getExprRuntimeType(expr))))

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
