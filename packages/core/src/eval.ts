import { defineProperty, isNullable, mapValues } from 'cosmokit'
import { Comparable, Flatten, isComparable, isEmpty, makeRegExp, Row } from './utils.ts'
import { Type } from './type.ts'
import { Field, Relation } from './model.ts'

export function isEvalExpr(value: any): value is Eval.Expr {
  return value && Object.keys(value).some(key => key.startsWith('$'))
}

export function isAggrExpr(expr: Eval.Expr): boolean {
  return expr['$'] || expr['$select']
}

export function hasSubquery(value: any): boolean {
  if (!isEvalExpr(value)) return false
  return Object.entries(value).filter(([k]) => k.startsWith('$')).some(([k, v]) => {
    if (isNullable(v) || isComparable(v)) return false
    if (k === '$exec') return true
    if (isEvalExpr(v)) return hasSubquery(v)
    if (Array.isArray(v)) return v.some(x => hasSubquery(x))
    if (typeof v === 'object') return Object.values(v).some(x => hasSubquery(x))
    return false
  })
}

export type Uneval<U, A extends boolean> =
  | U extends Relation<(infer T)[]> ? Eval.Term<Partial<T>, A>[]
  : U extends Relation<infer T> ? Eval.Term<Partial<T>, A>
  : U extends number ? Eval.Term<number, A>
  : U extends string ? Eval.Term<string, A>
  : U extends boolean ? Eval.Term<boolean, A>
  : U extends Date ? Eval.Term<Date, A>
  : U extends RegExp ? Eval.Term<RegExp, A>
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
    [Type.kType]?: Type<T>
  }

  export type Any<A extends boolean = boolean> = Comparable | Expr<any, A>

  export type Term<T, A extends boolean = boolean> = T | Expr<T, A>
  export type Array<T, A extends boolean = boolean> = Term<T, A>[] | Expr<T[], A>

  export type Unary<S, R> = <T extends S, A extends boolean>(x: Term<T, A>) => Expr<R, A>
  export type Binary<S, R> = <T extends S, A extends boolean>(x: Term<T, A>, y: Term<T, A>) => Expr<R, A>
  export type Multi<S, R> = <T extends S, A extends boolean>(...args: Term<T, A>[]) => Expr<R, A>

  export interface Aggr<S> {
    <T extends S>(value: Term<T, false>): Expr<T, true>
    <T extends S, A extends boolean>(value: Array<T, A>): Expr<T, A>
  }

  export interface Branch<T, A extends boolean> {
    case: Term<boolean, A>
    then: Term<T, A>
  }

  export interface Static {
    <A extends boolean>(key: string, value: any, type: Type): Eval.Expr<any, A>

    ignoreNull<T, A extends boolean>(value: Eval.Expr<T, A>): Eval.Expr<T, A>
    select(...args: Any[]): Expr<any[], false>
    update<T extends object>(modifier: Relation.Modifier<T>): Expr<T>[]

    // univeral
    if<T extends Comparable, A extends boolean>(cond: Any<A>, vThen: Term<T, A>, vElse: Term<T, A>): Expr<T, A>
    ifNull<T extends Comparable, A extends boolean>(...args: Term<T, A>[]): Expr<T, A>
    switch<T, A extends boolean>(branches: Branch<T, A>[], vDefault: Term<T, A>): Expr<T, A>

    // arithmetic
    add: Multi<number, number>
    mul: Multi<number, number>
    multiply: Multi<number, number>
    sub: Binary<number, number>
    subtract: Binary<number, number>
    div: Binary<number, number>
    divide: Binary<number, number>
    mod: Binary<number, number>
    modulo: Binary<number, number>

    // mathematic
    abs: Unary<number, number>
    floor: Unary<number, number>
    ceil: Unary<number, number>
    round: Unary<number, number>
    exp: Unary<number, number>
    log<A extends boolean>(x: Term<number, A>, base?: Term<number, A>): Expr<number, A>
    pow: Binary<number, number>
    power: Binary<number, number>
    random(): Expr<number, false>

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
    in<T extends Comparable, A extends boolean>(x: Term<T, A>, array: Array<T, A>): Expr<boolean, A>
    in<T extends Comparable, A extends boolean>(x: Term<T, A>[], array: Array<T[], A>): Expr<boolean, A>
    nin<T extends Comparable, A extends boolean>(x: Term<T, A>, array: Array<T, A>): Expr<boolean, A>
    nin<T extends Comparable, A extends boolean>(x: Term<T, A>[], array: Array<T[], A>): Expr<boolean, A>

    // string
    concat: Multi<string, string>
    regex<A extends boolean>(x: Term<string, A>, y: Term<string, A> | Term<RegExp, A>): Expr<boolean, A>

    // logical
    and: Multi<boolean, boolean>
    or: Multi<boolean, boolean>
    not: Unary<boolean, boolean>

    // typecast
    literal<T>(value: T, type?: Type<T> | Field.Type<T> | Field.NewType<T> | string): Expr<T, false>
    number: Unary<any, number>

    // aggregation / json
    sum: Aggr<number>
    avg: Aggr<number>
    max: Aggr<Comparable>
    min: Aggr<Comparable>
    count(value: Any<false>): Expr<number, true>
    length(value: Any<false>): Expr<number, true>
    size<A extends boolean>(value: (Any | Expr<Any, A>)[] | Expr<Any[], A>): Expr<number, A>
    length<A extends boolean>(value: any[] | Expr<any[], A>): Expr<number, A>

    object<T extends any>(row: Row.Cell<T>): Expr<T, false>
    object<T extends any>(row: Row<T>): Expr<T, false>
    array<T>(value: Expr<T, false>): Expr<T[], true>
  }
}

export const Eval = ((key, value, type) => defineProperty(defineProperty({ ['$' + key]: value }, kExpr, true), Type.kType, type)) as Eval.Static

const operators = Object.create(null) as Record<`$${keyof Eval.Static}`, (args: any, data: any) => any>

operators['$'] = getRecursive

type UnaryCallback<T> = T extends (value: infer R) => Eval.Expr<infer S> ? (value: R, data: any[]) => S : never
function unary<K extends keyof Eval.Static>(key: K, callback: UnaryCallback<Eval.Static[K]>, type: Type | ((...args: any[]) => Type)): Eval.Static[K] {
  operators[`$${key}`] = callback
  return ((value: any) => Eval(key, value, typeof type === 'function' ? type(value) : type)) as any
}

type MultivariateCallback<T> = T extends (...args: infer R) => Eval.Expr<infer S> ? (args: R, data: any) => S : never
function multary<K extends keyof Eval.Static>(
  key: K, callback: MultivariateCallback<Eval.Static[K]>,
  type: Type | ((...args: any[]) => Type),
): Eval.Static[K] {
  operators[`$${key}`] = callback
  return (...args: any) => Eval(key, args, typeof type === 'function' ? type(...args) : type) as any
}

type BinaryCallback<T> = T extends (...args: any[]) => Eval.Expr<infer S> ? (...args: any[]) => S : never
function comparator<K extends keyof Eval.Static>(key: K, callback: BinaryCallback<Eval.Static[K]>): Eval.Static[K] {
  operators[`$${key}`] = (args, data) => {
    const left = executeEval(data, args[0])
    const right = executeEval(data, args[1])
    if (isNullable(left) || isNullable(right)) return true
    return callback(left.valueOf(), right.valueOf())
  }
  return (...args: any) => Eval(key, args, Type.Boolean) as any
}

Eval.switch = (branches, vDefault) => Eval('switch', { branches, default: vDefault }, Type.fromTerm(branches[0]))
operators.$switch = (args, data) => {
  for (const branch of args.branches) {
    if (executeEval(data, branch.case)) return executeEval(data, branch.then)
  }
  return executeEval(data, args.default)
}

Eval.ignoreNull = (expr) => (expr[Type.kType]!.ignoreNull = true, expr)
Eval.select = multary('select', (args, table) => args.map(arg => executeEval(table, arg)), Type.Array())
Eval.update = (modifier) => modifier as any

// univeral
Eval.if = multary('if', ([cond, vThen, vElse], data) => executeEval(data, cond) ? executeEval(data, vThen)
  : executeEval(data, vElse), (cond, vThen, vElse) => Type.fromTerm(vThen))
Eval.ifNull = multary('ifNull', ([value, fallback], data) => executeEval(data, value) ?? executeEval(data, fallback), (value) => Type.fromTerm(value))

// arithmetic
Eval.add = multary('add', (args, data) => args.reduce<number>((prev, curr) => prev + executeEval(data, curr), 0), Type.Number)
Eval.mul = Eval.multiply = multary('multiply', (args, data) => args.reduce<number>((prev, curr) => prev * executeEval(data, curr), 1), Type.Number)
Eval.sub = Eval.subtract = multary('subtract', ([left, right], data) => executeEval(data, left) - executeEval(data, right), Type.Number)
Eval.div = Eval.divide = multary('divide', ([left, right], data) => executeEval(data, left) / executeEval(data, right), Type.Number)
Eval.mod = Eval.modulo = multary('modulo', ([left, right], data) => executeEval(data, left) % executeEval(data, right), Type.Number)

// mathematic
Eval.abs = unary('abs', (arg, data) => Math.abs(executeEval(data, arg)), Type.Number)
Eval.floor = unary('floor', (arg, data) => Math.floor(executeEval(data, arg)), Type.Number)
Eval.ceil = unary('ceil', (arg, data) => Math.ceil(executeEval(data, arg)), Type.Number)
Eval.round = unary('round', (arg, data) => Math.round(executeEval(data, arg)), Type.Number)
Eval.exp = unary('exp', (arg, data) => Math.exp(executeEval(data, arg)), Type.Number)
Eval.log = multary('log', ([left, right], data) => Math.log(executeEval(data, left)) / Math.log(executeEval(data, right ?? Math.E)), Type.Number)
Eval.pow = Eval.power = multary('power', ([left, right], data) => Math.pow(executeEval(data, left), executeEval(data, right)), Type.Number)
Eval.random = () => Eval('random', {}, Type.Number)
operators.$random = () => Math.random()

// comparison
Eval.eq = comparator('eq', (left, right) => left === right)
Eval.ne = comparator('ne', (left, right) => left !== right)
Eval.gt = comparator('gt', (left, right) => left > right)
Eval.ge = Eval.gte = comparator('gte', (left, right) => left >= right)
Eval.lt = comparator('lt', (left, right) => left < right)
Eval.le = Eval.lte = comparator('lte', (left, right) => left <= right)

// element
Eval.in = (value, array) => Eval('in', [Array.isArray(value) ? Eval.select(...value) : value, array], Type.Boolean)
operators.$in = ([value, array], data) => {
  const val = executeEval(data, value), arr = executeEval(data, array)
  if (typeof val === 'object') return arr.includes(val) || arr.map(JSON.stringify).includes(JSON.stringify(val))
  return arr.includes(val)
}
Eval.nin = (value, array) => Eval('nin', [Array.isArray(value) ? Eval.select(...value) : value, array], Type.Boolean)
operators.$nin = ([value, array], data) => {
  const val = executeEval(data, value), arr = executeEval(data, array)
  if (typeof val === 'object') return !arr.includes(val) && !arr.map(JSON.stringify).includes(JSON.stringify(val))
  return !arr.includes(val)
}

// string
Eval.concat = multary('concat', (args, data) => args.map(arg => executeEval(data, arg)).join(''), Type.String)
Eval.regex = multary('regex', ([value, regex], data) => makeRegExp(executeEval(data, regex)).test(executeEval(data, value)), Type.Boolean)

// logical
Eval.and = multary('and', (args, data) => args.every(arg => executeEval(data, arg)), Type.Boolean)
Eval.or = multary('or', (args, data) => args.some(arg => executeEval(data, arg)), Type.Boolean)
Eval.not = unary('not', (value, data) => !executeEval(data, value), Type.Boolean)

// typecast
Eval.literal = multary('literal', ([value, type]) => {
  if (type) throw new TypeError('literal cast is not supported')
  else return value
}, (value, type) => type ? Type.fromField(type) : Type.fromTerm(value))
Eval.number = unary('number', (arg, data) => {
  const value = executeEval(data, arg)
  return value instanceof Date ? Math.floor(value.valueOf() / 1000) : Number(value)
}, Type.Number)

const unwrapAggr = (expr: any, def?: Type) => {
  let type = Type.fromTerm(expr)
  type = Type.getInner(type) ?? type
  return (def && type.type === 'expr') ? def : type
}

// aggregation
Eval.sum = unary('sum', (expr, table) => Array.isArray(table)
  ? table.reduce<number>((prev, curr) => prev + executeAggr(expr, curr), 0)
  : Array.from<number>(executeEval(table, expr)).reduce((prev, curr) => prev + curr, 0), Type.Number)
Eval.avg = unary('avg', (expr, table) => {
  if (Array.isArray(table)) return table.reduce((prev, curr) => prev + executeAggr(expr, curr), 0) / table.length
  else {
    const array = Array.from<number>(executeEval(table, expr))
    return array.reduce((prev, curr) => prev + curr, 0) / array.length
  }
}, Type.Number)
Eval.max = unary('max', (expr, table) => Array.isArray(table)
  ? table.map(data => executeAggr(expr, data)).reduce((x, y) => x > y ? x : y, -Infinity)
  : Array.from<number>(executeEval(table, expr)).reduce((x, y) => x > y ? x : y, -Infinity), (expr) => unwrapAggr(expr, Type.Number))
Eval.min = unary('min', (expr, table) => Array.isArray(table)
  ? table.map(data => executeAggr(expr, data)).reduce((x, y) => x < y ? x : y, Infinity)
  : Array.from<number>(executeEval(table, expr)).reduce((x, y) => x < y ? x : y, Infinity), (expr) => unwrapAggr(expr, Type.Number))
Eval.count = unary('count', (expr, table) => new Set(table.map(data => executeAggr(expr, data))).size, Type.Number)
defineProperty(Eval, 'length', unary('length', (expr, table) => Array.isArray(table)
  ? table.map(data => executeAggr(expr, data)).length
  : Array.from(executeEval(table, expr)).length, Type.Number))

operators.$object = (field, table) => mapValues(field, value => executeAggr(value, table))
Eval.object = (fields: any) => {
  if (fields.$model) {
    const modelFields: [string, Field][] = Object.entries(fields.$model.fields)
    const prefix: string = fields.$prefix
    fields = Object.fromEntries(modelFields
      .filter(([, field]) => Field.available(field))
      .filter(([path]) => path.startsWith(prefix))
      .map(([k]) => [k.slice(prefix.length), fields[k.slice(prefix.length)]]))
    return Eval('object', fields, Type.Object(mapValues(fields, (value) => Type.fromTerm(value))))
  }
  return Eval('object', fields, Type.Object(mapValues(fields, (value) => Type.fromTerm(value)))) as any
}

Eval.array = unary('array', (expr, table) => Array.isArray(table)
  ? table.map(data => executeAggr(expr, data)).filter(x => !expr[Type.kType]?.ignoreNull || !isEmpty(x))
  : Array.from(executeEval(table, expr)).filter(x => !expr[Type.kType]?.ignoreNull || !isEmpty(x)), (expr) => Type.Array(Type.fromTerm(expr)))

Eval.exec = unary('exec', (expr, data) => (expr.driver as any).executeSelection(expr, data), (expr) => Type.fromTerm(expr.args[0]))

export { Eval as $ }

type MapUneval<S> = {
  [K in keyof S]?: null | Uneval<Exclude<S[K], undefined>, boolean>
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
