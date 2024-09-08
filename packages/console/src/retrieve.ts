import { Driver, Eval, isAggrExpr, isEvalExpr, Model, Query, Selection, Type } from 'minato'
import { defineProperty, Dict, makeArray, mapValues } from 'cosmokit'

const Executable = Object.getPrototypeOf(Selection)

type ExtractUnary<T> = T extends [infer U] ? U : T

interface EvalTypeContext {
  tables: Dict<Model>
  driver: Driver
}

type EvalTypeSolvers = {
  [K in keyof Eval.Static as `$${K}`]: (expr: ExtractUnary<Parameters<Eval.Static[K]>>, ctx?: EvalTypeContext) => Type
} & { $: (expr: any, ctx: EvalTypeContext) => Type }

const solvers: EvalTypeSolvers = Object.create(null)

const solverFactory = (type: Type) => (args, ctx) => (retrieveExprsType(makeArray(args), ctx), type)

solvers.$ = (arg, ctx) => {
  if (typeof arg === 'string') return Type.fromField('expr')
  const [ref, path] = arg
  return ctx.tables[ref]?.getType(path) ?? Type.fromField('expr')
}

solvers.$select = solverFactory(Type.Array())
solvers.$exec = (expr) => Type.fromTerm(expr.args[0])

solvers.$if = (args, ctx) => (retrieveExprsType(args, ctx), Type.fromTerm(args[1]))
solvers.$ifNull = (args, ctx) => (retrieveExprsType(args, ctx), Type.fromTerm(args[0]))

solvers.$add = solvers.$multiply = solvers.$subtract = solvers.$divide = solvers.$modulo = solverFactory(Type.Number)
solvers.$abs = solvers.$floor = solvers.$ceil = solvers.$round = solvers.$exp = solvers.$log = solvers.$pow = solverFactory(Type.Number)
solvers.$random = () => Type.Number

solvers.$eq = solvers.$ne = solvers.$gt = solvers.$gte = solvers.$lt = solvers.$lte = solverFactory(Type.Boolean)
solvers.$in = solvers.$nin = solverFactory(Type.Boolean)

solvers.$concat = solverFactory(Type.String)
solvers.$regex = solverFactory(Type.Boolean)

solvers.$and = solvers.$or = solvers.$xor = (args, ctx) => (retrieveExprsType(args, ctx), Type.fromTerms(args, Type.Boolean))
solvers.$not = (arg, ctx) => (retrieveExprType(arg, ctx), Type.fromTerms([arg], Type.Boolean))

solvers.$literal = ([value, type], ctx) => type ? Type.fromField(type) : Type.fromTerm(value)
solvers.$number = solverFactory(Type.Number)

const unwrapAggr = (expr: any, ctx: EvalTypeContext | undefined, def?: Type) => {
  let type = retrieveExprType(expr, ctx)
  type = Type.getInner(type) ?? type
  return (def && type.type === 'expr') ? def : type
}

solvers.$sum = solvers.$avg = solvers.$count = solvers.$length = solverFactory(Type.Number)
solvers.$min = solvers.$max = (expr, ctx) => unwrapAggr(expr, ctx, Type.Number)

solvers.$object = (fields, ctx) => {
  const types = mapValues(fields, (value) => retrieveExprType(value, ctx))
  return Type.Object(types)
}
solvers.$array = (expr, ctx) => Type.Array(retrieveExprType(expr, ctx))

function retrieveExprType<T>(expr: Eval.Term<T>, ctx: EvalTypeContext | undefined): Type {
  if (!isEvalExpr(expr)) return Type.fromTerm(expr)
  if (!expr[Type.kType]) {
    for (const key in expr) {
      if (key.startsWith('$') && key in solvers) {
        if (key === '$exec') {
          expr[key] = retrieveSelection(expr[key], ctx?.driver!)
        }
        const type = solvers[key](expr[key], ctx)
        if (expr['$ignoreNull']) type.ignoreNull = true
        defineProperty(expr, Type.kType, type)
        return type
      }
    }
  }
  return expr[Type.kType]!
}

function retrieveExprsType(exprs: Eval.Term<any>[], ctx: EvalTypeContext | undefined) {
  exprs.forEach(expr => retrieveExprType(expr, ctx))
}

function retrieveQuery(query: Query.Expr, ctx) {
  if (query.$expr) retrieveExprType(query.$expr, ctx)
  if (query.$and) query.$and.forEach((expr) => retrieveQuery(expr, ctx))
  if (query.$or) query.$or.forEach((expr) => retrieveQuery(expr, ctx))
  if (query.$not) retrieveQuery(query.$not, ctx)
}

export function retrieveSelection<T extends Selection.Mutable | Selection.Immutable>(sel: T, driver: Driver, models?: Record<string, Model>): T {
  if (sel instanceof Selection) return sel
  models ??= {}
  if (Selection.is(sel.table)) {
    sel.table = retrieveSelection(sel.table, driver, models)
  } else if (typeof sel.table === 'object') {
    sel.table = mapValues(sel.table, (table) => retrieveSelection(table, driver, models))
  }

  sel = new Executable(driver, sel) as T
  models[sel.ref] = sel.model
  sel.tables = mapValues(sel.tables, (_, k) => models[k])

  if (sel.query) retrieveQuery(sel.query, sel)
  if (sel.type === 'get') {
    retrieveExprType(sel.args[0].having, sel)
    sel.args[0].sort.forEach(([field]) => retrieveExprType(field, sel))
    sel.args[0].limit ??= Infinity
    Object.values(sel.args[0].fields ?? {}).forEach((field) => retrieveExprType(field, sel))
  } else if (sel.type === 'set') {
    Object.values(sel.args[0]).forEach((field) => retrieveExprType(field, sel))
  } else if (sel.type === 'upsert') {
    sel.args[0].map(update => Object.values(update).forEach((field) => retrieveExprType(field, sel)))
  } else if (sel.type === 'eval') {
    retrieveExprType(sel.args[0], sel)
    if (isAggrExpr(sel.args[0])) defineProperty(sel.args[0], Type.kType, Type.Array(Type.fromTerm(sel.args[0])))
  }
  return sel
}
