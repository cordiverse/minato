import { MaybeArray } from 'cosmokit'
import { Update as EvalUpdate } from './eval.ts'
import { AtomicTypes, Flatten, Keys, Row, Values } from './utils.ts'
import { Query } from './query.ts'
import { Selection } from './selection.ts'

export type Relation<T = object> = Partial<T> & Relation.Mark

export namespace Relation {
  const Mark = Symbol('minato.relation')
  export type Mark = { [Mark]: true }

  type UnArray<T> = T extends (infer I)[] ? I : T

  export type Include<S> = boolean | {
    [P in Keys<S, Mark>]?: S[P] extends Relation<infer T> | undefined ? Include<UnArray<T>> : never
  }

  export type QueryExpr<S> = {
    $every: Query.Expr<Flatten<S>>
    $some: Query.Expr<Flatten<S>>
    $none: Query.Expr<Flatten<S>>
  }

  export type Create<S> = S
    | (S extends Values<AtomicTypes> ? never
    : S extends Relation<(infer T)[]> ? Create<T>[]
    : S extends Relation<infer T> ? Create<T>
    : S extends any[] ? never
    : string extends keyof S ? never
    : S extends object ? { [K in keyof S]: Create<S[K]> }
    : never)

  export interface UpdateExpr<S> {
    $create?: MaybeArray<Create<S>>
    $set?: Row.Computed<S, EvalUpdate<S>>
    $remove?: Query.Expr<Flatten<S>> | Selection.Callback<S, boolean>
    $connect?: Query.Expr<Flatten<S>> | Selection.Callback<S, boolean>
    $disconnect?: Query.Expr<Flatten<S>> | Selection.Callback<S, boolean>
  }

  export type UpdateInner<S> = EvalUpdate<S>
    | (S extends Values<AtomicTypes> ? never
    : S extends Relation<(infer T)[]> ? UpdateExpr<T> | Create<T>[]
    : S extends Relation<infer T> ? Create<T>
    : S extends any[] ? never
    : string extends keyof S ? never
    : S extends object ? { [K in keyof S]?: Update<S[K]> }
    : never)

  export type Update<S> = {[K in keyof S]?: UpdateInner<S[K]> }

  export function buildAssociationTable(...tables: [string, string]) {
    return '_' + tables.sort().join('To')
  }

  export function buildAssociationKey(key: string, table: string) {
    return `${table}_${key}`
  }

  export namespace Update {
    export interface Create<S> {
      $create: MaybeArray<S>
    }

    export interface Update<S> {
      $update: [Query<S>, S]
    }

    export interface Delete<S> {
      $delete: Query<S>
    }

    export type Expr<S> = Create<S> | Update<S> | Delete<S>
  }
}
