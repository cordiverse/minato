import * as cordis from 'cordis'
import { Database } from '@minatojs/core'

export * from 'cordis'
export * from '@minatojs/core'

export class Context extends cordis.Context {
  constructor() {
    super()
    this.provide('model', undefined, true)
    this.model = new Database(this)
  }
}
