import { Database, Driver } from '@minatojs/core'
import ns from 'ns-require'

declare module '@minatojs/core' {
  interface Database {
    connect<T>(constructor: Driver.Constructor<T>, config?: T, name?: string): Promise<void>
    connect(constructor: string, config?: any, name?: string): Promise<void>
  }
}

const scope = ns({
  namespace: 'minato',
  prefix: 'driver',
  official: 'minatojs',
})

Database.prototype.connect = async function connect(constructor: string | Driver.Constructor, config: any, name = 'default') {
  if (typeof constructor === 'string') {
    constructor = scope.require(constructor) as Driver.Constructor
  }
  const driver = new constructor(this, config)
  await driver.start()
  this.drivers[name] = driver
  this.refresh()
}

export * from '@minatojs/core'
