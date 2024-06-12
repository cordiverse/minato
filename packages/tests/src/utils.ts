import { mapValues } from 'cosmokit'
import { Database } from 'minato'

export async function setup<S, K extends keyof S & string>(database: Database<S>, name: K, table: Partial<S[K]>[]) {
  await database.remove(name, {})
  const result: S[K][] = []
  for (const item of table) {
    const data: any = mapValues(item, (v, k) => (v && database.tables[name].fields[k]?.relation) ? { $literal: v } : v)
    result.push(await database.create(name, data))
  }
  return result
}
