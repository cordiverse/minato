import { mapValues } from 'cosmokit'
import { Database, Tables } from 'minato'

export async function setup<K extends keyof Tables>(database: Database, name: K, table: Partial<Tables[K]>[]) {
  await database.remove(name, {})
  const result: Tables[K][] = []
  for (const item of table) {
    const data: any = mapValues(item, (v, k) => (v && database.tables[name].fields[k]?.relation) ? { $literal: v } : v)
    result.push(await database.create(name, data))
  }
  return result
}
