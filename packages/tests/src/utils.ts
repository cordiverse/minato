import { Database } from 'minato'

export async function setup<S, K extends keyof S & string>(database: Database<S>, name: K, table: Partial<S[K]>[]) {
  await database.remove(name, {})
  const result: S[K][] = []
  for (const item of table) {
    result.push(await database.create(name, item as any))
  }
  return result
}
