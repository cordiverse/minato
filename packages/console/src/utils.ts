import { Binary } from 'cosmokit'

export function serialize(value: any): string {
  if (Binary.isSource(value)) return `b${Binary.toBase64(Binary.fromSource(value))}`
  if (typeof value === 'string') return 's' + value
  if (typeof value === 'bigint') return 'B' + value.toString()
  if (value instanceof Date) return 'd' + new Date(value).toJSON()
  if (value instanceof RegExp) return 'r' + JSON.stringify([value.source, value.flags])
  return JSON.stringify(value, (_, v) => _serialzie(v))
}

function _serialzie(value: any): any {
  if (Binary.isSource(value)) return `b${Binary.toBase64(Binary.fromSource(value))}`
  if (typeof value === 'string') return 's' + value
  if (typeof value === 'bigint') return 'B' + value.toString()
  if (typeof value === 'object') {
    if (value === null) return null
    if (value instanceof Date) return 'd' + new Date(value).toJSON()
    if (value instanceof RegExp) return 'r' + JSON.stringify([value.source, value.flags])
    const o = Array.isArray(value) ? [] : {}
    for (const k in value) {
      o[k] = _serialzie(value[k])
      if (o[k] !== value[k]) o[k].toJSON = undefined
    }
    return o
  }
  return value
}

export function deserialize(value: string): any {
  if (value === undefined) return undefined
  if (typeof value === 'string') {
    if (value.startsWith('b')) return Binary.fromBase64(value.slice(1))
    if (value.startsWith('s')) return value.slice(1)
    if (value.startsWith('B')) return BigInt(value.slice(1))
    if (value.startsWith('d')) return new Date(value.slice(1))
    if (value.startsWith('r')) {
      const [source, flags] = JSON.parse(value.slice(1))
      return new RegExp(source, flags)
    }
  }
  return JSON.parse(value, (_, v) => _deserialize(v))
}

function _deserialize(value: any): any {
  if (typeof value === 'string') {
    if (value.startsWith('b')) return Binary.fromBase64(value.slice(1))
    if (value.startsWith('s')) return value.slice(1)
    if (value.startsWith('B')) return BigInt(value.slice(1))
    if (value.startsWith('d')) return new Date(value.slice(1))
    if (value.startsWith('r')) {
      const [source, flags] = JSON.parse(value.slice(1))
      return new RegExp(source, flags)
    }
  }
  if (typeof value === 'object') {
    if (value === null) return null
    if (Array.isArray(value)) return value.map(_deserialize)
    const o: any = {}
    for (const k in value) {
      o[k] = _deserialize(value[k])
    }
    return o
  }
  return value
}
