export namespace RuntimeError {
  export type Code =
    | 'duplicate-entry'
    | 'unsupported-expression'
}

export class RuntimeError<T extends RuntimeError.Code> extends Error {
  name = 'RuntimeError'

  constructor(public code: T, message?: string) {
    super(message || code.replace('-', ' '))
  }

  static check<T extends RuntimeError.Code>(error: any, code?: RuntimeError.Code): error is RuntimeError<T> {
    if (!(error instanceof RuntimeError)) return false
    return !code || error.message === code
  }
}
