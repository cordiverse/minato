/// <reference types="chai" />
/// <reference types="deep-eql" />

interface DeepEqualOptions<T1 = unknown, T2 = unknown> {
  comparator?: (leftHandOperand: T1, rightHandOperand: T2) => boolean | null;
}

declare namespace Chai {
  interface Config {
    deepEqual: (<T1, T2>(
      leftHandOperand: T1,
      rightHandOperand: T2,
      options?: DeepEqualOptions<T1, T2>,
    ) => boolean) | null | undefined
  }

  interface ChaiUtils {
    eql: <T1, T2>(
      leftHandOperand: T1,
      rightHandOperand: T2,
      options?: DeepEqualOptions<T1, T2>,
    ) => boolean
  }

  interface Assertion {
    shape(expected: any, message?: string): Assertion
  }

  interface Ordered {
    shape(expected: any, message?: string): Assertion
  }

  interface Eventually {
    shape(expected: any, message?: string): PromisedAssertion
  }

  interface PromisedOrdered {
    shape(expected: any, message?: string): PromisedAssertion
  }
}

declare module './shape' {
  declare const ChaiShape: Chai.ChaiPlugin

  export = ChaiShape
}
