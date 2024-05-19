import { Binary, deepEqual, isNullable } from 'cosmokit'
import { inspect } from 'util'

function flag(obj, key, value?) {
  var flags = obj.__flags || (obj.__flags = Object.create(null));
  if (arguments.length === 3) {
    flags[key] = value;
  } else {
    return flags[key];
  }
};

function isSubsetOf(subset, superset, cmp, contains, ordered) {
  if (!contains) {
    if (subset.length !== superset.length) return false;
    superset = superset.slice();
  }

  return subset.every(function (elem, idx) {
    if (ordered) return cmp ? cmp(elem, superset[idx]) : elem === superset[idx];

    if (!cmp) {
      var matchIdx = superset.indexOf(elem);
      if (matchIdx === -1) return false;

      // Remove match from superset so not counted twice if duplicate in subset.
      if (!contains) superset.splice(matchIdx, 1);
      return true;
    }

    return superset.some(function (elem2, matchIdx) {
      if (!cmp(elem, elem2)) return false;

      // Remove match from superset so not counted twice if duplicate in subset.
      if (!contains) superset.splice(matchIdx, 1);
      return true;
    });
  });
}

export default (({ Assertion }) => {
  function checkShape(expect, actual, path, ordered) {
    if (actual === expect || Number.isNaN(expect) && Number.isNaN(actual)) return

    function formatError(expect, actual) {
      return `expected to have ${expect} but got ${actual} at path ${path}`
    }

    if (isNullable(expect) && isNullable(actual)) return

    if (!expect || ['string', 'number', 'boolean', 'bigint'].includes(typeof expect)) {
      return formatError(inspect(expect), inspect(actual))
    }

    // dates
    if (expect instanceof Date) {
      if (!(actual instanceof Date) || +expect !== +actual) {
        return formatError(inspect(expect), inspect(actual))
      }
      return
    }

    // binary
    if (Binary.is(expect)) {
      if (!Binary.is(actual) || !deepEqual(actual, expect)) {
        return formatError(inspect(expect), inspect(actual))
      }
      return
    }

    if (actual === null) {
      const type = Object.prototype.toString.call(expect).slice(8, -1).toLowerCase()
      return formatError(`a ${type}`, 'null')
    }

    // array / object
    if (!ordered && Array.isArray(expect) && Array.isArray(actual)) {
      if (!isSubsetOf(expect, actual, (x, y) => !checkShape(x, y, `${path}/`, ordered), false, false)) {
        return `expected same shape of members`
      }
      return
    }

    for (const prop in expect) {
      if (isNullable(actual[prop]) && !isNullable(expect[prop])) {
        return `expected "${prop}" field to be defined at path ${path}`
      }
      const message = checkShape(expect[prop], actual[prop], `${path}${prop}/`, ordered)
      if (message) return message
    }
  }

  Assertion.addMethod('shape', function (expect) {
    var ordered = flag(this, 'ordered');
    const message = checkShape(expect, this._obj, '/', ordered)
    if (message) this.assert(false, message, '', expect, this._obj)
  })
}) as Chai.ChaiPlugin
