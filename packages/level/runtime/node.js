"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.getStats = exports.resolveLocation = void 0;
const fs_1 = require("fs");
var path_1 = require("path");
Object.defineProperty(exports, "resolveLocation", { enumerable: true, get: function () { return path_1.resolve; } });
async function getStats(location) {
    const { size } = await fs_1.promises.stat(location);
    return { size };
}
exports.getStats = getStats;
