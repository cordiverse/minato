# minato

[![Codecov](https://img.shields.io/codecov/c/github/shigma/minato?style=flat-square)](https://codecov.io/gh/shigma/minato)
[![downloads](https://img.shields.io/npm/dm/minato?style=flat-square)](https://www.npmjs.com/package/minato)
[![npm](https://img.shields.io/npm/v/minato?style=flat-square)](https://www.npmjs.com/package/minato)
[![GitHub](https://img.shields.io/github/license/shigma/minato?style=flat-square)](https://github.com/shigma/minato/blob/master/LICENSE)

Type Driven Database Framework.

## Features

- **Compatibility.** Complete driver-independent. Supports many drivers with a unified API.
- **Powerful.** It can do everything that SQL can do, even though you are not using SQL drivers.
- **Well-typed.** Minato is written with TypeScript, and it provides top-level typing support.
- **Extensible.** Simultaneous accesss to different databases based on your needs.
- **Modern.** Perform all the operations with a JavaScript API or even in the brower with low code.

## Driver Supports

| Driver | Version | Notes |
| ------ | ------ | ----- |
| [MySQL](https://github.com/shigma/minato/tree/master/packages/mysql) | [![npm](https://img.shields.io/npm/v/@minatojs/driver-mysql?style=flat-square)](https://www.npmjs.com/package/@minatojs/driver-mysql) | MySQL 5.7, MySQL 8.0, MariaDB 10.5 |
| [MongoDB](https://github.com/shigma/minato/tree/master/packages/mongodb) | [![npm](https://img.shields.io/npm/v/@minatojs/driver-mongo?style=flat-square)](https://www.npmjs.com/package/@minatojs/driver-mongo) | |
| [SQLite](https://github.com/shigma/minato/tree/master/packages/sqlite) | [![npm](https://img.shields.io/npm/v/@minatojs/driver-sqlite?style=flat-square)](https://www.npmjs.com/package/@minatojs/driver-sqlite) | |
| [LevelDB](https://github.com/shigma/minato/tree/master/packages/level) | [![npm](https://img.shields.io/npm/v/@minatojs/driver-level?style=flat-square)](https://www.npmjs.com/package/@minatojs/driver-level) | |
| [Memory](https://github.com/shigma/minato/tree/master/packages/memory) | [![npm](https://img.shields.io/npm/v/@minatojs/driver-memory?style=flat-square)](https://www.npmjs.com/package/@minatojs/driver-memory) | In-memory driver support |

## Basic Usage

```ts
import { Database } from 'minato'

const database = new Database()

await database.connect('mysql', {
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: '',
  database: 'minato',
})
```

## Data Definition

```ts
database.extend('user', {
  id: 'number',
  name: 'string',
  age: 'number',
  money: { type: 'number', initial: 100 },
}, {
  primary: 'id',
  autoInc: true,
})
```

## Simple API

### create

```ts
const user = await driver.create('user', {
  name: 'John',
  age: 20,
}) // { id: 1, name: 'John', age: 20, money: 100 }
```

### get

### remove

### set

### upsert

## Selection API

## Using TypeScript

## Using Multiple Drivers
