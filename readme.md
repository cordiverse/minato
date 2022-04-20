# cosmotype

[![Codecov](https://img.shields.io/codecov/c/github/cosmotype/cosmotype?style=flat-square)](https://codecov.io/gh/cosmotype/cosmotype)
[![npm](https://img.shields.io/npm/v/cosmotype?style=flat-square)](https://www.npmjs.com/package/cosmotype)

Type Driven Database Framework.

Currently supports MySQL (MariaDB), SQLite, MongoDB, LevelDB.

## Features

- **Compatibility.** Complete driver-independent. Supports many drivers with a unified API.
- **Powerful.** It can do everything that SQL can do, even though you are not using SQL drivers.
- **Well-typed.** Cosmotype is written with TypeScript, and it provides top-level typing support.
- **Extensible.** Simultaneous accesss to different databases based on your needs.
- **Modern.** Perform all the operations with a JavaScript API or even in the brower with low code.

## Basic Usage

```ts
import { Database } from 'cosmotype'
import MySQLDriver from '@cosmotype/driver-mysql'

const database = new Database()
const driver = new MySQLDriver(database, {
  host: 'localhost',
  port: 3306,
  user: 'root',
  password: '',
  database: 'cosmotype',
})

await driver.start()
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