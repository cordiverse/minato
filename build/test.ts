import { cwd, getPackages } from 'yakumo'
import Mocha from 'mocha'
import globby from 'globby'
import cac from 'cac'

const { args } = cac().help().parse()

;(async () => {
  const packages = await getPackages(args)
  const patterns = Object
    .keys(packages)
    .filter(folder => folder !== '/')
    .map(folder => `${folder.slice(1)}/**/*.spec.ts`)

  const mocha = new Mocha()
  mocha.files = await globby(patterns, { cwd })
  mocha.run(failures => process.exit(failures))
})()
