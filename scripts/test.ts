import { getPackages } from 'yakumo'
import Mocha from 'mocha'
import globby from 'globby'
import cac from 'cac'

const { args } = cac().help().parse()
const cwd = process.cwd()

;(async () => {
  const packages = await getPackages(args)
  const patterns = Object
    .keys(packages)
    .map(folder => `${folder}/tests/*.spec.ts`.slice(1))

  const mocha = new Mocha()
  mocha.files = await globby(patterns, { cwd })
  mocha.run(failures => process.exit(failures))
})()
