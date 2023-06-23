process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const deathProcessing = require('../index.js')

describe('deaths processing', () => {
  it('should run a task', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
        urlOpposition: config.urlOpposition
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'inconsistency',
        maxAge: 130,
        dataset: {
          title: 'Test Deaths code',
          id: 'test-deaths-code'
        },
        datasetInsee: {
          title: 'Code officiel geographique',
          id: 'code-officiel-geographique'
        },
        datasetCodeInseePays: {
          title: 'Base officielle des codes Pays',
          id: 'process-cog'
        },
        datasetCodesActuels: {
          title: 'Communes actuelles',
          id: 'test-communes-actuelles'
        },
        datasetID: 'fichier-des-personnes-decedees'
      },
      tmpDir: 'data/'
    }, config, false)
    await deathProcessing.run(context)
  })
})
