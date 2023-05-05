process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const deathsProcessing = require('../')

describe('Code Officiel Géographique', function () {
  it('should download, process files and upload a csv on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'create',
        dataset: { title: 'Code Officiel Géographique' },
        filter: '56',
        apiKey: config.inseeAPIKey,
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await deathsProcessing.run(context)
  })
}) /*
describe('Code Officiel Géographique', function () {
  it('should update a dataset on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'update',
        dataset: { title: 'Code Officiel Géographique', id: 'code-officiel-geographique' },
        filter: '75',
        apiKey: '0EdrxJbVffwhQd9KrvC9epaPNq4a',
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await cogProcessing.run(context)
  })
}) */
