process.env.NODE_ENV = 'test'
const config = require('config')
const testUtils = require('@data-fair/processings-test-utils')
const deathProcessing = require('../index.js')
/*
process.env.NODE_ENV = 'local-dev'
const fs = require('fs-extra')
const config = require('config')
const axios = require('axios')
const chalk = require('chalk')
const moment = require('moment')
const assert = require('assert').strict
const processing = require('../')
const path = require('path')

describe('deaths processing', () => {
  it('should expose a processing config schema for users', async () => {
    const schema = require('../processing-config-schema.json')
    assert.equal(schema.type, 'object')
  })

  it('should run a task', async function () {
    this.timeout(3600000000)

    const headers = { 'x-apiKey': config.dataFairAPIKey }
    const axiosInstance = axios.create({
      baseURL: config.dataFairUrl,
      headers,
      maxContentLength: Infinity,
      maxBodyLength: Infinity
    })

    // customize axios errors for shorter stack traces when a request fails
    axiosInstance.interceptors.response.use(response => response, error => {
      if (!error.response) return Promise.reject(error)
      delete error.response.request
      error.response.config = { method: error.response.config.method, url: error.response.config.url, data: error.response.config.data }
      return Promise.reject(error.response)
    })
  */
describe('deaths processing', () => {
  it('should run a task', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
        urlOpposition: config.urlOpposition
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'create',
        startYear: 1996,
        maxAge: 123,
        dataset: {
          title: 'Test Deaths 56',
          id: 'test-deaths-56'
        },
        datasetInsee: {
          title: 'Code officiel geographique',
          id: 'code-officiel-geographique'
        },
        datasetCodeInseePays: {
          title: 'Base officielle des codes Pays',
          id: 'process-cog'
        },
        datasetChangementCommune: {
          title: 'Changement de commune',
          id: 'cog-changements2'
        },
        datasetID: 'fichier-des-personnes-decedees'
      },
      tmpDir: 'data/'
    }, config, false)
    await deathProcessing.run(context)
  })
}) /*
describe('Deaths', function () {
  it('should download, process files and upload a csv on the staging', async function () {
    this.timeout(1000000)
    const context = testUtils.context({
      pluginConfig: {
      },
      processingConfig: {
        clearFiles: false,
        datasetMode: 'create',
        dataset: { title: 'Fichier des personnes décédées' },
        datasetID: 'fichier-des-personnes-decedees',
        filter: '56', // Département de la Loire-Atlantique
        forceUpdate: false
      },
      tmpDir: 'data/'
    }, config, false)
    await deathProcessing.run(context)
  })
}) */
