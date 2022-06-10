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

    const pluginConfig = { }

    const processingConfig = {
      clearFiles: true,
      datasetMode: 'create',
      startYear: 1970,
      dataset: {
        title: 'deces-test',
        id: 'process-deces-id'
      },
      datasetCodeInseeCommune: {
        title: 'Base officielle des codes postaux 0.1',
        id: 'laposte-hexasmal'
      },
      datasetCodeInseePays: {
        title: 'Base officielle des codes Pays',
        id: 'process-cog'
      },
      tmpDir: 'data/tmp',
      workDir: 'data/work'
    }

    const log = {
      step: (msg) => console.log(chalk.blue.bold.underline(`[${moment().format('LTS')}] ${msg}`)),
      error: (msg, extra) => console.log(chalk.red.bold(`[${moment().format('LTS')}] ${msg}`), extra),
      warning: (msg, extra) => console.log(chalk.red(`[${moment().format('LTS')}] ${msg}`), extra),
      info: (msg, extra) => console.log(chalk.blue(`[${moment().format('LTS')}] ${msg}`), extra),
      debug: (msg, extra) => {
        console.log(`[${moment().format('LTS')}] ${msg}`, extra)
      }
    }
    const patchConfig = async (patch) => {
      console.log('received config patch', patch)
      Object.assign(processingConfig, patch)
    }

    await fs.ensureDir(processingConfig.tmpDir)
    await fs.ensureDir(processingConfig.workDir)
    const tmpDir = path.resolve(processingConfig.tmpDir)
    process.chdir(processingConfig.workDir)
    await processing.run({ pluginConfig, processingConfig, tmpDir, axios: axiosInstance, log, patchConfig })
  })
})
