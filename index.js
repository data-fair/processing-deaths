const download = require('./lib/download')
const process = require('./lib/process')
// const upload = require('./lib/upload')
const fs = require('fs-extra')
const path = require('path')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await log.step('Configuration')
  await log.info(`Jeu de données à traiter : ${processingConfig.datasetID}`)
  await log.info(`Mise à jour forcée : ${processingConfig.forceUpdate}`)

  await download(processingConfig, tmpDir, axios, log)

  if (processingConfig.datasetMode === 'update' && !processingConfig.forceUpdate) {
    try {
      await log.step('Vérification de l\'en-tête du jeu de données')
      const schemaActuelDataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}/schema`, { params: { calculated: false } })).data.map((elem) => `"${elem.key}"`).join(',').replace(/['"]+/g, '')

      let files = await fs.readdir(tmpDir)
      files = files.filter(file => file.endsWith('.csv'))
      const file = files[0] && path.join(tmpDir, files[0])
      const headFile = fs.createReadStream(file, { encoding: 'utf8' })
      let head

      await new Promise((resolve) => {
        headFile.once('data', (chunk) => {
          head = chunk.slice(0, chunk.indexOf('\n'))
          resolve()
        })
      })

      if (!head.includes(schemaActuelDataset.slice(0, head.length - 1))) {
        await log.info('Le jeu de données ne possède pas la même en-tête que le fichier téléchargé. Activez la mise à jour forcée pour mettre à jour')
        throw new Error('En-têtes différentes entre les fichiers')
      } else {
        await log.info('En-têtes identiques, mise à jour')
      }
    } catch (err) {
      await log.info(err)
      throw err
    }
  }

  let keyInseeComm, keyNomComm
  let keyInseePays, keyNomPays

  if (!processingConfig.maxAge) throw new Error('Pas d\'âge maximal défini')

  await log.step('Récupération des jeux de données de références')

  const schemaInseeCommRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseeCommune.id}/schema`)).data
  const schemaInseePaysRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseePays.id}/schema`)).data
  // console.log(schemaInseeRef)
  for (const i of schemaInseeCommRef) {
    if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codeCommune') keyInseeComm = i.key
    if (i['x-refersTo'] === 'http://schema.org/City') keyNomComm = i.key
  }

  if (!keyInseeComm) {
    await log.error(`Le jeu de données "${processingConfig.datasetCodeInseeCommune.title}" ne possède pas le concept requis "Code commune INSEE"`)
    throw new Error('Jeu de donnée de référence avec un concept manquant')
  }
  if (!keyNomComm) {
    await log.error(`Le jeu de données "${processingConfig.datasetCodeInseeCommune.title}" ne possède pas le concept requis "Commune"`)
    throw new Error('Jeu de donnée de référence avec un concept manquant')
  }

  for (const i of schemaInseePaysRef) {
    if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codePays') keyInseePays = i.key
    if (i['x-refersTo'] === 'http://schema.org/addressCountry') keyNomPays = i.key
  }

  if (!keyInseePays) {
    await log.error(`Le jeu de données "${processingConfig.datasetCodeInseePays.title}" ne possède pas le concept requis "Code pays INSEE"`)
    throw new Error('Jeu de donnée de référence avec un concept manquant')
  }
  if (!keyNomPays) {
    await log.error(`Le jeu de données "${processingConfig.datasetCodeInseePays.title}" ne possède pas le concept requis "Pays"`)
    throw new Error('Jeu de donnée de référence avec un concept manquant')
  }

  const keysRef = {
    keyInseeComm,
    keyNomComm,
    keyInseePays,
    keyNomPays
  }

  const refCodeInseeComm = []
  // let codesCommunes = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseeCommune.id}/lines`, { params: { size: 10000, select: `${keyInseeComm},${keyNomComm}` } })).data
  let codesCommunes = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseeCommune.id}/lines`, { params: { size: 10000, select: `${keyInseeComm},${keyNomComm}` } })).data
  refCodeInseeComm.push(...codesCommunes.results)
  while (codesCommunes.results.length === 10000) {
    codesCommunes = (await axios.get(codesCommunes.next)).data
    refCodeInseeComm.push(...codesCommunes.results)
  }

  await log.info(`${refCodeInseeComm.length} lignes dans les données de référence "${processingConfig.datasetCodeInseeCommune.title}"`)
  const refCodeInseePays = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseePays.id}/lines`, { params: { size: 10000, select: `${keyInseePays},${keyNomPays}` } })).data.results
  await log.info(`${refCodeInseePays.length} lignes dans les données de référence "${processingConfig.datasetCodeInseePays.title}"`)

  await process(tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, pluginConfig, processingConfig, axios, log)
}
