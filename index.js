const download = require('./lib/download')
const process = require('./lib/process')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  await log.step('Configuration')
  await log.info(`Jeu de données à traiter : ${processingConfig.datasetID}`)

  const baseDataset = {
    isRest: true,
    description: '',
    origin: '',
    license: {
      title: 'Licence Ouverte / Open Licence',
      href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
    },
    schema: require('./lib/schema.json'),
    primaryKey: ['nom', 'prenom', 'numero_acte_deces'],
    extras: {}
  }

  const body = {
    ...baseDataset,
    title: processingConfig.dataset.title
  }

  let dataset
  await log.step('Vérification du jeu de données')
  if (processingConfig.datasetMode === 'create') {
    if (processingConfig.dataset.id) {
      try {
        await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)
        throw new Error('le jeu de données existe déjà')
      } catch (err) {
        if (err.status !== 404) throw err
      }
      // permet de créer le jeu de donnée éditable avec l'identifiant spécifié
      dataset = (await axios.put('api/v1/datasets/' + processingConfig.dataset.id, body)).data
    } else {
      // si aucun identifiant n'est spécifié, on créer le dataset juste à partir de son nom
      dataset = (await axios.post('api/v1/datasets', body)).data
    }
    await log.info(`jeu de donnée créé, id="${dataset.id}", title="${dataset.title}"`)
    if (!processingConfig.startYear) processingConfig.startYear = 1970
    await log.info(`Début du traitement en : ${processingConfig.startYear}`)
  } else if (processingConfig.datasetMode === 'update' || processingConfig.datasetMode === 'inconsistency' || processingConfig.datasetMode === 'incremental') {
    // permet de vérifier l'existance du jeu de donnée avant de réaliser des opérations dessus
    try {
      dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
      await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
      const lastLine = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}/lines?size=1&sort=-_updatedAt`)).data
      if (lastLine.results.length > 0) {
        processingConfig.startYear = lastLine.results[0]._updatedAt.split('-')[0]
        await log.info(`Début du traitement en : ${processingConfig.startYear}`)
      } else {
        await log.info('Aucune donnée dans le jeu de données, début du traitement en 1970')
        processingConfig.startYear = 1970
      }
    } catch (err) {
      if (!dataset) throw new Error(`le jeu de données n'existe pas, id="${processingConfig.dataset.id}"`)
    }
  }

  await download(processingConfig, tmpDir, axios, log)

  let keyInseeComm, keyNomComm
  let keyInseeDept, keyNomDept
  let keyInseeRegion, keyNomRegion
  let keyInseePays, keyNomPays

  if (!processingConfig.maxAge) throw new Error('Pas d\'âge maximal défini')

  if (processingConfig.datasetMode !== 'inconsistency') {
    await log.step('Récupération des jeux de données de références')

    const schemaInseeRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetInsee.id}/schema`)).data
    const schemaInseePaysRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseePays.id}/schema`)).data
    // console.log(schemaInseeRef)
    for (const i of schemaInseeRef) {
      if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codeCommune') keyInseeComm = i.key
      if (i['x-refersTo'] === 'http://schema.org/City') keyNomComm = i.key
      if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codeDepartement') keyInseeDept = i.key
      keyNomDept = 'nom_departement'
      if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codeRegion') keyInseeRegion = i.key
      keyNomRegion = 'nom_region'
    }

    if (!keyInseeComm) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas le concept requis "Code commune INSEE"`)
      throw new Error('Jeu de donnée de référence avec un concept manquant')
    }
    if (!keyNomComm) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas le concept requis "Commune"`)
      throw new Error('Jeu de donnée de référence avec un concept manquant')
    }
    if (!keyInseeDept) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas le concept requis "Code département INSEE"`)
      throw new Error('Jeu de donnée de référence avec un concept manquant')
    }
    if (!keyNomDept) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas la colonne "nom_departement"`)
      throw new Error('Jeu de donnée de référence avec une colonne manquante')
    }
    if (!keyInseeRegion) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas le concept requis "Code région INSEE"`)
      throw new Error('Jeu de donnée de référence avec un concept manquant')
    }
    if (!keyNomRegion) {
      await log.error(`Le jeu de données "${processingConfig.datasetInsee.title}" ne possède pas la colonne "nom_region"`)
      throw new Error('Jeu de donnée de référence avec une colonne manquante')
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
      keyInseeDept,
      keyNomDept,
      keyInseeRegion,
      keyNomRegion,
      keyInseePays,
      keyNomPays
    }

    const refCodeInseeComm = []
    // let codesCommunes = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseeCommune.id}/lines`, { params: { size: 10000, select: `${keyInseeComm},${keyNomComm}` } })).data
    let codesCommunes = (await axios.get(`api/v1/datasets/${processingConfig.datasetInsee.id}/lines`, { params: { size: 10000, select: `${keyInseeComm},${keyNomComm},${keyInseeDept},${keyNomDept},${keyInseeRegion},${keyNomRegion}` } })).data
    refCodeInseeComm.push(...codesCommunes.results)
    while (codesCommunes.results.length === 10000) {
      codesCommunes = (await axios.get(codesCommunes.next)).data
      refCodeInseeComm.push(...codesCommunes.results)
    }

    await log.info(`${refCodeInseeComm.length} lignes dans les données de référence "${processingConfig.datasetInsee.title}"`)
    const refCodeInseePays = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseePays.id}/lines`, { params: { size: 10000, select: `${keyInseePays},${keyNomPays}` } })).data.results
    await log.info(`${refCodeInseePays.length} lignes dans les données de référence "${processingConfig.datasetCodeInseePays.title}"`)

    await process(tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, pluginConfig, processingConfig, dataset, axios, log)
  } else {
    await process(tmpDir, null, null, null, pluginConfig, processingConfig, dataset, axios, log)
  }
}
