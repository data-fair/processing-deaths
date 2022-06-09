const processData = require('./src/process')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log }) => {
  const baseDataset = {
    isRest: true,
    description: '',
    origin: '',
    license: {
      title: 'Licence Ouverte / Open Licence',
      href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
    },
    schema: require('./src/schema.json'),
    primaryKey: ['nom', 'prenom', 'numeroActeDeces'],
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
  } else if (processingConfig.datasetMode === 'update') {
    // permet de vérifier l'existance du jeu de donnée avant de réaliser des opérations dessus
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  let keyInseeComm, keyNomComm
  let keyInseePays, keyNomPays

  await log.step('Récupération des jeux de données de références')

  const schemaInseeCommRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseeCommune.id}/schema`)).data
  const schemaInseePaysRef = (await axios.get(`api/v1/datasets/${processingConfig.datasetCodeInseePays.id}/schema`)).data
  // console.log(schemaInseeRef)
  for (const i of schemaInseeCommRef) {
    if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codeCommune') keyInseeComm = i.key
    if (i['x-refersTo'] === 'http://schema.org/City') keyNomComm = i.key
  }

  for (const i of schemaInseePaysRef) {
    if (i['x-refersTo'] === 'http://rdf.insee.fr/def/geo#codePays') keyInseePays = i.key
    if (i['x-refersTo'] === 'http://schema.org/addressCountry') keyNomPays = i.key
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

  await processData(tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, processingConfig, dataset, axios, log)
}
