const download = require('./src/download')
const processData = require('./src/process')
const upload = require('./src/upload')

exports.run = async ({ pluginConfig, processingConfig, tmpDir, axios, log, patchConfig }) => {
  
  // await download(pluginConfig, tmpDir, axios, log)

  const baseDataset = {
    isRest: true,
    description: '',
    origin: '',
    license: {
      title: 'Licence Ouverte / Open Licence',
      href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
    },
    schema: require('./src/schema.json'),
    primaryKey: ['nom','prenom','numero_acte_deces'],
    rest: {
      history: true,
      historyTTL: {
        active: true,
        delay: {
          value: 30,
          unit: 'days'
        }
      }
    }
  }

  const body = {
    ...baseDataset,
    title: processingConfig.dataset.title,
    extras: { }
  }

  let dataset
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
    await log.step('Vérification du jeu de données')
    dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    if (!dataset) throw new Error(`le jeu de données n'existe pas, id${processingConfig.dataset.id}`)
    await log.info(`le jeu de donnée existe, id="${dataset.id}", title="${dataset.title}"`)
  }

  await processData(tmpDir, dataset, axios, log)
  //if (!processingConfig.skipUpload) await upload(processingConfig, tmpDir, axios, log, patchConfig)
}