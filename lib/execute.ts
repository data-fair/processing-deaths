import fs from 'fs-extra'
import type { RunFunction } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import download from './download.ts'
import loadReferences from './references.ts'
import processFiles, { setShouldBeStopped, isStopped } from './process.ts'
import fixInconsistencies from './inconsistency.ts'
import schema from './output-schema.json' with { type: 'json' }

export const stop = async (): Promise<void> => { setShouldBeStopped(true) }

const FIRST_YEAR = 1970

const baseDataset = {
  isRest: true,
  description: '',
  origin: '',
  license: {
    title: 'Licence Ouverte / Open Licence',
    href: 'https://www.etalab.gouv.fr/licence-ouverte-open-licence'
  },
  schema,
  primaryKey: ['nom', 'prenom', 'numero_acte_deces']
}

export const run: RunFunction<ProcessingConfig> = async (context) => {
  const { processingConfig, processingId, tmpDir, axios, log, patchConfig } = context
  setShouldBeStopped(false)

  await log.step('Configuration')
  await log.info(`Jeu de données à traiter : ${processingConfig.datasetID}`)

  // year of the first death file to read, undefined when only the inconsistencies are checked
  let startYear: number | undefined
  let datasetId: string
  if (processingConfig.datasetMode === 'create') {
    await log.step('Création du jeu de données')
    const dataset = (await axios.post('api/v1/datasets', {
      ...baseDataset,
      title: processingConfig.datasetTitle || 'Décès',
      extras: { processingId }
    })).data
    await log.info(`jeu de données créé, id="${dataset.id}", title="${dataset.title}"`)
    // switch to update right away, a later failure must not create a second dataset
    await patchConfig({ datasetMode: 'update', dataset: { id: dataset.id, title: dataset.title } })
    datasetId = dataset.id
    startYear = processingConfig.startYear ?? FIRST_YEAR
  } else {
    await log.step('Vérification du jeu de données')
    const dataset = (await axios.get(`api/v1/datasets/${processingConfig.dataset.id}`)).data
    await log.info(`le jeu de données existe, id="${dataset.id}", title="${dataset.title}"`)
    datasetId = dataset.id
    if (processingConfig.datasetMode === 'update') {
      // start again from the year of the last update: the yearly file replaces the monthly ones
      const last = (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { size: 1, sort: '-_updatedAt' } })).data.results[0]
      startYear = last ? Number(last._updatedAt.slice(0, 4)) : FIRST_YEAR
    }
  }
  if (startYear !== undefined) await log.info(`Début du traitement en : ${startYear}`)

  await download(processingConfig.datasetID, startYear, tmpDir, axios, log)
  if (isStopped()) return await log.warning('Traitement interrompu')

  if (startYear !== undefined) {
    const refs = await loadReferences(processingConfig, axios, log)
    await processFiles(tmpDir, refs, processingConfig.maxAge, datasetId, axios, log)
    // lines are upserted by primary key, the next run sends the remaining ones again
    if (isStopped()) return await log.warning('Traitement interrompu, les lignes restantes seront envoyées à la prochaine exécution')
  }

  await fixInconsistencies(tmpDir, processingConfig.maxAge, datasetId, axios, log)
  if (isStopped()) return await log.warning('Traitement interrompu')

  if (processingConfig.clearFiles) await fs.emptyDir(tmpDir)
}
