const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))

const url = 'https://www.data.gouv.fr/api/1/datasets/'

module.exports = async (processingConfig, tmpDir, axios, log) => {
  await log.step('Téléchargement des données')

  await log.info('Filtrage des dernières données')
  const json = await axios.get(url + processingConfig.datasetID + '/')
  const filtre = []
  const data = json.data.resources
  const annee = data[0].title.split('deces-')[1].split('-')[0]
  for (const file of data) {
    if (file.title === 'deces-' + annee + '.txt') {
      filtre.push(file.url)
    } else if (file.title.includes('deces-' + annee + '-m')) {
      filtre.push(file.url)
    }
    if (file.title.match('deces-[0-9]{4}.txt') || file.title.includes('.csv')) {
      filtre.push(file.url)
    }
  }
  await log.info(`Nombres fichies trouvés : ${filtre.length}`)
  await log.info('Téléchargement des fichiers...')

  let res
  for (const file of filtre) {
    const fileName = path.basename(file)
    const fileDir = path.join(tmpDir, fileName)
    try {
      res = await axios.get(file, { responseType: 'stream' })
    } catch (err) {
      await log.error(`Téléchargement du fichier ${fileName} a échoué`)
      await log.error(err)
      throw new Error(JSON.stringify(err, null, 2))
    }
    await fs.ensureFile(fileDir)
    await pump(res.data, fs.createWriteStream(fileDir))
  }
  await log.info('Téléchargement terminé')
}
