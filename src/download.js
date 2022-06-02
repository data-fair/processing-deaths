const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const dayjs = require('dayjs')

const withStreamableFile = async (filePath, fn) => {
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(filePath + '.tmp')
  await fn(fs.createWriteStream(filePath + '.tmp'))
  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(filePath + '.tmp', 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  // write in tmp file then move it for a safer operation that doesn't create partial files
  await fs.move(filePath + '.tmp', filePath, { overwrite: true })
}

module.exports = async (dir = 'data', axios, log) => {
  const datasetId = '5de8f397634f4164071119c5'
  const res = await axios.get('https://www.data.gouv.fr/api/1/datasets/' + datasetId + '/')
  const ressources = res.data.resources

  for (const file of ressources) {
    if (file.title.match('deces-' + dayjs().year() + '-m[0-1][0-9].txt') || file.title.match('deces-[0-9]{4}.txt')) {
      const url = new URL(file.url)
      const filePath = `${dir}/${path.parse(url.pathname).base}`

      await log.info(`téléchargement du fichier ${file.title}, écriture dans ${filePath}`)
      await withStreamableFile(filePath, async (writeStream) => {
        const res = await axios({ url: url.href, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })
    }
  }
}
