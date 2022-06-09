const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const stream = require('stream')
const byline = require('byline')
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

// parse n lines and return an array of json object
function parseLines (lines, refCodeInseeComm, refCodeInseePays, keysRef) {
  const out = []
  for (const line of lines) {
    try {
      const identity = {
        nom: line.substr(0, line.indexOf('*')).replace('*', ''),
        prenom: line.substr(line.indexOf('*'), line.indexOf('/')).toString().replace('*', '').replace('/', ''),
        genre: line[80] === '1' ? 'Homme' : 'Femme',
        codeVilleNaissance: line.slice(89, 94),
        nomVilleNaissance: line.slice(94, 124).trim(),
        paysNaissance: line.slice(124, 154).trim(),
        ageDeces: '',
        dateNaissance: line.slice(81, 89),
        dateMort: line.slice(154, 162),
        codeVilleDeces: line.slice(162, 167),
        nomVilleDeces: '',
        nomPaysDeces: '',
        numeroActeDeces: line.slice(167, 176).trim()
      }
      const date1 = identity.dateNaissance
      let moisNaissance = date1.slice(4, 6)
      let jourNaissance = date1.slice(6, 8)
      if (moisNaissance === '00') {
        moisNaissance = date1.substr(4, 2).replace('00', '01')
      }
      if (jourNaissance === '00') {
        jourNaissance = date1.substr(4, 2).replace('00', '01')
      }
      identity.dateNaissance = date1.slice(0, 4) + '-' + moisNaissance + '-' + jourNaissance

      const date2 = identity.dateMort
      let moisDeces = date2.slice(4, 6)
      let jourDeces = date2.slice(6, 8)
      if (moisDeces === '00') {
        moisDeces = date2.substr(4, 2).replace('00', '01')
      }
      if (jourDeces === '00') {
        jourDeces = date2.substr(4, 2).replace('00', '01')
      }
      identity.dateMort = date2.slice(0, 4) + '-' + moisDeces + '-' + jourDeces
      identity.ageDeces = getAge(identity.dateNaissance, identity.dateMort) < 150 ? getAge(identity.dateNaissance, identity.dateMort) : undefined

      if (identity.codeVilleDeces.startsWith('99')) {
        for (const elem of refCodeInseePays) {
          if (elem[keysRef.keyInseePays] === identity.codeVilleDeces) {
            identity.nomPaysDeces = elem[keysRef.keyNomPays]
          }
        }
      } else {
        identity.nomPaysDeces = 'FRANCE'
        for (const elem of refCodeInseeComm) {
          if (elem[keysRef.keyInseeComm] === identity.codeVilleDeces) {
            identity.nomVilleDeces = elem[keysRef.keyNomComm]
          }
        }
      }
      if (identity.paysNaissance.match(/[A-Z]+/g) === null) {
        identity.paysNaissance = 'FRANCE'
      }
      out.push(identity)
    } catch (err) {
      console.log(line)
      console.log(err)
    }
  }
  return out
}

function getAge (FirstDate, SecondDate) {
  const birthDate = new Date(FirstDate)
  const deathDate = new Date(SecondDate)

  const yearDiff = deathDate.getFullYear() - birthDate.getFullYear()
  const monthDiff = deathDate.getMonth() - birthDate.getMonth()
  const pastDate = deathDate.getDate() - birthDate.getDate()

  if (monthDiff < 0 || (monthDiff === 0 && pastDate < 0)) {
    return yearDiff - 1
  }

  return yearDiff
}

module.exports = async (tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, processingConfig, dataset, axios, log) => {
  const datasetId = '5de8f397634f4164071119c5'
  const res = await axios.get('https://www.data.gouv.fr/api/1/datasets/' + datasetId + '/')
  const ressources = res.data.resources
  // get the current extras
  const extras = dataset.extras
  if (!extras.currentFile) extras.currentFile = ''

  const downloadFile = []
  // get the title and url of file to process
  for (const file of ressources) {
    let downloadYear = true
    if (extras.currentFile.includes('m') && (extras.currentFile.substr(6, 4) === file.title.substr(6, 4))) downloadYear = false
    if (file.title.match('deces-' + dayjs().year() + '-m[0-1][0-9].txt') || (file.title.match('deces-[0-9]{4}.txt') && downloadYear)) {
      downloadFile.push({ title: file.title, url: file.url })
    }
    if (file.title === extras.currentFile) break
  }

  await log.step(`Traitement des ${downloadFile.length} fichier(s) restant(s).`)
  if (extras.currentFile !== '') { await log.info(`Dernier fichier traité : ${extras.currentFile}`) }
  let linesTab = []

  try {
    // reverse the array to process latest file first
    for (const file of downloadFile.reverse()) {
      extras.currentFile = file.title
      // set the file name in the extras
      await axios.patch(`api/v1/datasets/${dataset.id}/`, { extras })

      // download the file to process
      const url = new URL(file.url)
      const filePath = `${tmpDir}/${path.parse(url.pathname).base}`
      await log.info(`Téléchargement du fichier ${file.title}, écriture dans ${filePath}`)
      await withStreamableFile(filePath, async (writeStream) => {
        const res = await axios({ url: url.href, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })

      await log.info(`Traitement du fichier ${file.title}`)
      await pump(
        byline.createStream(fs.createReadStream(path.join(tmpDir, file.title), { encoding: 'utf8' })),
        new stream.Transform({
          objectMode: true,
          transform: async (obj, _, next) => {
            linesTab.push(obj)
            if (linesTab.length >= 15000) {
              const sendTab = parseLines(linesTab, refCodeInseeComm, refCodeInseePays, keysRef)
              linesTab = []
              const sizeTab = sendTab.length
              await log.info(`envoi de ${sizeTab} lignes vers le jeu de données`)
              while (sendTab.length) {
                // split the array into smaller arrays to avoid too heavy request
                const lines = sendTab.splice(0, 3000)
                try {
                  await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
                } catch (err) {
                  await log.info(err.statusText)
                }
              }
            }
            next()
          },
          flush: async (callback) => {
            if (linesTab.length > 0) {
              const sendTab = parseLines(linesTab, refCodeInseeComm, refCodeInseePays, keysRef)
              await log.info(`envoi de ${sendTab.length} lignes vers le jeu de données`)
              while (sendTab.length) {
                // split the array into smaller arrays to avoid too much requests
                const lines = sendTab.splice(0, 3000)
                try {
                  await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
                } catch (err) {
                  await log.info(err.statusText)
                }
              }
              linesTab = []
              callback()
            }
          }
        })
      )
      if (processingConfig.clearFiles) {
        await log.info(`Suppression de ${filePath}`)
        try {
          await fs.remove(filePath)
        } catch (err) {
          await log.info(err.statusText)
        }
      }
    }
  } catch (err) {
    await log.info(err.statusText)
  }
}
