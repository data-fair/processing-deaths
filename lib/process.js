const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const stream = require('stream')
const byline = require('byline')
const dayjs = require('dayjs')
const csvSync = require('csv/sync')

const withStreamableFile = async (filePath, fn) => {
  // creating empty file before streaming seems to fix some weird bugs with NFS
  await fs.ensureFile(filePath + '.tmp')
  await fn(fs.createWriteStream(filePath + '.tmp'))
  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(filePath + '.tmp', 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  // // write in tmp file then move it for a safer operation that doesn't create partial files
  await fs.move(filePath + '.tmp', filePath, { overwrite: true })
}

// parse n lines and return an array of json object
function parseLines (lines, refCodeInseeComm, refCodeInseePays, keysRef, processingConfig) {
  const out = []
  for (const line of lines) {
    try {
      const identity = {
        nom: line.substr(0, line.indexOf('*')).replace('*', '').trim(),
        prenom: line.substr(line.indexOf('*'), line.indexOf('/') - 1).toString().replace('*', '').replace('/', '').trim(),
        sexe: line[80],
        codeVilleNaissance: line.slice(89, 94).trim(),
        nomVilleNaissance: line.slice(94, 124).trim(),
        paysNaissance: line.slice(124, 154).trim(),
        ageDeces: '',
        dateNaissance: line.slice(81, 89).trim(),
        dateMort: line.slice(154, 162).trim(),
        codeVilleDeces: line.slice(162, 167).trim(),
        nomVilleDeces: '',
        nomPaysDeces: '',
        numeroActeDeces: line.slice(167, 176).trim()
      }
      const dateN = identity.dateNaissance
      const anneeNaissance = dateN.slice(0, 4).padStart(4, '0')
      const moisNaissance = dateN.slice(4, 6)
      const jourNaissance = dateN.slice(6, 8)
      if (anneeNaissance === '0000') identity.dateNaissance = undefined
      else if (moisNaissance === '00') identity.dateNaissance = anneeNaissance
      else if (jourNaissance === '00') identity.dateNaissance = anneeNaissance + '-' + moisNaissance
      else identity.dateNaissance = anneeNaissance + '-' + moisNaissance + '-' + jourNaissance

      const dateM = identity.dateMort
      const anneeDeces = dateM.slice(0, 4).padStart(4, '0')
      const moisDeces = dateM.slice(4, 6)
      const jourDeces = dateM.slice(6, 8)
      if (anneeDeces === '0000') identity.dateMort = undefined
      else if (moisDeces === '00') identity.dateMort = anneeDeces
      else if (jourDeces === '00') identity.dateMort = anneeDeces + '-' + moisDeces
      else identity.dateMort = anneeDeces + '-' + moisDeces + '-' + jourDeces

      if (identity.dateMort && identity.dateNaissance) {
        const age = parseFloat(dayjs(identity.dateMort).diff(dayjs(identity.dateNaissance), 'year', true).toFixed(3))
        if (age >= 0 && age <= processingConfig.maxAge) {
          identity.ageDeces = age
        } else {
          identity.ageDeces = undefined
          identity.dateNaissance = undefined
        }
      }

      if (identity.codeVilleDeces.startsWith('99')) {
        for (const elem of refCodeInseePays) {
          if (elem[keysRef.keyInseePays] === identity.codeVilleDeces) {
            identity.nomPaysDeces = elem[keysRef.keyNomPays].trim()
          }
        }
      } else {
        identity.nomPaysDeces = 'FRANCE'
        for (const elem of refCodeInseeComm) {
          if (elem[keysRef.keyInseeComm] === identity.codeVilleDeces) {
            identity.nomVilleDeces = elem[keysRef.keyNomComm].trim()
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

async function updateInconsistency (tmpDir, pluginConfig, processingConfig, dataset, axios, log) {
  await log.step('Vérification des données incohérentes et demandes d\'opposition')
  const currDate = dayjs().format('YYYY-MM-DD')
  const weirdAge = (await axios.get(`api/v1/datasets/${dataset.id}/lines`, { params: { qs: `ageDeces:(<0 OR >${processingConfig.maxAge})`, size: 10000 } })).data.results
  const weirdDateNaissance = (await axios.get(`api/v1/datasets/${dataset.id}/lines`, { params: { qs: `dateNaissance:([* TO 1800-01-01] OR [${currDate} TO *])`, size: 10000 } })).data.results
  const weirdDateMort = (await axios.get(`api/v1/datasets/${dataset.id}/lines`, { params: { qs: `dateMort:([* TO 1800-01-01] OR [${currDate} TO *])`, size: 10000 } })).data.results
  await log.info(`${weirdAge.length} ligne(s) avec un age incohérent (<0 ou >${processingConfig.maxAge})`)
  await log.info(`${weirdDateNaissance.length} ligne(s) avec une date de naissance incohérente (<1800 ou >${currDate})`)
  await log.info(`${weirdDateMort.length} ligne(s) avec une date de décès incohérente (<1800 ou >${currDate})`)
  const toUpdate = []

  const stats = {
    update: 0,
    remove: 0
  }

  const inconsistencyLine = [
    ...weirdAge,
    ...weirdDateMort,
    ...weirdDateNaissance
  ]

  const result = inconsistencyLine.reduce((unique, o) => {
    if (!unique.some(obj => obj.nom === o.nom && obj.prenom === o.prenom && obj.dateMort === o.dateMort && obj.numeroActeDeces === o.numeroActeDeces)) {
      unique.push(o)
    }
    return unique
  }, [])

  await log.info(`Suppression des champs des ${result.length} lignes incohérentes`)

  for (const line of result) {
    line._action = 'update'
    line.ageDeces = undefined
    if (weirdDateNaissance.filter(f => line.nom === f.nom && line.prenom === f.prenom && line.numeroActeDeces === f.numeroActeDeces).length) line.dateNaissance = undefined
    if (weirdDateMort.filter(f => line.nom === f.nom && line.prenom === f.prenom && line.numeroActeDeces === f.numeroActeDeces).length) line.dateMort = undefined
    delete line._score
    delete line._rand
    delete line._i
    delete line._updatedAt
    toUpdate.push(line)
    stats.update++
  }

  if (pluginConfig.urlOpposition) {
    try {
      const url = new URL(pluginConfig.urlOpposition)
      const filePath = `${tmpDir}/${path.parse(url.pathname).base}`

      await withStreamableFile(filePath, async (writeStream) => {
        const res = await axios({ url: url.href, method: 'GET', responseType: 'stream' })
        await pump(res.data, writeStream)
      })

      const opposition = csvSync.parse(fs.readFileSync(filePath), { delimiter: ';' })
      await log.info(`Traitement des ${opposition.length - 1} lignes du fichier des oppositions`)
      for (const line of opposition) {
        if (line[0].match(/[0-9]{8}/)) {
          const date = `${line[0].substr(0, 4)}-${line[0].substr(4, 2)}-${line[0].substr(6, 2)}`
          const params = {
            qs: `dateMort:${date} AND codeVilleDeces:${line[1]} AND numeroActeDeces:${line[2]}`
          }
          const inconsistencyLine = (await axios.get(`api/v1/datasets/${dataset.id}/lines`, { params })).data
          if (inconsistencyLine.total === 1) {
            const line = inconsistencyLine.results[0]
            line._action = 'delete'
            delete line._score
            delete line._rand
            delete line._i
            delete line._updatedAt
            toUpdate.push(line)
            stats.remove++
          } else if (inconsistencyLine.total > 1) {
            console.log(inconsistencyLine)
            await log.info('Impossible de déterminer la personne à supprimer')
          }
        }
      }

      await log.info(`Suppression de ${stats.remove} ligne(s) suite à demande d'opposition`)

      await log.info(`Suppression de ${filePath}`)
      try {
        await fs.remove(filePath)
      } catch (err) {
        await log.info(`${err.status}, ${err.statusText}`)
      }
    } catch (err) {
      await log.info(`${err.status}, ${err.statusText}`)
    }
  }

  try {
    if (toUpdate.length > 0) await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, toUpdate)
  } catch (err) {
    await log.info(`${err.status}, ${err.statusText} ${JSON.stringify(err.data.errors)}`)
  }
}

module.exports = async (tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, pluginConfig, processingConfig, dataset, axios, log) => {
  if (processingConfig.datasetMode !== 'inconsistency') {
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
        if (processingConfig.datasetMode === 'create') {
          if (parseInt(file.title.substr(6, 4)) >= processingConfig.startYear) downloadFile.push({ title: file.title, url: file.url })
        } else {
          downloadFile.push({ title: file.title, url: file.url })
        }
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
                const sendTab = parseLines(linesTab, refCodeInseeComm, refCodeInseePays, keysRef, processingConfig)
                linesTab = []
                const sizeTab = sendTab.length
                await log.info(`envoi de ${sizeTab} lignes vers le jeu de données`)
                while (sendTab.length) {
                  // split the array into smaller arrays to avoid too heavy request
                  const lines = sendTab.splice(0, 3000)
                  try {
                    await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
                  } catch (err) {
                    await log.info(`${err.status}, ${err.statusText} ${err.errors.error[0]}`)
                  }
                }
              }
              next()
            },
            flush: async (callback) => {
              if (linesTab.length > 0) {
                const sendTab = parseLines(linesTab, refCodeInseeComm, refCodeInseePays, keysRef, processingConfig)
                await log.info(`envoi de ${sendTab.length} lignes vers le jeu de données`)
                while (sendTab.length) {
                  // split the array into smaller arrays to avoid too much requests
                  const lines = sendTab.splice(0, 3000)
                  try {
                    await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
                  } catch (err) {
                    await log.info(`${err.status}, ${err.statusText}`)
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
            await log.info(`${err.status}, ${err.statusText}`)
          }
        }
      }
    } catch (err) {
      await log.info(`${err.status}, ${err.statusText}`)
    }
  } else {
    try {
      await updateInconsistency(tmpDir, pluginConfig, processingConfig, dataset, axios, log)
    } catch (err) {
      console.log(err)
      await log.info(`${err.status}, ${err.statusText}`)
    }
  }
}
