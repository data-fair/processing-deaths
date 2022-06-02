const fs = require('fs-extra')
const path = require('path')
const util = require('util')
const pump = util.promisify(require('pump'))
const stream = require('stream')
const byline = require('byline')

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

module.exports = async (tmpDir, refCodeInseeComm, refCodeInseePays, keysRef, dataset, axios, log) => {
  await log.step('Traitement des fichiers')
  let dir = await fs.readdir(tmpDir)
  dir = dir.filter(file => file.endsWith('.txt'))
  // console.log(dir)
  let linesTab = []
  for (const file of dir) {
    await log.info(`Traitement du fichier ${file}`)
    await pump(
      byline.createStream(fs.createReadStream(path.join(tmpDir, file), { encoding: 'utf8' })),
      new stream.Transform({
        objectMode: true,
        transform: async (obj, _, next) => {
          linesTab.push(obj)
          if (linesTab.length >= 10000) {
            const sendTab = parseLines(linesTab, refCodeInseeComm, refCodeInseePays, keysRef)
            linesTab = []
            const sizeTab = sendTab.length
            await log.info(`envoi de ${sizeTab} lignes vers le jeu de données`)
            while (sendTab.length) {
              // split the array into smaller arrays to avoid too much requests
              const lines = sendTab.splice(0, 3000)
              try {
                await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
                // console.log('envoyé')
              } catch (err) {
                console.log(err.status, err.statusText)
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
                console.log(err.status, err.statusText, err.errors)
              }
            }
            linesTab = []
            callback()
          }
        }
      })
    )
  }
}
