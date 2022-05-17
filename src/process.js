const fs = require('fs-extra');
const path = require('path')
const util = require('util')
const endOfLine = require('os').EOL
const csvSync = require('csv/sync')
const datasetSchema = require('./schema.json')
const csv = require('csv')
const InseeCode = require('insee-municipality-code')
const pump = util.promisify(require('pump'))
const stream = require('stream')
const lineReader = require('line-reader')

function parseLines(lines) {
  const out = []
  for (const line of lines) {
    identity = {
      nom: line.match('\^.+?\\*').toString().replace('\*', ''),
      prenom: line.match('\\*.+?\/') === null ? ' ' : line.match('\\*.+?\/').toString().replace('\*', '').replace('\/', ''),
      genre: line[80] === '1' ? 'Homme' : 'Femme',
      code_ville_naissance: line.slice(89, 94),
      nom_ville_naissance: line.slice(94, 124).trim(),
      pays_naissance: line.slice(124, 154).trim(),
      age_deces: '',
      date_naissance: line.slice(81, 89),
      date_mort: line.slice(154, 162),
      code_ville_deces: line.slice(162, 167),
      nom_ville_deces: '',
      numero_acte_deces: line.slice(168, 176)
    }            
    const date1 = identity.date_naissance
    let mois_naissance = date1.slice(4, 6)
    let jour_naissance = date1.slice(6, 8)
    if (mois_naissance === '00') {
      mois_naissance = date1.substr(4, 2).replace('00', '01')

    }
    if (jour_naissance === '00') {
      jour_naissance = date1.substr(4, 2).replace('00', '01')

    }
    identity.date_naissance = date1.slice(0, 4) + "-" + mois_naissance + "-" + jour_naissance

    const date2 = identity.date_mort
    let mois_deces = date2.slice(4, 6)
    let jour_deces = date2.slice(6, 8)
    if (mois_deces === '00') {
      mois_deces = date2.substr(4, 2).replace('00', '01')

    }
    if (jour_deces === '00') {
      jour_deces = date2.substr(4, 2).replace('00', '01')

    }
    identity.date_mort = date2.slice(0, 4) + "-" + mois_deces + "-" + jour_deces
    identity.age_deces = getAge(identity.date_naissance, identity.date_mort)

    if (identity.code_ville_deces.match('[0-9]{5}') ) {
      if (InseeCode.getMunicipality(identity.code_ville_deces) != null) {
        identity.nom_ville_deces =InseeCode.getMunicipality(identity.code_ville_deces).name
      }
    }
    if (identity.pays_naissance.match(/[A-Z]+/g) === null) {
      identity.pays_naissance = 'FRANCE'
    }
    out.push(Object.values(identity).join(','))
  }
  return out
}

module.exports = async (tmpDir, log) => {
  await log.step('Traitement des fichiers')
  let dir = await fs.readdir(tmpDir)
  dir = dir.filter(file => file.endsWith('.txt'))
  console.log(dir)
  const outStream = fs.createWriteStream("out.csv")
  outStream.write(datasetSchema.map(f => `"${f.key}"`).join(',') + endOfLine)
  const lines = []
  for (const file of dir) {

    let readStream = fs.createReadStream(file)
    lineReader.eachLine(readStream, async function(line) {
      if ( lines.length > 10000) {
        const tabLines = parseLines(lines, outStream)
        await log.info(`envoi de ${tablines.length} lignes vers le jeu de données`)
        while (tablines.length) {
          // if (_stopped) return await log.info('interruption demandée')
          const lines = tablines.splice(0, 1000)
          const res = await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines`, lines)
          if (res.data.nbErrors) {
            log.error(`${res.data.nbErrors} échecs sur ${lines.length} lignes à insérer`, res.data.errors)
            throw new Error('échec à l\'insertion des lignes dans le jeu de données')
          }
        }
        lines.length = 0
      } else {
        lines.push(line)
      }
    })
  }
}

function getAge(FirstDate, SecondDate) {
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
