const fs = require('fs');
const path = require('path')
const endOfLine = require('os').EOL
const datasetSchema = require('./schema.json')
//const datasetSchemaCode = require('./schemacode.json')
const InseeCode = require('insee-municipality-code')
//const csvtojsonV2 = require("csvtojson");

module.exports = async (tmpDir, log) => {

  await log.step('Traitement des fichiers')
  const outFile = await fs.promises.open(path.join(tmpDir, 'deces.csv'), 'w')
  await outFile.write(datasetSchema.map(f => `"${f.key}"`).join(',') + endOfLine)
  let identity;
  fs.readdir(tmpDir, function (err, files) {
    if (err) {
      console.log(err);
    } else {
      files.forEach(function forFile(file) {
        fs.readFile(file, function (error, result) {

          if (error === null && file.match('.txt')) {
            console.log('Traitement du fichier' + file)
            var data = result.toString().split('\n')
            for (var i = 0; i < data.length - 1; i++) {
              var code_date = data[i].match(/[0-9][0-9|A|B]{12,15}[A-Z]*/g)
              var code_naissance = code_date[0].toString()
              var code_mort = code_date[1].toString()
              identity = {
                nom: data[i].match('\^[A-Z, ,-]*').toString(),
                prenom: data[i].match('\\*.+?\/') === null ? ' ' : data[i].match('\\*.+?\/').toString().replace('\*', '').replace('\/', ''),
                genre: code_naissance.charAt(0) === '1' ? 'Homme' : 'Femme',
                code_ville_naissance: code_naissance.substr(9, 5),
                nom_ville_naissance: '"' + data[i].match('[0-9][^0-9]+?(?= {3,})').toString().substring(1).trim() + '"',
                pays_naissance: '',
                age_deces: '',
                date_naissance: code_naissance.substr(1, 8),
                date_mort: code_mort.substring(0, 8),
                code_ville_deces: code_mort.substr(8, 5),
                nom_ville_deces: '',
                numero_acte_deces: code_mort.substr(13)
              }

              const date1 = identity.date_naissance
              var mois_naissance = date1.slice(4, 6)
              var jour_naissance = date1.slice(6, 8)
              if (mois_naissance === '00') {
                mois_naissance = date1.substr(4, 2).replace('00', '01')

              }
              if (jour_naissance === '00') {
                jour_naissance = date1.substr(4, 2).replace('00', '01')

              }
              identity.date_naissance = date1.slice(0, 4) + "-" + mois_naissance + "-" + jour_naissance

              const date2 = identity.date_mort
              var mois_deces = date2.slice(4, 6)
              var jour_deces = date2.slice(6, 8)
              if (mois_deces === '00') {
                mois_deces = date2.substr(4, 2).replace('00', '01')

              }
              if (jour_deces === '00') {
                jour_deces = date2.substr(4, 2).replace('00', '01')

              }
              identity.date_mort = date2.slice(0, 4) + "-" + mois_deces + "-" + jour_deces
              identity.age_deces = getAge(identity.date_naissance, identity.date_mort)

              if (identity.code_ville_deces != '') {
                if (InseeCode.getMunicipality(identity.code_ville_deces) != null) {
                  identity.nom_ville_deces = '"' + InseeCode.getMunicipality(identity.code_ville_deces).name + '"'
                }
              }

              if (identity.code_ville_naissance.substr(0, 3).match('[[6][9][3]|[7][5][1]|[1][3]]') != null) {
                identity.nom_ville_naissance = InseeCode.getMunicipality(identity.code_ville_naissance).name

              }
              if (code_naissance.substr(9, 5).startsWith('99')) {
                if (InseeCode.getMunicipality(identity.code_ville_naissance) != null) {
                  identity.pays_naissance = '"' + InseeCode.getMunicipality(identity.code_ville_naissance).name + '"'
                }
              } else {
                identity.pays_naissance = 'FRANCE'
              }
              outFile.write(Object.values(identity).join(',') + endOfLine)

              //console.log(identity)
            }

          } else {
            console.log(error)
          }

        })
      })
    }
  })
  console.log(Object.values(identity).join(',') + endOfLine)
  await outFile.close()


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


