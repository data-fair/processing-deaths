import { describe, it } from 'node:test'
import assert from 'assert'
import processingConfigSchema from '../processing-config-schema.json' with { type: 'json' }
import outputSchema from '../lib/output-schema.json' with { type: 'json' }
import { countryName } from '../lib/countries.ts'
import { selectResources } from '../lib/download.ts'
import { communeKeys, resolveNewCodes } from '../lib/references.ts'
import { fixInconsistentLines, fixFirstNames, oppositionQuery } from '../lib/inconsistency.ts'

describe('Deaths processing', () => {
  it('exposes a processing config schema for users', () => {
    assert.equal(processingConfigSchema.type, 'object')
    assert.doesNotMatch(JSON.stringify(processingConfigSchema), /"x-(display|fromUrl|itemsProp|itemTitle|itemKey)"/)
  })

  it('keeps the columns of the published dataset', () => {
    assert.deepEqual(outputSchema.map(f => f.key), [
      'nom', 'prenom', 'sexe', 'date_naissance', 'date_mort',
      'code_source_ville_naissance', 'nom_source_ville_naissance', 'code_actuel_ville_naissance', 'nom_actuel_ville_naissance',
      'code_actuel_departement_naissance', 'nom_actuel_departement_naissance', 'code_actuel_region_naissance', 'nom_actuel_region_naissance',
      'pays_naissance', 'age_deces', 'code_source_ville_deces', 'code_actuel_ville_deces', 'nom_actuel_ville_deces',
      'code_actuel_departement_deces', 'nom_actuel_departement_deces', 'code_actuel_region_deces', 'nom_actuel_region_deces',
      'nom_pays_deces', 'numero_acte_deces'
    ])
  })
})

describe('countryName', () => {
  it('uses the name valid at the date', () => {
    assert.equal(countryName('99109', '1945-06-01'), 'Allemagne')
    assert.equal(countryName('99123', '1990'), 'URSS')
    assert.equal(countryName('99123', '1992-01'), 'Russie')
  })
  it('falls back to the current name', () => {
    assert.equal(countryName('99131'), 'Belgique')
    // 99109 has no period between 1949 and 1990 (two German states then)
    assert.equal(countryName('99109', '1970-01-01'), 'Allemagne')
  })
  it('returns an empty name for an unknown code', () => {
    assert.equal(countryName('99999', '2000-01-01'), '')
  })
})

describe('selectResources', () => {
  const resources = [
    { title: 'deces-2026-m02.txt', url: 'https://x/deces-2026-m02.txt' },
    { title: 'deces-2026-m01.txt', url: 'https://x/deces-2026-m01.txt' },
    { title: 'deces-2025-m12.txt', url: 'https://x/deces-2025-m12.txt' },
    { title: 'deces-2025.txt', url: 'https://x/deces-2025.txt' },
    { title: 'deces-2024.txt', url: 'https://x/deces-2024.txt' },
    { title: 'Fichier des oppositions à la publication des décès - Version du 20261030', url: 'https://x/fichier-opposition-deces-261030.csv', format: 'csv' },
    { title: 'documentation.pdf', url: 'https://x/documentation.pdf' }
  ]
  it('keeps the yearly files from the start year, the monthly files of the latest year and the opposition file', () => {
    assert.deepEqual(selectResources(resources, 2025).sort(), [
      'https://x/deces-2025.txt', 'https://x/deces-2026-m01.txt', 'https://x/deces-2026-m02.txt', 'https://x/fichier-opposition-deces-261030.csv'
    ])
  })
  it('keeps only the opposition file without a start year', () => {
    assert.deepEqual(selectResources(resources), ['https://x/fichier-opposition-deces-261030.csv'])
  })
})

describe('communeKeys', () => {
  it('finds the columns from the concepts', () => {
    const keys = communeKeys([
      { key: 'code_commune', 'x-refersTo': 'http://rdf.insee.fr/def/geo#codeCommune' },
      { key: 'nom_commune', 'x-refersTo': 'http://schema.org/City' },
      { key: 'code_departement', 'x-refersTo': 'http://rdf.insee.fr/def/geo#codeDepartement' },
      { key: 'nom_departement' },
      { key: 'code_region', 'x-refersTo': 'http://rdf.insee.fr/def/geo#codeRegion' },
      { key: 'nom_region' }
    ])
    assert.equal(keys.code, 'code_commune')
    assert.equal(keys.nomRegion, 'nom_region')
  })
  it('lists what is missing', () => {
    assert.throws(() => communeKeys([{ key: 'nom_region' }]), /Code commune INSEE.*nom_departement/)
  })
})

describe('fixFirstNames', () => {
  it('replaces a polluted first name and leaves the others', () => {
    const polluted = { _id: 'old', _score: null, nom: 'X', prenom: 'COLETTE MARIE THERESE              21928072721208COURTIVRON', numero_acte_deces: '7' }
    const ok = { _id: 'ok', nom: 'Y', prenom: 'JEAN', numero_acte_deces: '8' }
    assert.deepEqual(fixFirstNames([polluted, ok]), [
      { _id: 'old', _action: 'delete' },
      { nom: 'X', prenom: 'COLETTE MARIE THERESE', numero_acte_deces: '7', _action: 'createOrUpdate' }
    ])
  })
})

describe('fixInconsistentLines', () => {
  it('removes the faulty fields once per person', () => {
    const line = { _id: '1', _score: 1, nom: 'A', prenom: 'B', numero_acte_deces: '1', age_deces: -3, date_naissance: '2090-01-01', date_mort: '1990-01-01' }
    const fixed = fixInconsistentLines([line], [line], [])
    assert.deepEqual(fixed, [{ _id: '1', _action: 'update', nom: 'A', prenom: 'B', numero_acte_deces: '1', date_mort: '1990-01-01' }])
  })
})

describe('resolveNewCodes', () => {
  const communes = new Map([['49261', {}], ['39190', {}], ['39302', {}], ['01138', {}]])
  it('follows a commune merged several times to the current commune', () => {
    const codes = resolveNewCodes([
      { COM_AV: '49149', COM_AP: '49261', DATE_EFF: '2019-01-01' },
      { COM_AV: '49094', COM_AP: '49149', DATE_EFF: '2016-12-15' }
    ], communes)
    assert.equal(codes.get('49094'), '49261')
    assert.equal(codes.get('49149'), '49261')
  })
  it('keeps the most recent change of a code, whatever the order of the lines', () => {
    const codes = resolveNewCodes([
      { COM_AV: '39414', COM_AP: '39190', DATE_EFF: '2019-01-01' },
      { COM_AV: '39414', COM_AP: '39302', DATE_EFF: '1973-01-01' }
    ], communes)
    assert.equal(codes.get('39414'), '39190')
  })
  it('ignores current codes, cycles and codes ending nowhere', () => {
    const codes = resolveNewCodes([
      { COM_AV: '01138', COM_AP: '01039', DATE_EFF: '2023-01-01' },
      { COM_AV: 'A', COM_AP: 'B', DATE_EFF: '2000-01-01' },
      { COM_AV: 'B', COM_AP: 'A', DATE_EFF: '2001-01-01' },
      { COM_AV: '75059', COM_AP: '75056', DATE_EFF: '1980-01-01' }
    ], communes)
    assert.deepEqual([...codes], [])
  })
})

describe('oppositionQuery', () => {
  it('searches a death by date, place and record number', () => {
    assert.equal(oppositionQuery(['20160121', '30189', '170']), 'date_mort:"2016-01-21" AND code_source_ville_deces:"30189" AND numero_acte_deces:"170"')
  })
  it('leaves out a blank place code', () => {
    assert.equal(oppositionQuery(['20100716', '     ', '01355']), 'date_mort:"2010-07-16" AND numero_acte_deces:"01355"')
  })
  it('skips the header and rows with only a date', () => {
    assert.equal(oppositionQuery(['\uFEFFDate de décès', 'Code du lieu de décès', "Numéro d'acte de décès"]), undefined)
    assert.equal(oppositionQuery(['20100716', '', '']), undefined)
  })
})
