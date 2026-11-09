import fs from 'fs-extra'
import path from 'node:path'
import dayjs from 'dayjs'
import { parse } from 'csv-parse/sync'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { isStopped, sendBatch, SEND_BATCH } from './process.ts'
import { OPPOSITION_FILE } from './download.ts'

type Line = Record<string, any>

/** Drop the technical fields returned by /lines, they are refused by _bulk_lines */
const clean = ({ _score, _rand, _i, _updatedAt, ...line }: Line): Line => line

const sameDeath = (a: Line, b: Line) =>
  a.nom === b.nom && a.prenom === b.prenom && a.numero_acte_deces === b.numero_acte_deces

/**
 * Lines with an incoherent age or date: the faulty fields are removed.
 * The `update` action replaces the whole line, so a field left out is erased.
 */
export const fixInconsistentLines = (weirdAge: Line[], weirdBirth: Line[], weirdDeath: Line[]): Line[] => {
  const unique: Line[] = []
  for (const line of [...weirdAge, ...weirdDeath, ...weirdBirth]) {
    if (!unique.some(u => sameDeath(u, line))) unique.push(line)
  }
  return unique.map(line => {
    const fixed: Line = { ...clean(line), _action: 'update' }
    delete fixed.age_deces
    if (weirdBirth.some(w => sameDeath(w, line))) delete fixed.date_naissance
    if (weirdDeath.some(w => sameDeath(w, line))) delete fixed.date_mort
    return fixed
  })
}

/**
 * Lines whose first name swallowed the next fields (sex, dates, place), written by the parser before
 * v1: `COLETTE MARIE THERESE      21928072721208COURTIVRON`. Names hold no digit and the overflow always
 * starts with the sex digit, so the first name is what comes before the first digit.
 * The first name is part of the primary key, hence of the line id: the polluted line is deleted
 * and the fixed one created (or updated when a later import already created it).
 */
export const fixFirstNames = (lines: Line[]): Line[] => lines.flatMap(line => {
  const prenom = String(line.prenom ?? '').split(/\d/)[0].trim()
  if (prenom === line.prenom) return []
  const { _id, ...fixed } = clean(line)
  return [{ _id, _action: 'delete' }, { ...fixed, prenom, _action: 'createOrUpdate' }]
})

/** Above this number of lines, an opposition request is an anomaly and nothing is deleted */
export const MAX_OPPOSITION_MATCHES = 10

/**
 * Query string of a row of the opposition file (date of death, place code, record number).
 * Empty fields are left out of the query (some rows have a blank place code). `complete` tells if the 3 fields are set.
 */
export const oppositionQuery = (row: string[]): { qs: string, complete: boolean } | undefined => {
  const [date, code, acte] = row.map(v => v.trim().replace(/"/g, ''))
  if (!/^\d{8}$/.test(date ?? '')) return undefined
  const terms = [`date_mort:"${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}"`]
  if (code) terms.push(`code_source_ville_deces:"${code}"`)
  if (acte) terms.push(`numero_acte_deces:"${acte}"`)
  return terms.length > 1 ? { qs: terms.join(' AND '), complete: terms.length === 3 } : undefined
}

/**
 * Lines to delete for an opposition request, undefined when the request is ambiguous.
 * Several lines for a complete request are deleted together: mostly the same person published twice (a record
 * corrected by INSEE gets a new primary key), sometimes deaths abroad sharing the placeholder record number 999;
 * keeping a person who asked not to appear is worse than hiding another one.
 * An incomplete request (blank field) matches deaths all over France: only a single match is deleted.
 */
export const oppositionMatches = (complete: boolean, total: number, results: Line[]): Line[] | undefined => {
  if (total <= 1) return results
  if (complete && total <= MAX_OPPOSITION_MATCHES) return results
  return undefined
}

/** Every line matching a query string, following the `next` links */
const searchAll = async (axios: AxiosInstance, datasetId: string, qs: string): Promise<Line[]> => {
  const lines: Line[] = []
  let page = (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { qs, size: 10000 } })).data
  lines.push(...page.results)
  while (page.next && page.results.length) {
    page = (await axios.get(page.next)).data
    lines.push(...page.results)
  }
  return lines
}

/**
 * Fix the polluted first names, remove the incoherent ages and dates, then delete the people listed in the opposition file
 * (people who asked not to appear, identified by date of death, place code and record number).
 */
export default async (tmpDir: string, maxAge: number, datasetId: string, axios: AxiosInstance, log: LogFunctions): Promise<void> => {
  await log.step('Vérification des données incohérentes et demandes d\'opposition')

  const firstNames = fixFirstNames(await searchAll(axios, datasetId, 'prenom:/.*[0-9].*/'))
  await log.info(`${firstNames.length / 2} prénom(s) débordant sur les colonnes suivantes à corriger`)
  for (let i = 0; i < firstNames.length; i += SEND_BATCH) {
    if (isStopped()) return
    await sendBatch(axios, log, datasetId, firstNames.slice(i, i + SEND_BATCH))
  }

  const today = dayjs().format('YYYY-MM-DD')
  const search = async (qs: string): Promise<Line[]> =>
    (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { qs, size: 10000 } })).data.results
  const weirdAge = await search(`age_deces:(<0 OR >${maxAge})`)
  const weirdBirth = await search(`date_naissance:([* TO 1800-01-01] OR [${today} TO *])`)
  const weirdDeath = await search(`date_mort:([* TO 1800-01-01] OR [${today} TO *])`)
  await log.info(`${weirdAge.length} ligne(s) avec un âge incohérent (<0 ou >${maxAge})`)
  await log.info(`${weirdBirth.length} ligne(s) avec une date de naissance incohérente (<1800 ou >${today})`)
  await log.info(`${weirdDeath.length} ligne(s) avec une date de décès incohérente (<1800 ou >${today})`)

  const toUpdate = fixInconsistentLines(weirdAge, weirdBirth, weirdDeath)
  await log.info(`Suppression des champs incohérents de ${toUpdate.length} ligne(s)`)

  const filePath = path.join(tmpDir, OPPOSITION_FILE)
  if (await fs.pathExists(filePath)) {
    const rows: string[][] = parse(await fs.readFile(filePath), { delimiter: ';', relax_column_count: true })
    await log.info(`Traitement des ${rows.length - 1} lignes du fichier des oppositions`)
    const deleted = new Set<string>()
    let requests = 0
    let multiple = 0
    const ambiguous: string[] = []
    for (const row of rows) {
      if (isStopped()) return
      const query = oppositionQuery(row)
      if (!query) continue
      const found = (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { qs: query.qs, size: MAX_OPPOSITION_MATCHES + 1 } })).data
      const matches = oppositionMatches(query.complete, found.total, found.results)
      if (!matches) {
        ambiguous.push(`${row.map(v => v.trim()).join(', ')} : ${found.total} lignes`)
        continue
      }
      if (!matches.length) continue
      requests++
      if (matches.length > 1) multiple++
      for (const line of matches) {
        // two requests can find the same line
        if (deleted.has(line._id)) continue
        deleted.add(line._id)
        toUpdate.push({ ...clean(line), _action: 'delete' })
      }
    }
    await log.info(`Suppression de ${deleted.size} ligne(s) pour ${requests} demande(s) d'opposition, dont ${multiple} ayant trouvé plusieurs lignes (même personne publiée plusieurs fois, ou décès à l'étranger au numéro d'acte 999)`)
    if (ambiguous.length) {
      await log.warning(`${ambiguous.length} demande(s) d'opposition ambiguë(s) non appliquée(s) (champ vide et plusieurs lignes, ou plus de ${MAX_OPPOSITION_MATCHES} lignes)`, ambiguous)
    }
  }

  if (isStopped()) return
  for (let i = 0; i < toUpdate.length; i += SEND_BATCH) {
    await sendBatch(axios, log, datasetId, toUpdate.slice(i, i + SEND_BATCH))
  }
}
