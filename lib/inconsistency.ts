import fs from 'fs-extra'
import path from 'node:path'
import dayjs from 'dayjs'
import { parse } from 'csv-parse/sync'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { isStopped, sendBatch, SEND_BATCH } from './process.ts'

type Line = Record<string, any>

const OPPOSITION_FILE = 'fichier-opposition-deces.csv'

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
 * Remove the incoherent ages and dates, then delete the people listed in the opposition file
 * (people who asked not to appear, identified by date of death, place code and record number).
 */
export default async (tmpDir: string, maxAge: number, datasetId: string, axios: AxiosInstance, log: LogFunctions): Promise<void> => {
  await log.step('Vérification des données incohérentes et demandes d\'opposition')
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
    let removed = 0
    for (const row of rows) {
      if (isStopped()) return
      const [date, code, acte] = row.map(v => v.trim())
      if (!/^\d{8}$/.test(date ?? '')) continue
      const qs = `date_mort:"${date.slice(0, 4)}-${date.slice(4, 6)}-${date.slice(6, 8)}" AND code_source_ville_deces:"${code}" AND numero_acte_deces:"${acte}"`
      const found = (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { qs } })).data
      if (found.total === 1) {
        toUpdate.push({ ...clean(found.results[0]), _action: 'delete' })
        removed++
      } else if (found.total > 1) {
        await log.info(`Impossible de déterminer la personne à supprimer (${date}, ${code}, ${acte})`)
      }
    }
    await log.info(`Suppression de ${removed} ligne(s) suite à une demande d'opposition`)
  }

  if (isStopped()) return
  for (let i = 0; i < toUpdate.length; i += SEND_BATCH) {
    await sendBatch(axios, log, datasetId, toUpdate.slice(i, i + SEND_BATCH))
  }
}
