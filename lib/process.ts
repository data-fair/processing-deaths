import fs from 'fs-extra'
import path from 'node:path'
import readline from 'node:readline'
import dayjs from 'dayjs'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { countryName } from './countries.ts'

let shouldBeStopped = false
export const setShouldBeStopped = (v: boolean) => { shouldBeStopped = v }
export const isStopped = () => shouldBeStopped

/** A current commune, read from the reference dataset */
export type Commune = { code: string, nom: string, codeDepartement: string, nomDepartement: string, codeRegion: string, nomRegion: string }

export type References = {
  /** current communes by INSEE code */
  communes: Map<string, Commune>
  /** former INSEE code → current code */
  newCodes: Map<string, string>
}

export type Identity = {
  nom: string
  prenom: string
  sexe: string
  date_naissance?: string
  date_mort?: string
  code_source_ville_naissance: string
  nom_source_ville_naissance: string
  code_actuel_ville_naissance: string
  nom_actuel_ville_naissance: string
  code_actuel_departement_naissance: string
  nom_actuel_departement_naissance: string
  code_actuel_region_naissance: string
  nom_actuel_region_naissance: string
  pays_naissance: string
  age_deces?: number | ''
  code_source_ville_deces: string
  code_actuel_ville_deces: string
  nom_actuel_ville_deces: string
  code_actuel_departement_deces: string
  nom_actuel_departement_deces: string
  code_actuel_region_deces: string
  nom_actuel_region_deces: string
  nom_pays_deces: string
  numero_acte_deces: string
}

/** YYYYMMDD with unknown parts as 00 → YYYY, YYYY-MM, YYYY-MM-DD or undefined */
export const parseDate = (raw: string): string | undefined => {
  const year = raw.slice(0, 4).padStart(4, '0')
  const month = raw.slice(4, 6)
  const day = raw.slice(6, 8)
  if (year === '0000') return undefined
  if (month === '00') return year
  if (day === '00') return `${year}-${month}`
  return `${year}-${month}-${day}`
}

/** The current commune of an INSEE code, directly or through its former code */
const findCommune = (code: string, refs: References): Commune | undefined =>
  refs.communes.get(code) ?? refs.communes.get(refs.newCodes.get(code) ?? code)

/**
 * Parse one line of the INSEE death file (fixed width, see the file documentation on data.gouv.fr).
 * The first 80 characters hold `NOM*PRENOMS/` padded with spaces.
 */
export const parseLine = (line: string, refs: References, maxAge: number): Identity => {
  const names = line.slice(0, 80)
  const star = names.indexOf('*')
  const slash = names.indexOf('/', star)
  const nom = (star === -1 ? names : names.slice(0, star)).trim()
  const prenom = star === -1 ? '' : names.slice(star + 1, slash === -1 ? undefined : slash).trim()

  const identity: Identity = {
    nom,
    prenom,
    sexe: line[80],
    date_naissance: parseDate(line.slice(81, 89).trim()),
    date_mort: parseDate(line.slice(154, 162).trim()),
    code_source_ville_naissance: line.slice(89, 94).trim(),
    nom_source_ville_naissance: line.slice(94, 124).trim(),
    code_actuel_ville_naissance: '',
    nom_actuel_ville_naissance: '',
    code_actuel_departement_naissance: '',
    nom_actuel_departement_naissance: '',
    code_actuel_region_naissance: '',
    nom_actuel_region_naissance: '',
    pays_naissance: line.slice(124, 154).trim(),
    age_deces: '',
    code_source_ville_deces: line.slice(162, 167).trim(),
    code_actuel_ville_deces: '',
    nom_actuel_ville_deces: '',
    code_actuel_departement_deces: '',
    nom_actuel_departement_deces: '',
    code_actuel_region_deces: '',
    nom_actuel_region_deces: '',
    nom_pays_deces: '',
    numero_acte_deces: line.slice(167, 176).trim()
  }

  if (identity.date_mort && identity.date_naissance) {
    const age = parseFloat(dayjs(identity.date_mort).diff(dayjs(identity.date_naissance), 'year', true).toFixed(3))
    if (age >= 0 && age <= maxAge) {
      identity.age_deces = age
    } else {
      identity.age_deces = undefined
      identity.date_naissance = undefined
    }
  }

  // foreign places have a 99xxx code, named as the country was at the date of death
  identity.nom_pays_deces = identity.code_source_ville_deces.startsWith('99')
    ? countryName(identity.code_source_ville_deces, identity.date_mort)
    : 'France'

  const naissance = findCommune(identity.code_source_ville_naissance, refs)
  if (naissance) {
    identity.code_actuel_ville_naissance = naissance.code
    identity.nom_actuel_ville_naissance = naissance.nom
    identity.code_actuel_departement_naissance = naissance.codeDepartement
    identity.nom_actuel_departement_naissance = naissance.nomDepartement
    identity.code_actuel_region_naissance = naissance.codeRegion
    identity.nom_actuel_region_naissance = naissance.nomRegion
  }
  const deces = findCommune(identity.code_source_ville_deces, refs)
  if (deces) {
    identity.code_actuel_ville_deces = deces.code
    identity.nom_actuel_ville_deces = deces.nom
    identity.code_actuel_departement_deces = deces.codeDepartement
    identity.nom_actuel_departement_deces = deces.nomDepartement
    identity.code_actuel_region_deces = deces.codeRegion
    identity.nom_actuel_region_deces = deces.nomRegion
  }

  // a birth in France has no country in the source file
  if (!/[A-Z]/.test(identity.pays_naissance)) identity.pays_naissance = 'FRANCE'
  return identity
}

type BulkLine = Record<string, unknown>
type BulkError = { line: number, status: number, error?: string }

export const MAX_ATTEMPTS = 3
export const retryDelay = { ms: 10000 }
// 429 and 5xx are overloads (e.g. an elasticsearch circuit breaker), worth another try
const isTransient = (status?: number) => status === 429 || (status !== undefined && status >= 500)

/**
 * Send one batch of lines to the _bulk_lines endpoint, retrying transient failures:
 * POST is not retried by the context axios, and every action here is idempotent.
 * Lines rejected one by one (validation) are logged, they do not stop the run.
 */
export const sendBatch = async (axios: AxiosInstance, log: LogFunctions, datasetId: string, lines: BulkLine[]): Promise<void> => {
  for (let attempt = 1; ; attempt++) {
    let data: { nbErrors?: number, errors?: BulkError[] }
    try {
      data = (await axios.post(`api/v1/datasets/${datasetId}/_bulk_lines`, lines)).data
    } catch (err: any) {
      const status = err.status ?? err.response?.status
      if (attempt >= MAX_ATTEMPTS || !isTransient(status)) throw err
      await log.warning(`erreur ${status} à l'envoi des lignes, nouvel essai dans ${retryDelay.ms / 1000}s`)
      await new Promise(resolve => setTimeout(resolve, retryDelay.ms))
      continue
    }
    if (data.nbErrors) await log.warning(`${data.nbErrors} ligne(s) refusée(s) sur ${lines.length}`, data.errors)
    return
  }
}

export const READ_BATCH = 15000
export const SEND_BATCH = 3000

const sendLines = async (lines: string[], refs: References, maxAge: number, datasetId: string, axios: AxiosInstance, log: LogFunctions) => {
  const identities = lines.map(line => parseLine(line, refs, maxAge))
  await log.info(`envoi de ${identities.length} lignes vers le jeu de données`)
  for (let i = 0; i < identities.length; i += SEND_BATCH) {
    await sendBatch(axios, log, datasetId, identities.slice(i, i + SEND_BATCH))
  }
}

/** The yearly and monthly death files of tmpDir, newest first */
export const deathFiles = (files: string[]): string[] =>
  files.filter(f => /^deces-\d{4}(-m\d{2})?\.txt$/.test(f)).sort().reverse()

/** Read the downloaded death files and send their lines to the dataset */
export default async (tmpDir: string, refs: References, maxAge: number, datasetId: string, axios: AxiosInstance, log: LogFunctions): Promise<void> => {
  const files = deathFiles(await fs.readdir(tmpDir))
  await log.step(`Traitement des ${files.length} fichier(s)`)
  for (const file of files) {
    if (shouldBeStopped) return
    await log.info(`Traitement du fichier ${file}`)
    const input = fs.createReadStream(path.join(tmpDir, file), { encoding: 'utf8' })
    let batch: string[] = []
    try {
      for await (const line of readline.createInterface({ input, crlfDelay: Infinity })) {
        if (!line.trim()) continue
        batch.push(line)
        if (batch.length >= READ_BATCH) {
          await sendLines(batch, refs, maxAge, datasetId, axios, log)
          batch = []
          if (shouldBeStopped) return
        }
      }
      if (batch.length) await sendLines(batch, refs, maxAge, datasetId, axios, log)
    } finally {
      input.destroy()
    }
  }
}
