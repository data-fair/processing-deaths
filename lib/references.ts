import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { Commune, References } from './process.ts'

type SchemaField = { key: string, 'x-refersTo'?: string }
type DatasetRef = { id: string, title?: string }

const CONCEPTS = {
  code: { uri: 'http://rdf.insee.fr/def/geo#codeCommune', label: 'Code commune INSEE' },
  nom: { uri: 'http://schema.org/City', label: 'Commune' },
  codeDepartement: { uri: 'http://rdf.insee.fr/def/geo#codeDepartement', label: 'Code département INSEE' },
  codeRegion: { uri: 'http://rdf.insee.fr/def/geo#codeRegion', label: 'Code région INSEE' }
}
// columns read by name, the reference datasets carry no concept for them
const NOM_DEPARTEMENT = 'nom_departement'
const NOM_REGION = 'nom_region'

/** Every line of a dataset, following the `next` links */
const readAllLines = async (axios: AxiosInstance, datasetId: string, select?: string): Promise<Record<string, any>[]> => {
  const lines: Record<string, any>[] = []
  let page = (await axios.get(`api/v1/datasets/${datasetId}/lines`, { params: { size: 10000, ...(select ? { select } : {}) } })).data
  lines.push(...page.results)
  while (page.next && page.results.length) {
    page = (await axios.get(page.next)).data
    lines.push(...page.results)
  }
  return lines
}

const str = (v: unknown) => v === undefined || v === null ? '' : String(v).trim()

/**
 * Find the columns of the communes reference dataset from its concepts.
 * Throws with the list of what is missing.
 */
export const communeKeys = (schema: SchemaField[]): Record<keyof typeof CONCEPTS, string> & { nomDepartement: string, nomRegion: string } => {
  const missing: string[] = []
  const keys = {} as Record<keyof typeof CONCEPTS, string>
  for (const [name, { uri, label }] of Object.entries(CONCEPTS) as [keyof typeof CONCEPTS, { uri: string, label: string }][]) {
    const field = schema.find(f => f['x-refersTo'] === uri)
    if (field) keys[name] = field.key
    else missing.push(`le concept « ${label} »`)
  }
  for (const column of [NOM_DEPARTEMENT, NOM_REGION]) {
    if (!schema.some(f => f.key === column)) missing.push(`la colonne « ${column} »`)
  }
  if (missing.length) throw new Error(`Le jeu de données de référence des communes n'a pas ${missing.join(', ')}`)
  return { ...keys, nomDepartement: NOM_DEPARTEMENT, nomRegion: NOM_REGION }
}

type CodeChange = { COM_AV?: unknown, COM_AP?: unknown, DATE_EFF?: unknown }

/**
 * Map each former commune code to the current commune it ended in.
 * A commune can change several times (merged into a commune nouvelle, merged again later): the most recent
 * change of each code is followed until a current commune is reached. Resolved once here, so the lookup of
 * each death line stays a single map read.
 */
export const resolveNewCodes = (changes: CodeChange[], communes: Map<string, unknown>): Map<string, string> => {
  const latest = new Map<string, string>()
  const sorted = changes
    .map(c => ({ av: str(c.COM_AV), ap: str(c.COM_AP), date: str(c.DATE_EFF) }))
    .filter(c => c.av && c.ap && c.av !== c.ap)
    .sort((a, b) => a.date < b.date ? -1 : a.date > b.date ? 1 : 0)
  for (const { av, ap } of sorted) latest.set(av, ap)

  const newCodes = new Map<string, string>()
  for (const av of latest.keys()) {
    if (communes.has(av)) continue
    const seen = new Set<string>()
    let code = av
    while (!communes.has(code) && latest.has(code) && !seen.has(code)) {
      seen.add(code)
      code = latest.get(code)!
    }
    if (communes.has(code)) newCodes.set(av, code)
  }
  return newCodes
}

export default async (processingConfig: ProcessingConfig, axios: AxiosInstance, log: LogFunctions): Promise<References> => {
  await log.step('Récupération des jeux de données de référence')

  const communesDataset = processingConfig.datasetInsee as DatasetRef
  const schema: SchemaField[] = (await axios.get(`api/v1/datasets/${communesDataset.id}/schema`)).data
  const keys = communeKeys(schema)
  const communes = new Map<string, Commune>()
  for (const line of await readAllLines(axios, communesDataset.id, Object.values(keys).join(','))) {
    const code = str(line[keys.code])
    if (!code) continue
    communes.set(code, {
      code,
      nom: str(line[keys.nom]),
      codeDepartement: str(line[keys.codeDepartement]),
      nomDepartement: str(line[keys.nomDepartement]),
      codeRegion: str(line[keys.codeRegion]),
      nomRegion: str(line[keys.nomRegion])
    })
  }
  await log.info(`${communes.size} communes dans « ${communesDataset.title ?? communesDataset.id} »`)

  let newCodes = new Map<string, string>()
  const codesDataset = processingConfig.datasetCodesActuels as DatasetRef | undefined
  if (codesDataset?.id) {
    newCodes = resolveNewCodes(await readAllLines(axios, codesDataset.id), communes)
    await log.info(`${newCodes.size} anciens codes de communes dans « ${codesDataset.title ?? codesDataset.id} »`)
  } else {
    await log.warning('Aucun jeu de données des codes actuels des communes : les anciens codes ne seront pas rattachés')
  }

  return { communes, newCodes }
}
