import fs from 'fs-extra'
import path from 'node:path'
import { pipeline } from 'node:stream/promises'
import type { AxiosInstance } from 'axios'
import type { LogFunctions } from '@data-fair/lib-common-types/processings.js'
import { isStopped } from './process.ts'

export const OPPOSITION_FILE = 'fichier-opposition-deces.csv'

type Resource = { title: string, url: string, format?: string }

const DATAGOUV_API = 'https://www.data.gouv.fr/api/1/datasets/'
// data.gouv.fr serves its files through redirects, refused by default by the context axios
const externalOpts = { maxRedirects: 5 }

/** The opposition file: its title and its file name change with each version, only its CSV format is stable */
const isOpposition = ({ url, format }: Resource) =>
  format?.toLowerCase() === 'csv' || /\.csv$/i.test(new URL(url).pathname)

/**
 * Pick the files to download among the resources of the data.gouv.fr dataset:
 * - the yearly files `deces-YYYY.txt` from `startYear` on,
 * - the monthly files `deces-YYYY-mMM.txt` of the latest year (not yet in a yearly file),
 * - the opposition file (the only CSV resource, titled "Fichier des oppositions… - Version du YYYYMMDD"),
 *   read by the inconsistency step.
 * With `startYear` undefined, only the opposition files are kept.
 */
export const selectResources = (resources: Resource[], startYear?: number): string[] => {
  const years = resources.map(r => r.title.match(/^deces-(\d{4})/)?.[1]).filter((y): y is string => !!y).map(Number)
  const latestYear = Math.max(...years)
  const urls = new Set<string>()
  for (const resource of resources) {
    const { title, url } = resource
    if (isOpposition(resource)) urls.add(url)
    if (startYear === undefined) continue
    const yearly = title.match(/^deces-(\d{4})\.txt$/)
    if (yearly && Number(yearly[1]) >= startYear) urls.add(url)
    const monthly = title.match(/^deces-(\d{4})-m\d{2}\.txt$/)
    if (monthly && Number(monthly[1]) === latestYear && latestYear >= startYear) urls.add(url)
  }
  return [...urls]
}

export default async (datasetID: string, startYear: number | undefined, tmpDir: string, axios: AxiosInstance, log: LogFunctions): Promise<void> => {
  await log.step('Téléchargement des données')
  const resources: Resource[] = (await axios.get(DATAGOUV_API + encodeURIComponent(datasetID) + '/', externalOpts)).data.resources
  const urls = selectResources(resources, startYear)
  await log.info(`${urls.length} fichier(s) à télécharger`)

  await fs.ensureDir(tmpDir)
  for (const url of urls) {
    if (isStopped()) return
    // the name comes from remote content, keep only its last segment;
    // the opposition file is saved under a fixed name, its own name holds its version date
    const resource = resources.find(r => r.url === url)
    const fileName = resource && isOpposition(resource) ? OPPOSITION_FILE : path.basename(new URL(url).pathname)
    await log.info(`Téléchargement de ${fileName}`)
    const res = await axios.get(url, { ...externalOpts, responseType: 'stream' })
    await pipeline(res.data, fs.createWriteStream(path.join(tmpDir, fileName)))
  }
  await log.info('Téléchargement terminé')
}
