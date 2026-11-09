/**
 * Regenerate lib/countries.json from the INSEE "Code officiel géographique" country files.
 *
 * The list barely changes from one year to the next, so it ships with the plugin instead of
 * living in a reference dataset. Run it once a year when INSEE publishes a new vintage:
 *
 *   npm run update-countries -- <current list url> <list since 1943 url>
 *
 * Both files are listed on https://www.data.gouv.fr/datasets/code-officiel-geographique-cog
 * ("Pays et territoires étrangers au 01/01/YYYY" and "... depuis 1943").
 */
import fs from 'node:fs/promises'
import path from 'node:path'
import { parse } from 'csv-parse/sync'
import type { Countries } from '../lib/countries.ts'

const [currentUrl = 'https://www.insee.fr/fr/statistiques/fichier/8740222/v_pays_territoire_2026.csv',
  historyUrl = 'https://www.insee.fr/fr/statistiques/fichier/8740222/v_pays_et_territoire_depuis_1943.csv'] = process.argv.slice(2)

const fetchCsv = async (url: string): Promise<Record<string, string>[]> => {
  const res = await fetch(url)
  if (!res.ok) throw new Error(`${res.status} ${res.statusText} (${url})`)
  return parse(await res.text(), { columns: true, bom: true, skip_empty_lines: true })
}

const current = await fetchCsv(currentUrl)
const history = await fetchCsv(historyUrl)
for (const [name, rows, columns] of [['current', current, ['COG', 'LIBCOG']], ['history', history, ['COG', 'LIBCOG', 'DATE_DEBUT', 'DATE_FIN']]] as const) {
  const missing = columns.filter(c => !(c in (rows[0] ?? {})))
  if (missing.length) throw new Error(`${name} file misses the columns ${missing.join(', ')}`)
}

const countries: Countries = {}
for (const row of history) {
  const country = countries[row.COG] ??= { history: [] }
  country.history.push({ label: row.LIBCOG, start: row.DATE_DEBUT, ...(row.DATE_FIN ? { end: row.DATE_FIN } : {}) })
}
for (const row of current) {
  (countries[row.COG] ??= { history: [] }).label = row.LIBCOG
}
for (const country of Object.values(countries)) country.history.sort((a, b) => a.start.localeCompare(b.start))

const sorted = Object.fromEntries(Object.entries(countries)
  .sort(([a], [b]) => a.localeCompare(b))
  .map(([code, { label, history }]) => [code, { ...(label ? { label } : {}), history }]))
const out = path.join(import.meta.dirname, '../lib/countries.json')
await fs.writeFile(out, JSON.stringify({ sources: [currentUrl, historyUrl], countries: sorted }, null, 2) + '\n')
console.log(`${Object.keys(sorted).length} countries written to ${out}`)
