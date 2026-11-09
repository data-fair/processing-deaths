import data from './countries.json' with { type: 'json' }

export type CountryPeriod = { label: string, start: string, end?: string }
/** INSEE country code (99xxx) → current label (absent for a country that no longer exists) and labels over time */
export type Countries = Record<string, { label?: string, history: CountryPeriod[] }>

const countries: Countries = data.countries

/**
 * Name of a foreign country from its INSEE code (99xxx), as it was at the given date.
 * The death file goes back to 1970: a death in 1985 under code 99123 is in "URSS", not "Russie".
 * The date may be partial (YYYY or YYYY-MM) or missing; the current name is the fallback.
 */
export const countryName = (code: string, date?: string): string => {
  const country = countries[code]
  if (!country) return ''
  if (date) {
    // a partial date stands for its first day, enough to pick a period
    const day = date.length === 4 ? `${date}-01-01` : date.length === 7 ? `${date}-01` : date
    const period = country.history.find(p => p.start <= day && (!p.end || day < p.end))
    if (period) return period.label
  }
  return country.label ?? country.history[country.history.length - 1]?.label ?? ''
}
