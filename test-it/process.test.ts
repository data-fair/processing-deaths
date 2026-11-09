import { describe, it } from 'node:test'
import assert from 'assert'
import type { AxiosInstance } from 'axios'
import { parseLine, parseDate, deathFiles, sendBatch, retryDelay, type References } from '../lib/process.ts'

retryDelay.ms = 0
const log = { step: async () => {}, task: async () => {}, progress: async () => {}, info: async () => {}, error: async () => {}, warning: async () => {}, debug: async () => {} }

/** Build a line of the INSEE death file (fixed width) */
export const deathLine = (p: { names: string, sexe?: string, birth?: string, birthCode?: string, birthPlace?: string, birthCountry?: string, death?: string, deathCode?: string, acte?: string }) =>
  p.names.padEnd(80) +
  (p.sexe ?? '1') +
  (p.birth ?? '19400512') +
  (p.birthCode ?? '35238').padEnd(5) +
  (p.birthPlace ?? 'RENNES').padEnd(30) +
  (p.birthCountry ?? '').padEnd(30) +
  (p.death ?? '20260103') +
  (p.deathCode ?? '35238').padEnd(5) +
  (p.acte ?? '123').padEnd(9)

const refs: References = {
  communes: new Map([
    ['35238', { code: '35238', nom: 'Rennes', codeDepartement: '35', nomDepartement: 'Ille-et-Vilaine', codeRegion: '53', nomRegion: 'Bretagne' }],
    ['49092', { code: '49092', nom: 'Chemillé-en-Anjou', codeDepartement: '49', nomDepartement: 'Maine-et-Loire', codeRegion: '52', nomRegion: 'Pays de la Loire' }]
  ]),
  newCodes: new Map([['49008', '49092']])
}

describe('parseDate', () => {
  it('keeps the known parts of a date', () => {
    assert.equal(parseDate('19400512'), '1940-05-12')
    assert.equal(parseDate('19400500'), '1940-05')
    assert.equal(parseDate('19400000'), '1940')
    assert.equal(parseDate('00000000'), undefined)
  })
})

describe('parseLine', () => {
  it('reads names, dates, age and current communes', () => {
    const identity = parseLine(deathLine({ names: 'DUPONT*JEAN PIERRE/' }), refs, 130)
    assert.equal(identity.nom, 'DUPONT')
    assert.equal(identity.prenom, 'JEAN PIERRE')
    assert.equal(identity.sexe, '1')
    assert.equal(identity.date_naissance, '1940-05-12')
    assert.equal(identity.date_mort, '2026-01-03')
    assert.equal(identity.age_deces, 85.642)
    assert.equal(identity.nom_actuel_ville_naissance, 'Rennes')
    assert.equal(identity.nom_actuel_region_deces, 'Bretagne')
    assert.equal(identity.pays_naissance, 'FRANCE')
    assert.equal(identity.nom_pays_deces, 'France')
    assert.equal(identity.numero_acte_deces, '123')
  })

  it('does not read past the name field for long names', () => {
    // the slash sits at the end of the 80 characters, the old parser took the sex and birth date with it
    const names = 'LE COMPASSEUR CREQUI MONTFORT DE COURTIVRON*COLETTE MARIE THERESE ANTOINETTE/'
    const identity = parseLine(deathLine({ names, sexe: '2' }), refs, 130)
    assert.equal(identity.nom, 'LE COMPASSEUR CREQUI MONTFORT DE COURTIVRON')
    assert.equal(identity.prenom, 'COLETTE MARIE THERESE ANTOINETTE')
  })

  it('follows a former commune code to the current commune', () => {
    const identity = parseLine(deathLine({ names: 'MARTIN*PAUL/', birthCode: '49008' }), refs, 130)
    assert.equal(identity.code_source_ville_naissance, '49008')
    assert.equal(identity.code_actuel_ville_naissance, '49092')
    assert.equal(identity.nom_actuel_ville_naissance, 'Chemillé-en-Anjou')
  })

  it('leaves the current commune empty when the code is unknown', () => {
    const identity = parseLine(deathLine({ names: 'MARTIN*PAUL/', birthCode: '00000' }), refs, 130)
    assert.equal(identity.code_actuel_ville_naissance, '')
    assert.equal(identity.nom_actuel_departement_naissance, '')
  })

  it('names the country of a death abroad as it was at the date of death', () => {
    const before = parseLine(deathLine({ names: 'MARTIN*PAUL/', death: '19850304', deathCode: '99123' }), refs, 130)
    assert.equal(before.nom_pays_deces, 'URSS')
    const after = parseLine(deathLine({ names: 'MARTIN*PAUL/', death: '20200304', deathCode: '99123' }), refs, 130)
    assert.equal(after.nom_pays_deces, 'Russie')
    assert.equal(after.code_actuel_ville_deces, '')
  })

  it('keeps the birth country of the source file', () => {
    const identity = parseLine(deathLine({ names: 'MARTIN*PAUL/', birthCode: '99131', birthCountry: 'BELGIQUE' }), refs, 130)
    assert.equal(identity.pays_naissance, 'BELGIQUE')
  })

  it('drops an incoherent age and the birth date that produced it', () => {
    const identity = parseLine(deathLine({ names: 'MARTIN*PAUL/', birth: '18500101' }), refs, 130)
    assert.equal(identity.age_deces, undefined)
    assert.equal(identity.date_naissance, undefined)
  })
})

describe('deathFiles', () => {
  it('keeps the death files, newest first', () => {
    assert.deepEqual(
      deathFiles(['deces-2024.txt', 'fichier-opposition-deces.csv', 'deces-2026-m01.txt', 'deces-2025.txt', 'deces-2026-m02.txt', 'notes.txt']),
      ['deces-2026-m02.txt', 'deces-2026-m01.txt', 'deces-2025.txt', 'deces-2024.txt']
    )
  })
})

describe('sendBatch', () => {
  const fakeAxios = (responses: ({ status: number } | { data: any })[]) => {
    let calls = 0
    const axios = {
      post: async () => {
        calls++
        const res = responses.shift() ?? { data: { nbErrors: 0 } }
        if ('status' in res) throw Object.assign(new Error('http error'), { status: res.status })
        return res
      }
    } as unknown as AxiosInstance
    return { axios, calls: () => calls }
  }

  it('retries a transient failure', async () => {
    const { axios, calls } = fakeAxios([{ status: 503 }, { data: { nbErrors: 0 } }])
    await sendBatch(axios, log, 'ds', [{ nom: 'A' }])
    assert.equal(calls(), 2)
  })

  it('gives up after 3 attempts', async () => {
    const { axios, calls } = fakeAxios([{ status: 429 }, { status: 429 }, { status: 429 }])
    await assert.rejects(sendBatch(axios, log, 'ds', [{ nom: 'A' }]))
    assert.equal(calls(), 3)
  })

  it('does not retry a client error', async () => {
    const { axios, calls } = fakeAxios([{ status: 400 }])
    await assert.rejects(sendBatch(axios, log, 'ds', [{ nom: 'A' }]))
    assert.equal(calls(), 1)
  })
})
