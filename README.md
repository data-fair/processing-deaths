# @data-fair/processing-deaths

Data-fair processing plugin: imports the INSEE file of deceased persons (French death records since 1970,
published on [data.gouv.fr](https://www.data.gouv.fr/datasets/fichier-des-personnes-decedees)) into a
REST dataset, enriched with the current commune, department and region of the places of birth and death.

## Modes

- **Create**: creates the dataset and imports every yearly file from the start year (1970 by default),
  then switches the processing to update mode.
- **Update**: imports again the files of the year of the last update (yearly file, then the monthly files of
  the latest year). Lines are upserted on `nom`, `prenom`, `numero_acte_deces`.
- **Inconsistencies and oppositions**: only removes incoherent ages and dates, and deletes the people listed in
  the opposition file. Create and update modes run this step too, after the import.

## Reference data

- **Communes** (`datasetInsee`): a dataset of current communes with the concepts "Code commune INSEE",
  "Commune", "Code département INSEE", "Code région INSEE" and the columns `nom_departement` and `nom_region`
  (e.g. "Communes de France").
- **Former commune codes** (`datasetCodesActuels`, optional): columns `COM_AV` → `COM_AP`
  (e.g. "Anciens codes commune").
- **Countries**: shipped with the plugin in `lib/countries.json`, built from the INSEE COG country files
  (current list and list since 1943). A death abroad is named after the country as it was at the date of death
  (`99123` is "URSS" before 1991-12-25, "Russie" after). Refresh it once a year when INSEE publishes a new vintage:

  ```sh
  npm run update-countries -- <"Pays et territoires étrangers au 01/01/YYYY" url> <"... depuis 1943" url>
  ```

## Development

- Node 24 (`nvm use`), native TypeScript (no build step).
- `npm install`
- `npm run build-types`: generates the types from the JSON schemas.
- `npm test`: `node:test` tests.
- `npm run lint` / `npm run lint-fix`.

## Publication

Publication to the registry is done by GitHub Actions: push on `main` → staging, tag `v*` → production.
