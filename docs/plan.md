# Plan d'implémentation — NF26 BGES

_Dernière mise à jour : 2026-05-07_

---

## Vue d'ensemble

```
Phase 1 — Infrastructure        [x]
Phase 2 — Dimensions initiales  [x]
Phase 3 — ETL journalier        [ ]
Phase 4 — Analyse & KPIs        [ ]
```

---

## Phase 1 — Infrastructure

Tout le reste dépend de ces fichiers. À faire en premier.

| Tâche | Fichier | Statut |
|---|---|---|
| SparkSession factory | `src/utils/spark.py` | [x] |
| Constantes sites + chemins | `src/config/settings.py` | [x] |
| StructType de toutes les tables | `src/models/schemas.py` | [x] |
| Classe abstraite ETLJob | `src/etl/job.py` | [x] |
| Lecteurs CSV (`;` et `,`) | `src/etl/extract/readers.py` | [x] |

---

## Phase 2 — Dimensions initiales (`jobs/initial_load.py`)

Tables statiques, chargées une seule fois. Pas de paramètre date.

| Tâche | Fichier | Statut |
|---|---|---|
| DIM_DATE (généré 2026-01-01→2027-12-31) | `src/etl/transform/dim_date.py` | [x] |
| DIM_TRANSPORT_TYPE (hard-codé + facteurs CO2) | `src/etl/transform/dim_transport_type.py` | [x] |
| DIM_EQUIPMENT (CO2 ref CSV) | `src/etl/transform/dim_equipment.py` | [x] |
| DIM_CITY (depuis PERSONNEL × 6 sites) | `src/etl/transform/dim_city.py` | [x] |
| DIM_STAFF (PERSONNEL × 6 sites + normalisation) | `src/etl/transform/dim_staff.py` | [x] |
| Orchestrateur initial load | `src/jobs/initial_load.py` | [x] |
| Notebook validation initial load | `notebooks/01_initial_load.ipynb` | [x] |

**Normalisations à gérer dans DIM_STAFF** :
- `FONCTION_PERSONNEL` → 5 valeurs canoniques (multilingue)
- `BIRTH_DATE` depuis `DT_NAISS`

---

## Phase 3 — ETL journalier (`jobs/daily_load.py`)

Prend `--date-debut YYYY-MM-DD` et `--date-fin YYYY-MM-DD` en paramètres CLI.
Pas de checkpoint, pas de Parquet (à envisager comme amélioration future).

| Tâche | Fichier | Statut |
|---|---|---|
| Geocodage villes → coordonnées (run 1x) | `src/utils/geocoding.py` | [ ] |
| UDF haversine distance (km) | `src/utils/distance.py` | [ ] |
| Formules CO2 missions | `src/utils/co2.py` | [ ] |
| DIM_TRIP (paires villes + distance) | `src/etl/transform/dim_trip.py` | [ ] |
| FACT_MISSION (CO2 = dist × facteur × AR) | `src/etl/transform/fact_mission.py` | [ ] |
| FACT_EQUIPMENT (CO2 = lookup ref + fallback) | `src/etl/transform/fact_equipment.py` | [ ] |
| Normalisation TYPE_MISSION (multilingue) | dans `fact_mission.py` | [ ] |
| CLI date-debut / date-fin | `src/jobs/daily_load.py` | [ ] |
| Notebook daily load | `notebooks/02_daily_load.ipynb` | [ ] |

**Paramètres CLI** :
```bash
uv run python src/jobs/daily_load.py --date-debut 2026-04-29 --date-fin 2026-05-07
```

---

## Phase 4 — Analyse & KPIs

| Tâche | Fichier | Statut |
|---|---|---|
| Exploration sources brutes | `notebooks/00_exploration.ipynb` | [ ] |
| Questions Q1–Q17 (réponses chiffrées) | `notebooks/03_questions.ipynb` | [ ] |
| Q18 — Top 5 missions Paris | `notebooks/04_visualisations.ipynb` | [ ] |
| Q19 — Impact mensuel par transport et site | `notebooks/04_visualisations.ipynb` | [ ] |
| Q20 — Impact global mensuel | `notebooks/04_visualisations.ipynb` | [ ] |
| Slides résumé (≤ 10) | à faire manuellement | [ ] |
| Formulaire questions PDF rempli | à faire manuellement | [ ] |

---

## Améliorations futures (hors scope MVP)

- Écriture Parquet vers `warehouse/` pour persister les résultats
- Checkpoint automatique (ne retraiter que les dates manquantes)
- Tests unitaires sur les transformations

---

## Décisions d'architecture

| Date | Décision | Raison |
|---|---|---|
| 2026-05-07 | Pas de Parquet warehouse dans le MVP | Simplification ; résultats en mémoire suffisent pour répondre aux questions |
| 2026-05-07 | Pas de checkpoint JSON | Date-début/fin passées en CLI, relance explicite sur la plage voulue |
| 2026-05-07 | PERSONNEL + ref matériel = statiques | Pas de recrutement ni de nouveaux types de matériel pendant la période |
| 2026-05-07 | geopy.distance.geodesic pour distances | Recommandé par les slides (Labos1point5). Géocodage Nominatim run 1x, coordonnées en cache JSON |
| 2026-05-07 | Avion : 2 facteurs CO2 selon distance | Court-courrier < 1000 km : 0.230 kg/km ; long-courrier ≥ 1000 km : 0.187 kg/km (ADEME + RF) |
| 2026-05-07 | CO2 stocké en kg, reporté en tCO₂e | Cohérent avec le schéma (CO2_IMPACT_KG) ; diviser par 1000 dans les KPIs |
