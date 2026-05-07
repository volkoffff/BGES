# Specs — NF26 Projet BGES

_Dernière mise à jour : 2026-05-07_

---

## Contexte

Projet NF26 — estimation des émissions directes de GES (CO₂ équivalent) d'une organisation internationale sur 6 sites, via un Data-Warehouse PySpark.

**Deadline livrable : mardi 19 mai 2026, 23h59 (Moodle)**

---

## Sites

| Site | Dossier source |
|---|---|
| Paris | `BDD_BGES_PARIS/` |
| Berlin | `BDD_BGES_BERLIN/` |
| London | `BDD_BGES_LONDON/` |
| New York | `BDD_BGES_NEWYORK/` |
| Los Angeles | `BDD_BGES_LOSANGELES/` |
| Shanghai | `BDD_BGES_SHANGHAI/` |

---

## Sources de données

### Fichiers statiques (chargés une seule fois)

| Fichier | Délimiteur | Rôle DWH |
|---|---|---|
| `{SITE}/PERSONNEL_{SITE}.txt` | `;` | DIM_STAFF + DIM_CITY (sites) |
| `data/materiel_informatique_impact.csv` | `,` | DIM_EQUIPMENT (CO2 ref) |

**Contrainte confirmée** : pas de recrutement ni de mutation pendant la période. Pas de nouveaux types de matériel. Ces tables sont immuables une fois chargées.

### Fichiers journaliers (chargés sur plage de dates)

| Pattern | Délimiteur | Rôle DWH |
|---|---|---|
| `{SITE}/{SITE}_MISSION/MISSION_{YYYYMMDD}.txt` | `;` | FACT_MISSION + DIM_TRIP |
| `{SITE}/{SITE}_INFORMATIQUE/MATERIEL_INFORMATIQUE_{YYYYMMDD}.txt` | `;` | FACT_EQUIPMENT |

**Plage disponible** : 2026-04-29 → 2026-11-14 (200 jours × 6 sites × 2 types = 2 400 fichiers)

### Schémas des fichiers sources

**PERSONNEL_{SITE}.txt** (`;`)
```
ID_PERSONNEL ; NOM_PERSONNEL ; PRENOM_PERSONNEL ; DT_NAISS ; VILLE_NAISS ; PAYS_NAISS ;
NUM_SECU ; IND_PAYS_NUM_TELP ; NUM_TELEPHONE ; NUM_VOIE ; DSC_VOIE ; CMPL_VOIE ;
CD_POSTAL ; VILLE ; PAYS ; FONCTION_PERSONNEL ; TS_CREATION_PERSONNEL ; TS_MAJ_PPERSONNEL
```

**MISSION_{YYYYMMDD}.txt** (`;`)
```
ID_MISSION ; ID_PERSONNEL ; NOM_PERSONNEL ; PRENOM_PERSONNEL ; DATE_MISSION ;
TYPE_MISSION ; VILLE_DEPART ; PAYS_DEPART ; VILLE_DESTINATION ; PAYS_DESTINATION ;
TRANSPORT ; ALLER_RETOUR
```

**MATERIEL_INFORMATIQUE_{YYYYMMDD}.txt** (`;`)
```
ID_MATERIELINFO ; ID_PERSONNEL ; NOM_PERSONNEL ; PRENOM_PERSONNEL ;
DATE_ACHAT ; TYPE ; MODELE
```

**materiel_informatique_impact.csv** (`,`)
```
Type , Modèle , Impact   (Impact en kg CO₂eq)
```

---

## Modèle de données (schéma flocon)

Voir `mermaid.md` pour le diagramme ER complet.

### Tables de faits

| Table | Granularité |
|---|---|
| FACT_MISSION | 1 ligne = 1 mission d'1 employé (aller ou aller-retour) |
| FACT_EQUIPMENT | 1 ligne = 1 achat de matériel par 1 employé |

### Tables de dimensions

| Table | Source | Fréquence |
|---|---|---|
| DIM_STAFF | PERSONNEL_{SITE}.txt (×6) | Initial (1x) |
| DIM_CITY | PERSONNEL (VILLE/PAYS) + MISSION (origines/destinations) | Initial + enrichi |
| DIM_EQUIPMENT | materiel_informatique_impact.csv | Initial (1x) |
| DIM_TRANSPORT_TYPE | Valeurs hard-codées + facteurs CO2 | Initial (1x) |
| DIM_DATE | Générée (plage 2026-01-01 → 2027-12-31) | Initial (1x) |
| DIM_TRIP | Paires (ville_départ, ville_destination) + distance | Enrichi à chaque run |

---

## Normalisations obligatoires

### Fonctions / secteurs d'activité (FONCTION_PERSONNEL)

| Langue source | Valeur normalisée |
|---|---|
| Ingénieur Data / Data Engineer / ... | `Ingénieur Data` |
| Ingénieur Informaticien / IT Engineer / ... | `Ingénieur Informaticien` |
| Cadre / Manager / Führungskraft / ... | `Cadre` |
| Economiste / Economist / ... | `Economiste` |
| DRH / HR Director / Personalleiter / ... | `DRH` |

### Types de missions (TYPE_MISSION)

| Langue source | Valeur normalisée |
|---|---|
| Conférence / Conference / Konferenz / ... | `Conférence` |
| Réunion / Meeting / ... | `Réunion` |
| Rencontre entreprises / Business meeting / ... | `Rencontre entreprises` |
| Formation / Training / Schulung / ... | `Formation` |
| Développement / Development / Entwicklung / ... | `Développement` |
| Séminaire / Seminar / ... | `Séminaire` |

### Aller-retour (ALLER_RETOUR)

| Valeur source | Booléen |
|---|---|
| `oui` / `yes` / `ja` / `1` | `True` |
| `non` / `no` / `nein` / `0` / `` | `False` |

---

## Calcul des distances et du CO₂

### Distance géographique

- **Bibliothèque** : `geopy.distance.geodesic` (ellipsoïde WGS-84)
- **Stratégie** : pré-calcul des coordonnées (lat, lon) de chaque ville via Nominatim,
  stockées dans DIM_CITY. Distance calculée à la création de chaque DIM_TRIP.
- **UDF PySpark** : `haversine_udf(lat1, lon1, lat2, lon2) → float (km)`
  (formule pure `math`, pas de dépendance réseau au moment de l'ETL)
- **Unité** : kilomètres (float)

### CO₂ des missions (source : Labos1point5 / ADEME Base Carbone 2024)

Formule :
```
CO2_IMPACT_KG = DISTANCE_KM × CO2_FACTOR_KG_PER_KM × (2 si ROUND_TRIP else 1)
```

Facteurs par type de transport (DIM_TRANSPORT_TYPE) :

| TRANSPORT_NAME | CO2_FACTOR_KG_PER_KM | Source |
|---|---|---|
| Avion court-courrier (< 1 000 km) | 0.230 | ADEME + effet radiatif |
| Avion long-courrier (≥ 1 000 km) | 0.187 | ADEME + effet radiatif |
| Train | 0.006 | ADEME moyenne France |
| Taxi | 0.192 | ADEME voiture thermique solo |
| Transports en commun | 0.029 | ADEME moyenne TC |

**Cas Avion** : le facteur dépend de la distance → logique conditionnelle dans `fact_mission.py`,
pas dans DIM_TRANSPORT_TYPE (on insère 2 lignes Avion ou on applique le calcul inline).

**Unité stockée** : kg CO₂eq (float) → diviser par 1 000 pour obtenir tCO₂e dans les KPIs.

### CO₂ du matériel informatique

- Valeur directement dans `materiel_informatique_impact.csv` (colonne `Impact`, en kg CO₂eq/unité)
- Jointure sur `(TYPE, MODELE)` ; fallback sur `(TYPE, "modèle par défaut")` si modèle absent
- Données manquantes (MODELE vide) → fallback sur défaut du TYPE

---

## Questions à répondre

### Section 1 — Questions analytiques (réponse chiffrée)

| N° | Question |
|---|---|
| Q1 | Nb de cadres sur le site de Paris |
| Q2 | Nb d'ingénieurs Data sur les sites US (New York + Los Angeles) |
| Q3 | Nb d'ingénieurs informaticiens dans toute l'organisation |
| Q4 | Nb de PC fixes achetés entre juin et septembre 2026 |
| Q5 | Impact carbone des PC fixes sans écran, mai–octobre 2026 |
| Q6 | Impact carbone des PC portables achetés par ingénieurs Data, Londres + New York, mai–oct 2026 |
| Q7 | Impact carbone des écrans achetés par cadres, juillet–sept 2026, tous sites |
| Q8 | Impact carbone des missions sur sites européens, mai–oct 2026 |
| Q9 | 5 jours les plus impactants (missions avion, sites européens) |
| Q10 | Secteur d'activité avec le plus d'impact (missions + matériel, tous sites) |
| Q11 | Site avec le plus d'impact (missions + matériel) |
| Q12 | Impact missions inter-sites (départ ET arrivée = site org) en septembre 2026 |
| Q13 | Impact séminaires Los Angeles, juillet 2026 |
| Q14 | Secteur le plus impactant pour conférences, mai–sept 2026 |
| Q15 | Âge moyen des ingénieurs Data partis en formations, juillet–sept 2026 |
| Q16 | Destination la plus impactante (cumul), mai–oct 2026 |
| Q17 | 3 catégories de missions les plus impactantes pour cadres, sites européens, mai 2026 |

### Section 2 — Visualisations

| N° | Visualisation |
|---|---|
| Q18 | 5 missions les plus impactantes sur Paris |
| Q19 | Impact carbone mensuel des missions par type de transport et par site |
| Q20 | Impact carbone global mensuel de l'organisation |

---

## Livrables

| Livrable | Format |
|---|---|
| Notebooks (ETL + réponses) | `.ipynb` |
| Formulaire questions rempli | PDF |
| Résumé projet | Slides (≤ 10) |

**Dépôt** : ZIP sur Moodle avant le **19 mai 2026, 23h59**
