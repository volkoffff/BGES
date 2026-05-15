"""Generate notebooks/03_anwsers.ipynb from scratch."""

import json
import uuid
from pathlib import Path


def _cell_id() -> str:
    """Retourne un identifiant de cellule unique."""
    return str(uuid.uuid4())[:8]


def md(source: str) -> dict:
    """Crée une cellule Markdown."""
    return {
        "cell_type": "markdown",
        "id": _cell_id(),
        "metadata": {},
        "source": source,
    }


def code(source: str) -> dict:
    """Crée une cellule de code."""
    return {
        "cell_type": "code",
        "execution_count": None,
        "id": _cell_id(),
        "metadata": {},
        "outputs": [],
        "source": source,
    }


CELLS: list[dict] = [
    # ── 01 — Titre ───────────────────────────────────────────────────────────
    md("""# 03 — Réponses aux questions BGES

Réponses aux 20 questions du projet NF26 — Bilan Gaz à Effet de Serre.

Plage d'analyse : **2026-05-01 → 2026-10-31** (tous les 6 sites)."""),

    # ── 02a — Path setup (doit s'exécuter avant les imports projet) ──────────
    code("""\
import os
from pathlib import Path
import sys
import tomllib

os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

_cfg_file: Path = Path("../config/config.toml")
_project_root: Path = _cfg_file.parent.parent

with _cfg_file.open("rb") as _f:
    _cfg = tomllib.load(_f)

_src_path: Path = (_project_root / _cfg["paths"]["src_path"]).resolve()
_data_path: Path = (_project_root / _cfg["paths"]["data_path"]).resolve()

# Doit être exécuté AVANT tout import de module du projet (config, jobs, utils…)
sys.path.insert(0, str(_src_path))\
"""),

    # ── 02b — Imports projet + SparkSession ──────────────────────────────────
    code("""\
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from config.settings import ETLConfig
from jobs.daily_load import DailyTables, run_daily_load
from jobs.initial_load import InitialTables, run_initial_load
from utils.spark import get_spark

spark: SparkSession = get_spark()
spark.sparkContext.setLogLevel("WARN")
config: ETLConfig = ETLConfig(data_path=_data_path)\
"""),

    # ── 03 — ETL load ────────────────────────────────────────────────────────
    code("""\
initial: InitialTables = run_initial_load(spark, config)

DATE_DEBUT: date = date(2026, 5, 1)
DATE_FIN:   date = date(2026, 10, 31)

daily: DailyTables = run_daily_load(spark, config, DATE_DEBUT, DATE_FIN, initial)
print("ETL OK")\
"""),

    # ── 04 — DataFrames nommés + vues SQL ────────────────────────────────────
    code("""\
# ── Dimensions (initial_load) ──────────────────────────────────────────────
sdf_dim_date:           DataFrame = initial.dim_date
sdf_dim_staff:          DataFrame = initial.dim_staff
sdf_dim_equipment:      DataFrame = initial.dim_equipment
sdf_dim_transport_type: DataFrame = initial.dim_transport_type

# ── Dimensions (daily_load) ────────────────────────────────────────────────
sdf_dim_city: DataFrame = daily.dim_city   # 6 sites + toutes les villes de mission
sdf_dim_trip: DataFrame = daily.dim_trip

# ── Tables de faits (daily_load) ───────────────────────────────────────────
sdf_fact_mission:   DataFrame = daily.fact_mission
sdf_fact_equipment: DataFrame = daily.fact_equipment

_tables: dict[str, DataFrame] = {
    "dim_date":           sdf_dim_date,
    "dim_staff":          sdf_dim_staff,
    "dim_equipment":      sdf_dim_equipment,
    "dim_transport_type": sdf_dim_transport_type,
    "dim_city":           sdf_dim_city,
    "dim_trip":           sdf_dim_trip,
    "fact_mission":       sdf_fact_mission,
    "fact_equipment":     sdf_fact_equipment,
}

for _name, _sdf in _tables.items():
    _sdf.createOrReplaceTempView(_name)

print(f"{'Table':<25} {'Lignes':>8}  Colonnes")
print("-" * 80)
for _name, _sdf in _tables.items():
    print(f"{_name:<25} {_sdf.count():>8}  {', '.join(_sdf.columns)}")\
"""),

    # ── 05 — Head de toutes les tables ───────────────────────────────────────
    code("""\
for _name, _sdf in _tables.items():
    print(f"\\n{'=' * 70}")
    print(_name.upper())
    print("=" * 70)
    _sdf.show(5, truncate=False)\
"""),

    # ── 06 — DataFrames dénormalisés pour les requêtes ───────────────────────
    code("""\
# Constantes pour les sites (SK_CITY = SK_SITE dans DIM_STAFF)
SITE_BERLIN:   int = 1
SITE_LONDON:   int = 2
SITE_LA:       int = 3
SITE_NY:       int = 4
SITE_PARIS:    int = 5
SITE_SHANGHAI: int = 6

EU_SITES: list[int] = [SITE_BERLIN, SITE_LONDON, SITE_PARIS]
US_SITES: list[int] = [SITE_LA, SITE_NY]

# DIM_STAFF enrichi du nom de site
sdf_staff_site: DataFrame = sdf_dim_staff.join(
    sdf_dim_city.select(
        F.col("SK_CITY").alias("SK_SITE"),
        F.col("CITY_NAME").alias("SITE_NAME"),
    ),
    "SK_SITE",
)

# sdf_missions : table de faits missions dénormalisée (toutes dimensions jointes)
sdf_missions: DataFrame = (
    sdf_fact_mission
    .join(
        sdf_dim_date.select(F.col("SK_DATE").alias("SK_DATE_MISSION"), "DATE_ISO"),
        "SK_DATE_MISSION",
    )
    .withColumn("YEAR",  F.year(F.col("DATE_ISO")))
    .withColumn("MONTH", F.month(F.col("DATE_ISO")))
    .withColumn("DAY",   F.dayofmonth(F.col("DATE_ISO")))
    .join(
        sdf_staff_site.select(
            "SK_STAFF", "JOB_TITLE", "ACTIVITY_SECTOR", "BIRTH_DATE",
            "SK_SITE", "SITE_NAME",
        ),
        "SK_STAFF",
    )
    .join(
        sdf_dim_transport_type.select("SK_TRANSPORT_TYPE", "TRANSPORT_NAME"),
        "SK_TRANSPORT_TYPE",
    )
    .join(
        sdf_dim_trip.select(
            "SK_TRIP", "SK_CITY_ORIGIN", "SK_CITY_DESTINATION", "DISTANCE_KM"
        ),
        "SK_TRIP",
    )
    .join(
        sdf_dim_city.select(
            F.col("SK_CITY").alias("SK_CITY_DESTINATION"),
            F.col("CITY_NAME").alias("DESTINATION_NAME"),
            F.col("IS_ORG_SITE").alias("DEST_IS_ORG_SITE"),
        ),
        "SK_CITY_DESTINATION",
    )
    .cache()
)

# sdf_equipment : table de faits équipements dénormalisée
sdf_equipment: DataFrame = (
    sdf_fact_equipment
    .join(
        sdf_dim_date.select(F.col("SK_DATE").alias("SK_DATE_PURCHASE"), "DATE_ISO"),
        "SK_DATE_PURCHASE",
    )
    .withColumn("YEAR",  F.year(F.col("DATE_ISO")))
    .withColumn("MONTH", F.month(F.col("DATE_ISO")))
    .withColumn("DAY",   F.dayofmonth(F.col("DATE_ISO")))
    .join(
        sdf_staff_site.select(
            "SK_STAFF", "JOB_TITLE", "ACTIVITY_SECTOR", "SK_SITE", "SITE_NAME"
        ),
        "SK_STAFF",
    )
    .join(
        sdf_dim_equipment.select(
            "SK_EQUIPMENT", "TYPE", "MODEL", "CO2_IMPACT_KG_REF"
        ),
        "SK_EQUIPMENT",
    )
    .cache()
)

print(f"sdf_missions  : {sdf_missions.count()} lignes, colonnes : {sdf_missions.columns}")
print(f"sdf_equipment : {sdf_equipment.count()} lignes, colonnes : {sdf_equipment.columns}")\
"""),

    # ══════════════════════════════════════════════════════════════════════════
    # Section 1 — Premières questions
    # ══════════════════════════════════════════════════════════════════════════
    md("## Section 1 — Premières questions"),

    # Q1
    md("### Q1 — Combien de cadres travaillent sur le site de Paris ?"),
    code("""\
q1: int = (
    sdf_dim_staff
    .filter((F.col("JOB_TITLE") == "Cadre") & (F.col("SK_SITE") == SITE_PARIS))
    .count()
)
print(f"Q1 — Cadres sur le site de Paris : {q1}")\
"""),

    # Q2
    md("### Q2 — Combien d'ingénieurs Data travaillent sur les sites aux États-Unis ?"),
    code("""\
q2: int = (
    sdf_dim_staff
    .filter(
        (F.col("JOB_TITLE") == "Ingénieur Data") &
        F.col("SK_SITE").isin(US_SITES)
    )
    .count()
)
print(f"Q2 — Ingénieurs Data aux États-Unis : {q2}")\
"""),

    # Q3
    md("### Q3 — Combien d'ingénieurs informaticiens travaillent dans l'organisation (tous sites compris) ?"),
    code("""\
q3: int = (
    sdf_dim_staff
    .filter(F.col("JOB_TITLE") == "Ingénieur Informaticien")
    .count()
)
print(f"Q3 — Ingénieurs Informaticiens (tous sites) : {q3}")\
"""),

    # Q4
    md("### Q4 — Combien de PC fixes ont été achetés par l'organisation entre juin et septembre 2026 ?"),
    code("""\
q4: int = (
    sdf_equipment
    .filter(
        F.col("TYPE").like("PC fixe%") &
        F.col("MONTH").between(6, 9)
    )
    .count()
)
print(f"Q4 — PC fixes achetés (juin–septembre 2026) : {q4}")\
"""),

    # Q5
    md("### Q5 — Quelle a été l'impact carbone des PC fixes sans écran entre mai et octobre 2026 ?"),
    code("""\
q5: float = (
    sdf_equipment
    .filter(F.col("TYPE") == "PC fixe sans ecran")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q5 — Impact carbone PC fixes sans écran (mai–oct 2026) : {q5 / 1000:.3f} tCO₂e ({q5:.1f} kg)")\
"""),

    # Q6
    md("### Q6 — Quelle a été l'impact carbone des PC portables achetés par les ingénieurs Data entre mai et octobre 2026 sur les sites de Londres et New-York ?"),
    code("""\
q6: float = (
    sdf_equipment
    .filter(
        (F.col("TYPE") == "PC portable") &
        (F.col("JOB_TITLE") == "Ingénieur Data") &
        F.col("SK_SITE").isin([SITE_LONDON, SITE_NY])
    )
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q6 — Impact carbone PC portables Ingénieurs Data (Londres + NY, mai–oct) : {q6 / 1000:.3f} tCO₂e")\
"""),

    # Q7
    md("### Q7 — Quelle a été l'impact carbone des Écrans achetés par les cadres entre juillet et septembre 2026 sur tous les sites de l'organisation ?"),
    code("""\
q7: float = (
    sdf_equipment
    .filter(
        (F.col("TYPE") == "Ecran") &
        (F.col("JOB_TITLE") == "Cadre") &
        F.col("MONTH").between(7, 9)
    )
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q7 — Impact carbone Écrans / Cadres (juil–sept 2026) : {q7 / 1000:.3f} tCO₂e")\
"""),

    # Q8
    md("### Q8 — Quelle a été l'impact carbone des missions sur les sites Européens entre mai et octobre 2026 ?"),
    code("""\
q8: float = (
    sdf_missions
    .filter(F.col("SK_SITE").isin(EU_SITES))
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q8 — Impact carbone missions sites Européens (mai–oct 2026) : {q8 / 1000:.3f} tCO₂e")\
"""),

    # Q9
    md("### Q9 — Quels ont été les 5 jours les plus impactants concernant les missions en avion pour les sites Européens de l'organisation ?"),
    code("""\
print("Q9 — Top 5 jours les plus impactants (missions en avion, sites EU) :")
(
    sdf_missions
    .filter(
        F.col("SK_SITE").isin(EU_SITES) &
        F.col("TRANSPORT_NAME").like("Avion%")
    )
    .groupBy("DATE_ISO")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .orderBy(F.col("CO2_KG").desc())
    .select(
        "DATE_ISO",
        F.round("CO2_KG", 2).alias("CO2_KG"),
        F.round(F.col("CO2_KG") / 1000, 3).alias("tCO2e"),
    )
    .show(5)
)\
"""),

    # Q10
    md("### Q10 — Quel a été le secteur d'activité qui a eu le plus d'impact concernant les missions et le matériel informatique sur l'ensemble des sites de l'organisation ?"),
    code("""\
sdf_co2_mission_sector: DataFrame = (
    sdf_missions
    .groupBy("ACTIVITY_SECTOR")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_MISSION_KG"))
)
sdf_co2_equip_sector: DataFrame = (
    sdf_equipment
    .groupBy("ACTIVITY_SECTOR")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_EQUIP_KG"))
)
print("Q10 — Impact CO₂ total (missions + équipements) par secteur d'activité :")
(
    sdf_co2_mission_sector
    .join(sdf_co2_equip_sector, "ACTIVITY_SECTOR", "outer")
    .withColumn(
        "CO2_TOTAL_KG",
        F.coalesce(F.col("CO2_MISSION_KG"), F.lit(0.0))
        + F.coalesce(F.col("CO2_EQUIP_KG"), F.lit(0.0)),
    )
    .orderBy(F.col("CO2_TOTAL_KG").desc())
    .select(
        "ACTIVITY_SECTOR",
        F.round("CO2_MISSION_KG", 1).alias("CO2_MISSION_KG"),
        F.round("CO2_EQUIP_KG", 1).alias("CO2_EQUIP_KG"),
        F.round("CO2_TOTAL_KG", 1).alias("CO2_TOTAL_KG"),
    )
    .show()
)\
"""),

    # Q11
    md("### Q11 — Quel site a eu le plus d'impact concernant les missions et le matériel informatique sur l'ensemble des sites de l'organisation ?"),
    code("""\
sdf_co2_mission_site: DataFrame = (
    sdf_missions
    .groupBy("SITE_NAME")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_MISSION_KG"))
)
sdf_co2_equip_site: DataFrame = (
    sdf_equipment
    .groupBy("SITE_NAME")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_EQUIP_KG"))
)
print("Q11 — Impact CO₂ total (missions + équipements) par site :")
(
    sdf_co2_mission_site
    .join(sdf_co2_equip_site, "SITE_NAME", "outer")
    .withColumn(
        "CO2_TOTAL_KG",
        F.coalesce(F.col("CO2_MISSION_KG"), F.lit(0.0))
        + F.coalesce(F.col("CO2_EQUIP_KG"), F.lit(0.0)),
    )
    .orderBy(F.col("CO2_TOTAL_KG").desc())
    .select(
        "SITE_NAME",
        F.round("CO2_MISSION_KG", 1).alias("CO2_MISSION_KG"),
        F.round("CO2_EQUIP_KG", 1).alias("CO2_EQUIP_KG"),
        F.round("CO2_TOTAL_KG", 1).alias("CO2_TOTAL_KG"),
    )
    .show()
)\
"""),

    # Q12
    md("### Q12 — Quel a été l'impact carbone des missions reliant chaque site (départ = site org, arrivée = site org) durant le mois de septembre 2026 ?"),
    code("""\
# Les deux extrémités du trajet doivent être des sites de l'organisation.
sdf_org_origins: DataFrame = sdf_dim_city.filter(F.col("IS_ORG_SITE")).select(
    F.col("SK_CITY").alias("SK_CITY_ORIGIN")
)
q12: float = (
    sdf_missions
    .filter(F.col("MONTH") == 9)
    .filter(F.col("DEST_IS_ORG_SITE"))
    .join(sdf_org_origins, "SK_CITY_ORIGIN")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q12 — Impact carbone missions inter-sites (septembre 2026) : {q12 / 1000:.3f} tCO₂e")\
"""),

    # Q13
    md("### Q13 — Quel a été l'impact carbone des séminaires en juillet 2026 pour les employés de Los Angeles ?"),
    code("""\
# NB : le type 'Séminaire' n'apparaît pas dans les données (MISSION_TYPE connus :
# Conférence, Développement, Formation, Rencontre entreprises, Réunion).
# Le résultat sera 0 si aucune mission de ce type n'existe dans la plage.
q13: float = (
    sdf_missions
    .filter(
        (F.col("MISSION_TYPE") == "Séminaire") &
        (F.col("MONTH") == 7) &
        (F.col("SK_SITE") == SITE_LA)
    )
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .first()[0] or 0.0
)
print(f"Q13 — Impact carbone séminaires Los Angeles (juillet 2026) : {q13 / 1000:.3f} tCO₂e")\
"""),

    # Q14
    md('### Q14 — Quel secteur d\'activité a été le plus impactant pour les missions "conférences" entre mai et septembre 2026 ?'),
    code("""\
print("Q14 — Impact CO₂ missions 'Conférence' par secteur d'activité (mai–sept 2026) :")
(
    sdf_missions
    .filter(
        (F.col("MISSION_TYPE") == "Conférence") &
        F.col("MONTH").between(5, 9)
    )
    .groupBy("ACTIVITY_SECTOR")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .orderBy(F.col("CO2_KG").desc())
    .select("ACTIVITY_SECTOR", F.round("CO2_KG", 1).alias("CO2_KG"))
    .show()
)\
"""),

    # Q15
    md("### Q15 — Quel a été l'âge moyen des employés Ingénieurs Data qui sont partis en formations entre juillet et septembre 2026 ?"),
    code("""\
# L'âge est calculé par rapport à la date de la mission (DATE_ISO).
q15_row = (
    sdf_missions
    .filter(
        (F.col("JOB_TITLE") == "Ingénieur Data") &
        (F.col("MISSION_TYPE") == "Formation") &
        F.col("MONTH").between(7, 9)
    )
    .select(
        F.floor(
            F.months_between(F.col("DATE_ISO"), F.col("BIRTH_DATE")) / 12
        ).alias("AGE")
    )
    .agg(F.avg("AGE").alias("AGE_MOYEN"))
    .first()
)
q15: float = q15_row["AGE_MOYEN"] or 0.0
print(f"Q15 — Âge moyen Ingénieurs Data en formation (juil–sept 2026) : {q15:.1f} ans")\
"""),

    # Q16
    md("### Q16 — Quelle destination a été la plus impactante (en cumul) entre mai et octobre 2026 ?"),
    code("""\
print("Q16 — Destinations les plus impactantes (mai–oct 2026) :")
(
    sdf_missions
    .groupBy("DESTINATION_NAME")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .orderBy(F.col("CO2_KG").desc())
    .select("DESTINATION_NAME", F.round("CO2_KG", 1).alias("CO2_KG"))
    .show(5)
)\
"""),

    # Q17
    md("### Q17 — Quelles ont été les trois catégories de missions les plus impactantes pour les cadres dans les sites Européens en mai 2026 ?"),
    code("""\
print("Q17 — Top 3 types de missions (Cadres, sites EU, mai 2026) :")
(
    sdf_missions
    .filter(
        (F.col("JOB_TITLE") == "Cadre") &
        F.col("SK_SITE").isin(EU_SITES) &
        (F.col("MONTH") == 5)
    )
    .groupBy("MISSION_TYPE")
    .agg(F.sum("CO2_IMPACT_KG").alias("CO2_KG"))
    .orderBy(F.col("CO2_KG").desc())
    .select("MISSION_TYPE", F.round("CO2_KG", 1).alias("CO2_KG"))
    .show(3)
)\
"""),

    # ══════════════════════════════════════════════════════════════════════════
    # Section 2 — Questions avec illustrations
    # ══════════════════════════════════════════════════════════════════════════
    md("## Section 2 — Questions avec illustrations"),

    # Q18
    md("### Q18 — Quelles ont été les 5 missions les plus impactantes sur le site de Paris ?"),
    code("""\
import matplotlib.pyplot as plt

sdf_top5_paris: DataFrame = (
    sdf_missions
    .filter(F.col("SK_SITE") == SITE_PARIS)
    .orderBy(F.col("CO2_IMPACT_KG").desc())
    .limit(5)
)
print("Q18 — Top 5 missions les plus impactantes (Paris) :")
sdf_top5_paris.select(
    "NK_MISSION", "MISSION_TYPE", "TRANSPORT_NAME",
    F.round("CO2_IMPACT_KG", 1).alias("CO2_KG"),
).show(truncate=False)

pdf_top5: "pd.DataFrame" = sdf_top5_paris.select(
    "NK_MISSION",
    F.round("CO2_IMPACT_KG", 1).alias("CO2_KG"),
).toPandas()

import pandas as pd

fig, ax = plt.subplots(figsize=(9, 4))
ax.barh(pdf_top5["NK_MISSION"], pdf_top5["CO2_KG"])
ax.set_xlabel("CO₂ (kg)")
ax.set_title("Q18 — 5 missions les plus impactantes — Paris")
ax.invert_yaxis()
plt.tight_layout()
plt.show()\
"""),

    # Q19
    md("### Q19 — Figure comparant l'impact carbone mensuel des missions en fonction du type de transport et sur chaque site"),
    code("""\
import matplotlib.pyplot as plt
import pandas as pd

pdf_q19: pd.DataFrame = (
    sdf_missions
    .groupBy("SITE_NAME", "MONTH", "TRANSPORT_NAME")
    .agg(F.round(F.sum("CO2_IMPACT_KG") / 1000, 2).alias("tCO2e"))
    .orderBy("SITE_NAME", "MONTH")
    .toPandas()
)

sites: list[str] = sorted(pdf_q19["SITE_NAME"].unique())
transports: list[str] = sorted(pdf_q19["TRANSPORT_NAME"].unique())
months: list[int] = sorted(pdf_q19["MONTH"].unique())

fig, axes = plt.subplots(2, 3, figsize=(16, 9), sharey=False)
axes_flat = axes.flatten()

for ax, site in zip(axes_flat, sites):
    pdf_site: pd.DataFrame = pdf_q19[pdf_q19["SITE_NAME"] == site]
    pivot: pd.DataFrame = (
        pdf_site.pivot_table(
            index="MONTH", columns="TRANSPORT_NAME", values="tCO2e", aggfunc="sum"
        )
        .reindex(months)
        .fillna(0)
    )
    pivot.plot(kind="bar", ax=ax, legend=False)
    ax.set_title(site)
    ax.set_xlabel("Mois")
    ax.set_ylabel("tCO₂e")
    ax.set_xticklabels([str(m) for m in months], rotation=0)

handles, labels = axes_flat[0].get_legend_handles_labels()
fig.legend(handles, labels, loc="lower center", ncol=3, title="Transport")
fig.suptitle("Q19 — Impact carbone mensuel des missions par transport et par site", fontsize=13)
plt.tight_layout(rect=[0, 0.08, 1, 1])
plt.show()\
"""),

    # Q20
    md("### Q20 — Figure illustrant l'impact carbone global mensuel de l'organisation"),
    code("""\
import matplotlib.pyplot as plt
import pandas as pd

pdf_miss_month: pd.DataFrame = (
    sdf_missions
    .groupBy("MONTH")
    .agg(F.round(F.sum("CO2_IMPACT_KG") / 1000, 2).alias("tCO2e_missions"))
    .toPandas()
)
pdf_equip_month: pd.DataFrame = (
    sdf_equipment
    .groupBy("MONTH")
    .agg(F.round(F.sum("CO2_IMPACT_KG") / 1000, 2).alias("tCO2e_equipements"))
    .toPandas()
)
pdf_q20: pd.DataFrame = (
    pdf_miss_month
    .merge(pdf_equip_month, on="MONTH", how="outer")
    .fillna(0)
    .sort_values("MONTH")
    .reset_index(drop=True)
)
pdf_q20["tCO2e_total"] = pdf_q20["tCO2e_missions"] + pdf_q20["tCO2e_equipements"]

fig, ax = plt.subplots(figsize=(10, 5))
x = pdf_q20["MONTH"]
ax.bar(x - 0.2, pdf_q20["tCO2e_missions"],    width=0.35, label="Missions")
ax.bar(x + 0.15, pdf_q20["tCO2e_equipements"], width=0.35, label="Équipements")
ax.plot(x, pdf_q20["tCO2e_total"], marker="o", color="black", label="Total")
ax.set_xlabel("Mois")
ax.set_ylabel("tCO₂e")
ax.set_title("Q20 — Impact carbone global mensuel de l'organisation (mai–oct 2026)")
ax.set_xticks(x)
ax.legend()
plt.tight_layout()
plt.show()\
"""),
]

NOTEBOOK: dict = {
    "nbformat": 4,
    "nbformat_minor": 5,
    "metadata": {
        "kernelspec": {
            "display_name": "Python 3",
            "language": "python",
            "name": "python3",
        },
        "language_info": {"name": "python", "version": "3.12.0"},
    },
    "cells": CELLS,
}

out_path = Path(__file__).parent / "notebooks" / "03_anwsers.ipynb"
out_path.write_text(json.dumps(NOTEBOOK, indent=1, ensure_ascii=False), encoding="utf-8")
print(f"Notebook écrit : {out_path}  ({len(CELLS)} cellules)")
