"""PySpark StructType definitions for all DWH tables and raw CSV sources."""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

# ─── Dimension tables ─────────────────────────────────────────────────────────

DIM_DATE_SCHEMA = StructType(
    [
        StructField("SK_DATE", LongType(), nullable=False),
        StructField("DATE_ISO", DateType(), nullable=False),
        StructField("YEAR", IntegerType(), nullable=False),
        StructField("MONTH", IntegerType(), nullable=False),
        StructField("DAY", IntegerType(), nullable=False),
    ]
)

DIM_CITY_SCHEMA = StructType(
    [
        StructField("SK_CITY", LongType(), nullable=False),
        StructField("CITY_NAME", StringType(), nullable=False),
        StructField("COUNTRY_ISO2", StringType(), nullable=True),
        StructField("IS_ORG_SITE", BooleanType(), nullable=False),
        StructField("TIMEZONE_IANA", StringType(), nullable=True),
        StructField("LAT", DoubleType(), nullable=True),
        StructField("LON", DoubleType(), nullable=True),
    ]
)

DIM_STAFF_SCHEMA = StructType(
    [
        StructField("SK_STAFF", LongType(), nullable=False),
        StructField("NK_STAFF", StringType(), nullable=False),
        StructField("LAST_NAME", StringType(), nullable=True),
        StructField("FIRST_NAME", StringType(), nullable=True),
        StructField("JOB_TITLE", StringType(), nullable=True),
        StructField("BIRTH_DATE", DateType(), nullable=True),
        StructField("SK_SITE", LongType(), nullable=False),
    ]
)

DIM_EQUIPMENT_SCHEMA = StructType(
    [
        StructField("SK_EQUIPMENT", LongType(), nullable=False),
        StructField("TYPE", StringType(), nullable=False),
        StructField("MODEL", StringType(), nullable=True),
        StructField("CO2_IMPACT_KG_REF", DoubleType(), nullable=True),
    ]
)

DIM_TRANSPORT_TYPE_SCHEMA = StructType(
    [
        StructField("SK_TRANSPORT_TYPE", LongType(), nullable=False),
        StructField("TRANSPORT_NAME", StringType(), nullable=False),
        StructField("CO2_FACTOR_KG_PER_KM", DoubleType(), nullable=False),
    ]
)

DIM_TRIP_SCHEMA = StructType(
    [
        StructField("SK_TRIP", LongType(), nullable=False),
        StructField("SK_CITY_ORIGIN", LongType(), nullable=False),
        StructField("SK_CITY_DESTINATION", LongType(), nullable=False),
        StructField("DISTANCE_KM", DoubleType(), nullable=True),
    ]
)

# ─── Fact tables ──────────────────────────────────────────────────────────────

FACT_MISSION_SCHEMA = StructType(
    [
        StructField("SK_FACT_MISSION", LongType(), nullable=False),
        StructField("NK_MISSION", StringType(), nullable=False),
        StructField("SK_DATE_MISSION", LongType(), nullable=False),
        StructField("SK_STAFF", LongType(), nullable=False),
        StructField("SK_TRIP", LongType(), nullable=False),
        StructField("SK_TRANSPORT_TYPE", LongType(), nullable=False),
        StructField("MISSION_TYPE", StringType(), nullable=True),
        StructField("ROUND_TRIP_FLG", BooleanType(), nullable=False),
        StructField("CO2_IMPACT_KG", DoubleType(), nullable=True),
    ]
)

FACT_EQUIPMENT_SCHEMA = StructType(
    [
        StructField("SK_FACT_EQUIPMENT", LongType(), nullable=False),
        StructField("NK_EQUIPMENT", StringType(), nullable=False),
        StructField("SK_DATE_PURCHASE", LongType(), nullable=False),
        StructField("SK_STAFF", LongType(), nullable=False),
        StructField("SK_EQUIPMENT", LongType(), nullable=False),
        StructField("CO2_IMPACT_KG", DoubleType(), nullable=True),
    ]
)

# ─── Raw source schemas (CSV ingestion) ───────────────────────────────────────

PERSONNEL_RAW_SCHEMA = StructType(
    [
        StructField("ID_PERSONNEL", StringType(), nullable=True),
        StructField("NOM_PERSONNEL", StringType(), nullable=True),
        StructField("PRENOM_PERSONNEL", StringType(), nullable=True),
        StructField("DT_NAISS", StringType(), nullable=True),
        StructField("VILLE_NAISS", StringType(), nullable=True),
        StructField("PAYS_NAISS", StringType(), nullable=True),
        StructField("NUM_SECU", StringType(), nullable=True),
        StructField("IND_PAYS_NUM_TELP", StringType(), nullable=True),
        StructField("NUM_TELEPHONE", StringType(), nullable=True),
        StructField("NUM_VOIE", StringType(), nullable=True),
        StructField("DSC_VOIE", StringType(), nullable=True),
        StructField("CMPL_VOIE", StringType(), nullable=True),
        StructField("CD_POSTAL", StringType(), nullable=True),
        StructField("VILLE", StringType(), nullable=True),
        StructField("PAYS", StringType(), nullable=True),
        StructField("FONCTION_PERSONNEL", StringType(), nullable=True),
        StructField("TS_CREATION_PERSONNEL", StringType(), nullable=True),
        StructField("TS_MAJ_PPERSONNEL", StringType(), nullable=True),
    ]
)

MISSION_RAW_SCHEMA = StructType(
    [
        StructField("ID_MISSION", StringType(), nullable=True),
        StructField("ID_PERSONNEL", StringType(), nullable=True),
        StructField("NOM_PERSONNEL", StringType(), nullable=True),
        StructField("PRENOM_PERSONNEL", StringType(), nullable=True),
        StructField("DATE_MISSION", StringType(), nullable=True),
        StructField("TYPE_MISSION", StringType(), nullable=True),
        StructField("VILLE_DEPART", StringType(), nullable=True),
        StructField("PAYS_DEPART", StringType(), nullable=True),
        StructField("VILLE_DESTINATION", StringType(), nullable=True),
        StructField("PAYS_DESTINATION", StringType(), nullable=True),
        StructField("TRANSPORT", StringType(), nullable=True),
        StructField("ALLER_RETOUR", StringType(), nullable=True),
    ]
)

EQUIPMENT_RAW_SCHEMA = StructType(
    [
        StructField("ID_MATERIELINFO", StringType(), nullable=True),
        StructField("ID_PERSONNEL", StringType(), nullable=True),
        StructField("NOM_PERSONNEL", StringType(), nullable=True),
        StructField("PRENOM_PERSONNEL", StringType(), nullable=True),
        StructField("DATE_ACHAT", StringType(), nullable=True),
        StructField("TYPE", StringType(), nullable=True),
        StructField("MODELE", StringType(), nullable=True),
    ]
)

CO2_REF_RAW_SCHEMA = StructType(
    [
        StructField("Type", StringType(), nullable=True),
        StructField("Modèle", StringType(), nullable=True),
        StructField("Impact", StringType(), nullable=True),
    ]
)
