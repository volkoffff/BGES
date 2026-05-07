erDiagram
FACT_EQUIPMENT {
    int SK_FACT_EQUIPMENT PK
    string NK_EQUIPMENT "source ID"
    int SK_DATE_PURCHASE FK
    int SK_STAFF FK
    int SK_EQUIPMENT FK
    float CO2_IMPACT_KG "nullable"
}
FACT_MISSION {
    int SK_FACT_MISSION PK
    string NK_MISSION "source ID"
    int SK_DATE_MISSION FK
    int SK_STAFF FK
    int SK_TRIP FK
    int SK_TRANSPORT_TYPE FK
    string MISSION_TYPE "enum"
    bool ROUND_TRIP_FLG
    float CO2_IMPACT_KG "precomputed ETL"
}
DIM_DATE {
    int SK_DATE PK
    DATE_ISO date_global
}
DIM_STAFF {
    int SK_STAFF PK
    string NK_STAFF "source ID"
    string LAST_NAME
    string FIRST_NAME
    string JOB_TITLE
    string ACTIVITY_SECTOR
    date BIRTH_DATE
    int SK_SITE FK
}
DIM_CITY {
    int SK_CITY PK
    string CITY_NAME
    string COUNTRY_ISO2
    bool IS_ORG_SITE
    string TIMEZONE_IANA
}
DIM_TRIP {
    int SK_TRIP PK
    int SK_CITY_ORIGIN FK
    int SK_CITY_DESTINATION FK
    float DISTANCE_KM
}
DIM_EQUIPMENT {
    int SK_EQUIPMENT PK
    string TYPE
    string MODEL
    float CO2_IMPACT_KG_REF "nullable"
}
DIM_TRANSPORT_TYPE {
    int SK_TRANSPORT_TYPE PK
    string TRANSPORT_NAME
    float CO2_FACTOR_KG_PER_KM
}
FACT_EQUIPMENT--o{ DIM_DATE : "purchase date"
FACT_EQUIPMENT
--o{ DIM_STAFF : ""
FACT_EQUIPMENT--o{ DIM_EQUIPMENT : ""
FACT_MISSION
--o{ DIM_DATE : "mission date"
FACT_MISSION--o{ DIM_STAFF : ""
FACT_MISSION
--o{ DIM_TRIP : ""
FACT_MISSION--o{ DIM_TRANSPORT_TYPE : ""
DIM_STAFF
--o{ DIM_CITY : "home site"
DIM_TRIP--o{ DIM_CITY : "origin"
DIM_TRIP
--o{ DIM_CITY : "destination"