# CLAUDE.md — NF26/BGES

## Contexte

Projet de groupe **NF26** — Bilan Gaz à Effet de Serre (BGES).
Objectif : traiter et analyser des données d'émissions GES pour plusieurs sites
(Paris, Berlin, London, New York, Los Angeles, Shanghai) avec **PySpark**.

---

## Stack technique

| Outil        | Usage                                                  |
|--------------|--------------------------------------------------------|
| Python 3.12  | Langage principal (version fixée dans `.python-version`) |
| uv           | Gestionnaire de paquets et venv (`uv run`, `uv add`)   |
| PySpark 3.5+ | Traitement distribué des données                       |
| Jupyter Lab  | Notebooks interactifs                                  |
| ruff         | Linting + formatage                                    |
| mypy         | Typage statique                                        |
| nbstripout   | Suppression des sorties de cellules avant commit       |

### Commandes clés

```bash
uv sync --all-groups           # installer toutes les dépendances
uv run jupyter lab             # lancer Jupyter Lab
uv run python src/hello_world.py  # exécuter un script
uv run ruff check src/         # lint
uv run ruff format src/        # format
uv run mypy src/               # type check
```

---

## Structure du projet

```
BGES/
├── data/                  # Données brutes (lecture seule)
│   └── BDD_BGES_*/
├── notebooks/             # Jupyter notebooks
│   └── hello_world.ipynb
├── src/                   # Scripts Python
│   └── hello_world.py
├── .python-version        # Version Python (3.12)
├── pyproject.toml         # Config projet + outils
├── uv.lock                # Dépendances verrouillées
└── CLAUDE.md
```

---

## Standards de code

### Initialisation PySpark

```python
import os
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"  # toujours en premier

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = (
    SparkSession.builder
    .appName("BGES")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

### Nommage

- DataFrames Spark SQL : préfixe `sdf_` → `sdf_missions`
- DataFrames pandas : préfixe `pdf_` → `pdf_summary`

### Règles PySpark

```python
# ✅ Toujours col() pour les colonnes
df.filter(F.col("ville") == "Paris")
df.groupBy("site").agg(F.sum("emission").alias("total_emission"))

# ❌ À éviter
df.filter(df["ville"] == "Paris")
df.groupBy("site").agg({"emission": "sum"})
```

---

## Git

- Commits : style Conventional Commits (`feat:`, `fix:`, `refactor:`)
- Ne jamais committer : `.venv/`, `__pycache__/`, `.ipynb_checkpoints/`, `spark-warehouse/`
- `nbstripout` est configuré — les sorties de cellules sont supprimées automatiquement
