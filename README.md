# BGES — Bilan Gaz à Effet de Serre

Projet NF26 — Traitement de données GES avec **PySpark**.

## Stack

| Outil        | Version              |
|--------------|----------------------|
| Python       | 3.12                 |
| PySpark      | 4.x                  |
| uv           | gestionnaire de paquets |
| Jupyter Lab  | 4.x                  |
| ruff         | lint + format        |
| mypy         | typage statique      |

## Installation

```bash
# Cloner le repo
git clone <url-du-repo>
cd BGES

# Créer le venv et installer toutes les dépendances
uv sync --all-groups

# Configurer nbstripout (une fois par membre)
uv run nbstripout --install
```

## Commandes

```bash
uv run jupyter lab                # Jupyter Lab → http://localhost:8888
uv run python src/hello_world.py

uv run ruff check src/            # lint
uv run ruff format src/           # format
uv run mypy src/                  # typage
```

## Structure

```text
BGES/
├── data/          # Données brutes (lecture seule)
├── notebooks/     # Jupyter notebooks
├── src/           # Scripts Python
├── pyproject.toml
└── uv.lock
```
