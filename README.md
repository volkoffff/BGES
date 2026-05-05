# BGES – Bilan Gaz à Effet de Serre

Projet NF26 – Traitement de données avec **PySpark** dans un environnement Docker partagé.

## Stack technique

| Composant   | Version                      |
|-------------|------------------------------|
| Python      | 3.13                         |
| Java        | Temurin 21 LTS (via SDKMAN)  |
| PySpark     | 3.5.5                        |
| Jupyter Lab | 4.4.2                        |

## Prérequis

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (ou Docker Engine + Compose plugin)
- Git

## Démarrage rapide

```bash
# 1. Cloner le dépôt
git clone <url-du-repo>
cd BGES

# 2. Construire et lancer l'environnement (première fois ~5 min)
docker compose up --build

# 3. Ouvrir Jupyter Lab dans le navigateur
#    → http://localhost:8888
```

Les notebooks se trouvent dans `notebooks/`, les scripts Python dans `src/`.
Les données sont montées en lecture seule depuis `BDD/` → accessibles via `/app/data/` dans le conteneur.

## Commandes utiles

```bash
# Lancer sans reconstruire l'image
docker compose up

# Arrêter proprement
docker compose down

# Reconstruire l'image (après modification du Dockerfile ou pyproject.toml)
docker compose up --build

# Exécuter un script Python directement
docker compose exec spark-jupyter python src/hello_world.py

# Ouvrir un shell dans le conteneur
docker compose exec spark-jupyter bash
```

## Interfaces disponibles

| Interface   | URL                                                              |
|-------------|------------------------------------------------------------------|
| Jupyter Lab | <http://localhost:8888>                                          |
| Spark UI    | <http://localhost:4040> (disponible pendant une session Spark)   |

## Structure du projet

```text
BGES/
├── BDD/                    # Données brutes (lecture seule dans Docker)
├── notebooks/              # Jupyter notebooks
│   └── hello_world.ipynb
├── src/                    # Scripts Python
│   └── hello_world.py
├── Dockerfile
├── docker-compose.yml
├── pyproject.toml
├── .gitignore
└── .gitattributes
```

## Bonnes pratiques Git

Les **sorties de cellules** sont supprimées avant le commit via `nbstripout`.
Chaque membre de l'équipe doit le configurer une fois :

```bash
pip install nbstripout
nbstripout --install  # dans le repo cloné
```

- Les fins de ligne sont normalisées en **LF** via `.gitattributes`.
- Ne pas committer `__pycache__/`, `.ipynb_checkpoints/`, etc. (couverts par `.gitignore`).
