# Contributing to SkySafe

Merci de contribuer au projet SkySafe ! Voici les lignes directrices pour participer.

## Prérequis

- Python 3.10+
- Docker & Docker Compose
- Java 11+ (pour les tests Spark en local)

## Installation locale

```bash
git clone https://github.com/tahianahajanirina/skysafe-datalake.git
cd skysafe-datalake
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
pip install flake8 pytest
```

## Lancer les tests

```bash
# Tests locaux (Spark tests auto-skippés sans Java)
make test

# Tests complets dans Docker
make test-docker
```

## Linter

```bash
make lint
```

## Workflow de contribution

1. Créez une branche depuis `main` : `git checkout -b feature/ma-feature`
2. Faites vos modifications
3. Ajoutez des tests si nécessaire
4. Vérifiez que les tests passent : `make test`
5. Vérifiez le lint : `make lint`
6. Committez avec un message clair : `git commit -m 'feat: description'`
7. Poussez et ouvrez une Pull Request

## Convention de commits

- `feat:` — nouvelle fonctionnalité
- `fix:` — correction de bug
- `docs:` — documentation
- `test:` — ajout/modification de tests
- `refactor:` — refactoring sans changement fonctionnel

## Structure du projet

- `src/` — Code source du pipeline
- `dags/` — DAGs Airflow
- `tests/` — Tests unitaires (pytest)
- `docker-compose.yml` — Infrastructure locale

## Code de conduite

Soyez respectueux et constructif dans vos interactions.
