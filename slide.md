# 🎌 AniData Lab — Présentation (6 minutes)

---

## Slide 1 — Contexte & objectif

- **But** : construire une mini‑plateforme *data* Anime/Manga de bout en bout
- **Entrées** : 3 CSV Kaggle (`anime.csv`, `anime_with_synopsis.csv`, `rating_complete.csv`)
- **Livrable principal** :
  - un dataset **gold** nettoyé + enrichi (**CSV + JSON**)
  - une “base de données” de recherche/analytics dans **Elasticsearch**
  - des dashboards **Grafana** alimentés automatiquement
- **Pourquoi** : passer d’un dataset brut à un produit exploitable (qualité, features, visualisation)

---

## Slide 2 — Architecture (vision globale)

- **Docker Compose** orchestre la stack locale
- **Airflow** = orchestration (pipeline, vérifications, branches, notifications)
- **PostgreSQL** = base metadata Airflow (DAG runs, task instances, états)
- **Scripts Python** = transformation (audit → nettoyage → feature engineering → validation)
- **Elasticsearch** = stockage + recherche + agrégations (sert de “DB” pour Grafana)
- **Grafana** = visualisation (datasource Elasticsearch + dashboard provisionné)
- **Logstash (optionnel)** = ingestion CSV → Elasticsearch (profil `--profile ingest`)

Schéma mental :
`CSV bruts → (Python) gold → (script_prof.py / Logstash) Elasticsearch → Grafana`

---

## Slide 3 — Data Refinement (de brut → gold)

- **Audit** : qualité, doublons, types, valeurs manquantes, colonnes suspectes
- **Nettoyage** :
  - normalisation types (numériques, dates)
  - correction/renommage de colonnes (ex: `sypnopsis` → `synopsis`)
  - gestion des “Unknown” / valeurs vides → `NaN`
  - robustesse encodage (UTF‑8 + encodages alternatifs si besoin)
- **Feature engineering (métier)** :
  - score pondéré, ratios (engagement/drop), extraction année/décennie
  - studio principal / tiers, genres principaux, synopsis length
- **Exports** : `data/gold/anime_gold.csv` + `data/gold/anime_gold.json`

---

## Slide 4 — ELK/Grafana : de gold → recherche + dashboards

- **Index Elasticsearch** : mapping pour typer correctement les champs (évite “tout en text”)
- **Indexation** :
  - via `script_prof.py` (**indexation incrémentale / upsert**) → met à jour sans tout supprimer
  - ou via Logstash (pipeline CSV → ES)
- **Requêtes** : full‑text sur `synopsis` + agrégations (top studios, genres, distributions)
- **Grafana** :
  - datasource Elasticsearch
  - dashboard de démarrage provisionné (`grafana/dashboards/anidata-overview.json`)

Message clé : **si Grafana affiche des chiffres, c’est qu’Elasticsearch a des documents**.

---

## Slide 5 — Airflow : fiabilité, contrôle & résultats (démo)

- **DAG “full pipeline”** : exécute 01→05 puis indexation
- **Robustesse** :
  - vérif des fichiers attendus + messages de succès
  - double‑run (01→05) + comparaison de hash → détecte non‑déterminisme
  - callback email en cas d’échec (1 email max par run)
- **Résultat** :
  - pipeline reproductible
  - index Elasticsearch rempli (~17k animes)
  - dashboards Grafana opérationnels

Démo (si 30–45s) :
- `curl http://localhost:9200/anime/_count`
- ouvrir Grafana (total, top studios, genres, etc.)

---

## Slide 6 — Schéma des tasks (DAG2 + DAG1)

### DAG 2 — `anidata_dag2_convert_and_send`

`task1_recuperer_fichiers`  
→ `task2_branch_par_extension`  
→ (`task2_json_vers_csv` et/ou `task2_xml_vers_csv`)  
→ `task3_preparer_xcom_pour_dag1`  
→ `task4_transmettre_au_dag1` *(TriggerDagRun vers `anidata_full_pipeline`)*

### DAG 1 — `anidata_full_pipeline`

`00_receive_from_dag2`  
→ `01_audit_complet`  
→ `check_audit_status`  
→ **Branche OK**: `02_audit_visuel` → `03_nettoyage` → `04_feature_engineering` → `05_validation` → `06_indexation_elasticsearch`  
→ **Branche FAIL**: `send_email_audit_failed`

Message clé :
**DAG2 prépare/convertit et transmet; DAG1 exécute le pipeline data complet + indexation Elasticsearch.**

