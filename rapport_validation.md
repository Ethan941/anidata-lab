# Rapport de validation (extrait terminal)

Source: `terminals/1.txt` (lignes 414-500)

```text
romain@MacBook-Air-de-Romain anidata-lab % docker compose exec airflow-webserver python /opt/airflow/dags/05_validation.py

============================================================
  VALIDATION — Dataset Gold
============================================================

  Chargement du dataset gold...
  ✅ Fichier chargé : 17,562 lignes × 49 colonnes

============================================================
  ASSERTIONS DE VALIDATION
============================================================


--- 1. Assertions structurelles ---
  ✅ PASS — Le dataset contient des données
  ✅ PASS — Au moins 10 000 animes

--- 2. Assertions d'unicité ---
  ✅ PASS — Clé primaire 'mal_id' unique
  ✅ PASS — Aucun doublon exact

--- 3. Assertions de complétude ---
  ✅ PASS — Colonne 'mal_id' sans NaN
  ✅ PASS — Colonne 'name' sans NaN
  ❌ FAIL — Colonne 'type' sans NaN
         37 NaN trouvés
  ✅ PASS — Taux global de NaN < 30%

--- 4. Assertions de plage de valeurs ---
  ✅ PASS — Scores entre 1 et 10 (hors NaN)
  ✅ PASS — Aucun score = 0 (NaN déguisé nettoyé)
  ✅ PASS — Épisodes > 0 (hors NaN)
  ✅ PASS — Members ≥ 0

--- 5. Assertions sur les features enrichies ---
  ✅ PASS — weighted_score > 0 (hors NaN)
  ✅ PASS — drop_ratio entre 0 et 1
  ✅ PASS — score_category dans les valeurs autorisées
  ✅ PASS — studio_tier dans {Top, Mid, Indie}
  ✅ PASS — Décennies cohérentes (1910-2030)

--- 6. Assertions d'encodage ---
  ✅ PASS — Toutes les colonnes textuelles encodables en UTF-8

============================================================
  RAPPORT DE SYNTHÈSE
============================================================


  Assertions passées  : 17
  Assertions échouées : 1
  Total               : 18
  Taux de réussite    : 94%


--- Résumé du dataset gold ---

  Lignes      : 17,562
  Colonnes    : 49
  NaN total   : 138,676 (16.1%)
  Outliers    : 0 (marqués, non supprimés)


--- Export CSV validé ---
  ✅ output/anime_gold_validated.csv (6.0 MB)

--- Export JSON pour Elasticsearch ---
  Préparation du JSON (format compatible bulk Elasticsearch)...
  ✅ output/anime_gold.json (16.4 MB) — 17,562 documents NDJSON

--- Export du rapport de validation ---
  ✅ output/rapport_validation.txt

============================================================
  RÉCAPITULATIF DES FICHIERS GÉNÉRÉS
============================================================

  📄 output/anime_gold_validated.csv               (dataset final CSV)
  📄 output/anime_gold.json                        (prêt pour Elasticsearch)
  📄 output/rapport_validation.txt                 (rapport de validation)

✅ Pipeline Data Refinement terminé !

→ Cet après-midi : indexer anime_gold.json dans Elasticsearch
   et créer les dashboards Grafana !
```
