# DAG 2 — `anidata_dag2_convert_and_send` (explication pas a pas)

Ce document decompose `airflow/dags/anidata_dag2_convert_and_send.py` avec le code et les explications progressives.

---

## 1) Imports et configuration

```python
from __future__ import annotations

from datetime import datetime, timezone
import csv
import json
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

try:
    from elasticsearch import Elasticsearch, helpers
except ImportError:
    Elasticsearch = None
    helpers = None
```

**Ce que ca fait**
- Charge les outils de conversion JSON/XML -> CSV.
- Charge Airflow TaskFlow API.
- Charge Elasticsearch pour indexer les resultats de DAG2.

```python
DEFAULT_ARGS = {"owner": "anidata-lab", "depends_on_past": False, "retries": 0}

BASE_DIR = Path("/opt/airflow")
INPUT_FILES = [
    BASE_DIR / "data" / "anime_2.json",
    BASE_DIR / "data" / "anime_3.xml",
]
OUTPUT_DIR = BASE_DIR / "output" / "dag2"
TARGET_INDEX_NAME = "anime"
```

**Ce que ca fait**
- Lit 2 fichiers d'entree.
- Ecrit les CSV convertis dans `output/dag2`.
- Indexe ensuite dans l'index principal `anime` (option 2).

---

## 2) Fonctions de conversion

### a) Normalisation des valeurs

```python
def _flatten_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)
```

**Ce que ca fait**
- Prepare les valeurs pour le CSV (listes/dicts convertis en JSON texte).

### b) Ecriture CSV

```python
def _write_rows_to_csv(rows: list[dict[str, Any]], output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = sorted({k for r in rows for k in r.keys()})
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _flatten_value(row.get(k)) for k in fieldnames})
    return str(output_path)
```

**Ce que ca fait**
- Cree le CSV avec toutes les colonnes detectees dynamiquement.

### c) JSON -> rows

```python
def _json_to_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        return [i if isinstance(i, dict) else {"value": i} for i in payload["records"]]
    if isinstance(payload, list):
        return [i if isinstance(i, dict) else {"value": i} for i in payload]
    if isinstance(payload, dict):
        return [payload]
    return [{"value": payload}]
```

**Ce que ca fait**
- Supporte plusieurs formats JSON (`{"records":[...]}`, liste, objet).

### d) XML -> rows

```python
def _xml_to_rows(xml_text: str) -> list[dict[str, Any]]:
    root = ET.fromstring(xml_text)
    rows = []
    for node in list(root):
        row = dict(node.attrib)
        for child in list(node):
            row[child.tag] = (child.text or "").strip()
        if not row:
            row[node.tag] = (node.text or "").strip()
        rows.append(row)
    if rows:
        return rows
    return [{root.tag: (root.text or "").strip(), **root.attrib}]
```

**Ce que ca fait**
- Transforme un XML en lignes tabulaires.

### e) Routeur extension -> conversion

```python
def _convert_file_to_csv(input_path: Path) -> str:
    suffix = input_path.suffix.lower()
    output_path = OUTPUT_DIR / f"{input_path.stem}.csv"
    text = input_path.read_text(encoding="utf-8")

    if suffix == ".json":
        return _write_rows_to_csv(_json_to_rows(json.loads(text)), output_path)

    if suffix == ".xml":
        stripped = text.lstrip()
        if stripped.startswith("<"):
            return _write_rows_to_csv(_xml_to_rows(text), output_path)
        if stripped.startswith("{") or stripped.startswith("["):
            return _write_rows_to_csv(_json_to_rows(json.loads(text)), output_path)
        raise ValueError(...)

    raise ValueError(...)
```

**Ce que ca fait**
- Branche 1: `.json` -> CSV.
- Branche 2: `.xml` -> CSV (avec fallback JSON si contenu mal suffixe).

---

## 3) Indexation Elasticsearch (option 2)

```python
def _index_csvs_in_elasticsearch(csv_paths: list[str]) -> dict[str, Any]:
    es = Elasticsearch("http://elasticsearch:9200", request_timeout=60)
    if not es.indices.exists(index=TARGET_INDEX_NAME):
        es.indices.create(index=TARGET_INDEX_NAME, body=...)

    def generate_actions():
        now = datetime.now(timezone.utc).isoformat()
        for csv_path in csv_paths:
            with Path(csv_path).open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row_idx, row in enumerate(reader):
                    raw_anime_id = (row.get("anime_id") or row.get("mal_id") or "").strip()
                    doc_id = raw_anime_id or f"{Path(csv_path).stem}_{row_idx}"
                    clean_doc = {"@timestamp": now, ...}
                    yield {
                        "_op_type": "index",
                        "_index": TARGET_INDEX_NAME,
                        "_id": doc_id,
                        "_source": clean_doc,
                    }

    for ok_flag, _ in helpers.streaming_bulk(es, generate_actions(), ...):
        ...
    es.indices.refresh(index=TARGET_INDEX_NAME)
    return {"indexed": success_count, "errors": error_count}
```

**Ce que ca fait**
- Prend les CSV convertis.
- Les indexe en **upsert** dans `anime` via `_id` stable.
- Rend les docs visibles immediatement avec `refresh`.

---

## 4) Definition du DAG et des tasks

```python
with DAG(
    dag_id="anidata_dag2_convert_and_send",
    default_args=DEFAULT_ARGS,
    description="Task1: pick files; Task2: branch JSON/XML -> CSV; then send to dag1",
    start_date=datetime(2026, 3, 26),
    schedule=None,
    catchup=False,
    tags=["anidata", "example", "dag2"],
) as dag:
```

### Task 1 — recuperer les fichiers

```python
@task(task_id="task1_recuperer_fichiers")
def task1_recuperer_fichiers() -> list[str]:
    return [str(p) for p in INPUT_FILES]
```

### Task 2 — branche selon extension

```python
@task.branch(task_id="task2_branch_par_extension")
def task2_branch_par_extension(input_files: list[str]) -> list[str]:
    ...
    return sorted(set(task_ids))
```

### Task 2a/2b — conversion JSON et XML

```python
@task(task_id="task2_json_vers_csv")
def task2_json_vers_csv(input_files: list[str]) -> list[str]:
    ...

@task(task_id="task2_xml_vers_csv")
def task2_xml_vers_csv(input_files: list[str]) -> list[str]:
    ...
```

### Task 3 — preparer payload XCom

```python
@task(task_id="task3_preparer_xcom_pour_dag1")
def task3_preparer_xcom_pour_dag1(...) -> dict[str, Any]:
    payload = {"csv_outputs": all_csvs, "output_dir": str(OUTPUT_DIR)}
    return payload
```

### Task 4 — indexer dans `anime`

```python
@task(task_id="task4_indexer_csvs_vers_anime_dag2")
def task4_indexer_csvs_vers_anime_dag2(xcom_payload: dict[str, Any]) -> dict[str, Any]:
    csv_outputs = xcom_payload.get("csv_outputs", []) or []
    return _index_csvs_in_elasticsearch(csv_outputs)
```

### Task 5 — declencher DAG1

```python
trigger_dag1 = TriggerDagRunOperator(
    task_id="task5_transmettre_au_dag1",
    trigger_dag_id="anidata_full_pipeline",
    conf=xcom_payload,
    wait_for_completion=False,
    reset_dag_run=True,
)
```

**Ce que ca fait**
- Envoie le payload vers DAG1 via `dag_run.conf`.

---

## 5) Graphe final des dependances

```python
branches >> [json_csvs, xml_csvs] >> xcom_payload >> index_dag2 >> trigger_dag1
```

**Lecture simple**
- DAG2 convertit -> indexe dans `anime` -> declenche DAG1.
- C'est ce mecanisme qui permet de voir l'evolution du count Grafana apres DAG2.

