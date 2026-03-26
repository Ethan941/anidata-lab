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
except ImportError:  # pragma: no cover
    Elasticsearch = None  # type: ignore[assignment]
    helpers = None  # type: ignore[assignment]


DEFAULT_ARGS: dict[str, object] = {
    "owner": "anidata-lab",
    "depends_on_past": False,
    "retries": 0,
}

# In the Airflow container, the repo is typically mounted at /opt/airflow.
BASE_DIR = Path("/opt/airflow")
INPUT_FILES = [
    BASE_DIR / "data" / "anime_2.json",
    BASE_DIR / "data" / "anime_3.xml",
]
OUTPUT_DIR = BASE_DIR / "output" / "dag2"

# Option 2 : indexer directement dans l'index Elasticsearch principal.
TARGET_INDEX_NAME = "anime"


def _flatten_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def _write_rows_to_csv(rows: list[dict[str, Any]], output_path: Path) -> str:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames: list[str] = sorted({k for r in rows for k in r.keys()})
    with output_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _flatten_value(row.get(k)) for k in fieldnames})
    return str(output_path)


def _json_to_rows(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, dict) and isinstance(payload.get("records"), list):
        items = payload["records"]
        return [i if isinstance(i, dict) else {"value": i} for i in items]
    if isinstance(payload, list):
        return [i if isinstance(i, dict) else {"value": i} for i in payload]
    if isinstance(payload, dict):
        return [payload]
    return [{"value": payload}]


def _xml_to_rows(xml_text: str) -> list[dict[str, Any]]:
    """
    Generic XML -> rows:
    - each direct child of root becomes a row
    - row columns are child tags + attributes
    """
    root = ET.fromstring(xml_text)
    rows: list[dict[str, Any]] = []
    for node in list(root):
        row: dict[str, Any] = dict(node.attrib)
        for child in list(node):
            row[child.tag] = (child.text or "").strip()
        if not row:
            row[node.tag] = (node.text or "").strip()
        rows.append(row)
    if rows:
        return rows
    return [{root.tag: (root.text or "").strip(), **root.attrib}]


def _convert_file_to_csv(input_path: Path) -> str:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    suffix = input_path.suffix.lower()
    output_path = OUTPUT_DIR / f"{input_path.stem}.csv"
    text = input_path.read_text(encoding="utf-8")

    # Branch 1: json -> csv
    if suffix == ".json":
        payload = json.loads(text)
        return _write_rows_to_csv(_json_to_rows(payload), output_path)

    # Branch 2: xml -> csv
    if suffix == ".xml":
        # Some "xml" files in projects are actually JSON exported with a wrong extension.
        # We still try XML first; if it fails and looks like JSON, we fallback.
        stripped = text.lstrip()
        if stripped.startswith("<"):
            return _write_rows_to_csv(_xml_to_rows(text), output_path)
        if stripped.startswith("{") or stripped.startswith("["):
            payload = json.loads(text)
            return _write_rows_to_csv(_json_to_rows(payload), output_path)
        raise ValueError(f"Unsupported .xml content for {input_path}")

    raise ValueError(f"Unsupported file extension: {input_path}")


def _index_csvs_in_elasticsearch(csv_paths: list[str]) -> dict[str, Any]:
    if Elasticsearch is None or helpers is None:
        raise RuntimeError("Package 'elasticsearch' manquant dans l'image Airflow.")

    es = Elasticsearch("http://elasticsearch:9200", request_timeout=60)

    # Mapping minimal pour garantir que @timestamp est bien interprété comme date.
    index_mapping = {
        "mappings": {
            "properties": {
                "@timestamp": {"type": "date"},
            }
        }
    }
    # Si l'index anime n'existe pas, on crée un mapping minimal compatible.
    if not es.indices.exists(index=TARGET_INDEX_NAME):
        index_mapping = {
            "mappings": {
                "properties": {
                    "@timestamp": {"type": "date"},
                    "mal_id": {"type": "integer"},
                    "name": {"type": "text"},
                    "score": {"type": "float"},
                    "genres": {"type": "keyword"},
                    "type": {"type": "keyword"},
                    "episodes": {"type": "integer"},
                    "members": {"type": "long"},
                    "studios": {"type": "keyword"},
                    "year": {"type": "integer"},
                }
            }
        }
        es.indices.create(index=TARGET_INDEX_NAME, body=index_mapping)

    def generate_actions():
        now = datetime.now(timezone.utc).isoformat()
        for csv_path in csv_paths:
            path = Path(csv_path)
            if not path.exists():
                raise FileNotFoundError(f"CSV not found: {csv_path}")

            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                stem = path.stem
                for row_idx, row in enumerate(reader):
                    # Colonnes attendues (issues de anime_2.json / anime_3.xml convertis) :
                    # anime_id, name, genre, type, episodes, rating, members, year, studio
                    raw_anime_id = (row.get("anime_id") or row.get("mal_id") or "").strip()
                    raw_name = (row.get("name") or "").strip()

                    # Déterminer un ID stable : mal_id/anime_id (pour upsert) sinon stem+row_idx
                    doc_id = raw_anime_id or f"{stem}_{row_idx}"

                    # Transformer pour coller au schéma attendu par l'index `anime` (script_prof.py)
                    clean_doc: dict[str, Any] = {"@timestamp": now}

                    if raw_anime_id:
                        try:
                            clean_doc["mal_id"] = int(raw_anime_id)
                        except ValueError:
                            # garde seulement comme texte si conversion impossible
                            pass

                    if raw_name:
                        clean_doc["name"] = raw_name

                    # Dans anime_2.json, "rating" correspond en pratique à un score (chez toi c'est 8.61..)
                    raw_score = (row.get("rating") or row.get("score") or "").strip()
                    if raw_score:
                        try:
                            clean_doc["score"] = float(raw_score)
                        except ValueError:
                            pass

                    raw_type = (row.get("type") or "").strip()
                    if raw_type:
                        clean_doc["type"] = raw_type

                    raw_episodes = (row.get("episodes") or "").strip()
                    if raw_episodes:
                        try:
                            clean_doc["episodes"] = int(raw_episodes)
                        except ValueError:
                            pass

                    raw_members = (row.get("members") or "").strip()
                    if raw_members:
                        try:
                            clean_doc["members"] = int(raw_members)
                        except ValueError:
                            pass

                    raw_year = (row.get("year") or "").strip()
                    if raw_year:
                        try:
                            clean_doc["year"] = int(raw_year)
                        except ValueError:
                            pass

                    raw_studio = (row.get("studio") or row.get("studios") or "").strip()
                    if raw_studio:
                        clean_doc["studios"] = raw_studio

                    raw_genres = (row.get("genre") or row.get("genres") or "").strip()
                    if raw_genres:
                        # Si c'est une string JSON du type ["Action","Comedy"], on tente un parse.
                        try:
                            parsed = json.loads(raw_genres)
                            if isinstance(parsed, list):
                                clean_doc["genres"] = parsed
                            else:
                                clean_doc["genres"] = raw_genres
                        except Exception:
                            clean_doc["genres"] = raw_genres

                    yield {
                        "_op_type": "index",
                        "_index": TARGET_INDEX_NAME,
                        "_id": doc_id,
                        "_source": clean_doc,
                    }

    success_count = 0
    error_count = 0

    for ok_flag, _item in helpers.streaming_bulk(
        es,
        generate_actions(),
        chunk_size=200,
        raise_on_error=False,
        raise_on_exception=False,
    ):
        if ok_flag:
            success_count += 1
        else:
            error_count += 1

    es.indices.refresh(index=TARGET_INDEX_NAME)
    return {"indexed": success_count, "errors": error_count}


with DAG(
    dag_id="anidata_dag2_convert_and_send",
    default_args=DEFAULT_ARGS,
    description="Task1: pick files; Task2: branch JSON/XML -> CSV; then send to dag1",
    start_date=datetime(2026, 3, 26),
    schedule=None,
    catchup=False,
    tags=["anidata", "example", "dag2"],
) as dag:

    @task(task_id="task1_recuperer_fichiers")
    def task1_recuperer_fichiers() -> list[str]:
        # Returning paths makes them available as XCom automatically (TaskFlow API).
        return [str(p) for p in INPUT_FILES]

    @task.branch(task_id="task2_branch_par_extension")
    def task2_branch_par_extension(input_files: list[str]) -> list[str]:
        task_ids: list[str] = []
        for p in input_files:
            suffix = Path(p).suffix.lower()
            if suffix == ".json":
                task_ids.append("task2_json_vers_csv")
            elif suffix == ".xml":
                task_ids.append("task2_xml_vers_csv")
            else:
                raise ValueError(f"Unsupported extension for {p}")
        # Return list of task_ids so both branches can run when needed.
        return sorted(set(task_ids))

    @task(task_id="task2_json_vers_csv")
    def task2_json_vers_csv(input_files: list[str]) -> list[str]:
        outputs: list[str] = []
        for p in input_files:
            if Path(p).suffix.lower() == ".json":
                outputs.append(_convert_file_to_csv(Path(p)))
        return outputs

    @task(task_id="task2_xml_vers_csv")
    def task2_xml_vers_csv(input_files: list[str]) -> list[str]:
        outputs: list[str] = []
        for p in input_files:
            if Path(p).suffix.lower() == ".xml":
                outputs.append(_convert_file_to_csv(Path(p)))
        return outputs

    @task(task_id="task3_preparer_xcom_pour_dag1")
    def task3_preparer_xcom_pour_dag1(json_csvs: list[str] | None, xml_csvs: list[str] | None) -> dict[str, Any]:
        all_csvs = []
        for part in (json_csvs or []), (xml_csvs or []):
            all_csvs.extend(part)
        payload = {
            "csv_outputs": all_csvs,
            "output_dir": str(OUTPUT_DIR),
        }
        # This dict is stored as XCom for dag2 and will be sent to dag1 via `conf`.
        return payload

    @task(task_id="task4_indexer_csvs_vers_anime_dag2")
    def task4_indexer_csvs_vers_anime_dag2(xcom_payload: dict[str, Any]) -> dict[str, Any]:
        csv_outputs = xcom_payload.get("csv_outputs", []) or []
        return _index_csvs_in_elasticsearch(csv_outputs)

    input_files = task1_recuperer_fichiers()
    branches = task2_branch_par_extension(input_files)
    json_csvs = task2_json_vers_csv(input_files)
    xml_csvs = task2_xml_vers_csv(input_files)
    xcom_payload = task3_preparer_xcom_pour_dag1(json_csvs, xml_csvs)

    index_dag2 = task4_indexer_csvs_vers_anime_dag2(xcom_payload)

    trigger_dag1 = TriggerDagRunOperator(
        task_id="task5_transmettre_au_dag1",
        trigger_dag_id="anidata_full_pipeline",
        conf=xcom_payload,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    branches >> [json_csvs, xml_csvs] >> xcom_payload >> index_dag2 >> trigger_dag1

