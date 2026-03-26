from __future__ import annotations

from datetime import datetime
import csv
import json
from pathlib import Path
from typing import Any
import xml.etree.ElementTree as ET

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


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

    input_files = task1_recuperer_fichiers()
    branches = task2_branch_par_extension(input_files)
    json_csvs = task2_json_vers_csv(input_files)
    xml_csvs = task2_xml_vers_csv(input_files)
    xcom_payload = task3_preparer_xcom_pour_dag1(json_csvs, xml_csvs)

    trigger_dag1 = TriggerDagRunOperator(
        task_id="task4_transmettre_au_dag1",
        trigger_dag_id="anidata_full_pipeline",
        conf=xcom_payload,
        wait_for_completion=False,
        reset_dag_run=True,
    )

    branches >> [json_csvs, xml_csvs] >> xcom_payload >> trigger_dag1

