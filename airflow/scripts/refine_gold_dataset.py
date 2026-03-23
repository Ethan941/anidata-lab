"""
Pipeline de Data Refinement pour AniData Lab.

Objectifs:
- Nettoyage: doublons, valeurs manquantes, normalisation texte
- Encodage UTF-8: nettoyage de caracteres parasites et normalisation Unicode
- Typage: coercition des colonnes numeriques/categorielles et extraction de dates
- Feature engineering metier:
  - weighted_popularity_score
  - dropped_completed_ratio
  - studio_class
- Export dataset "gold" en CSV + JSON
"""

from __future__ import annotations

import json
import unicodedata
from pathlib import Path

import numpy as np
import pandas as pd


REPLACEMENT_CHAR = "�"
ENCODINGS_TO_TRY = ("utf-8", "utf-8-sig", "cp932", "shift_jis")


def read_csv_smart(path: Path) -> pd.DataFrame:
    """Charge un CSV en testant plusieurs encodages si necessaire."""
    for enc in ENCODINGS_TO_TRY:
        try:
            return pd.read_csv(path, encoding=enc, low_memory=False)
        except UnicodeDecodeError:
            continue
    return pd.read_csv(path, encoding="utf-8", low_memory=False, errors="ignore")


def clean_text_value(value: object) -> object:
    """Normalise Unicode et supprime les artefacts d'encodage simples."""
    if pd.isna(value):
        return value
    text = str(value)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace(REPLACEMENT_CHAR, "")
    return text.strip()


def parse_aired_start_year(series: pd.Series) -> pd.Series:
    """Extrait l'annee de debut depuis la colonne Aired."""
    years = series.astype(str).str.extract(r"(\d{4})", expand=False)
    return pd.to_numeric(years, errors="coerce").astype("Int64")


def classify_studios(df: pd.DataFrame, studio_col: str = "Studios") -> pd.Series:
    """Classe les studios en Major / Mid / Niche selon le volume d'animes."""
    base = df[studio_col].fillna("Unknown").astype(str)
    primary_studio = base.str.split(",").str[0].str.strip().replace("", "Unknown")

    counts = primary_studio.value_counts(dropna=False)
    if counts.empty:
        return pd.Series(["Niche"] * len(df), index=df.index)

    q66 = counts.quantile(0.66)
    q33 = counts.quantile(0.33)

    def _label(studio: str) -> str:
        c = counts.get(studio, 0)
        if c >= q66:
            return "Major"
        if c >= q33:
            return "Mid"
        return "Niche"

    return primary_studio.map(_label)


def build_gold_dataset(base_dir: Path) -> tuple[pd.DataFrame, Path, Path]:
    data_dir = base_dir / "data"
    gold_dir = data_dir / "gold"
    gold_dir.mkdir(parents=True, exist_ok=True)

    path_anime = data_dir / "anime.csv"
    path_synopsis = data_dir / "anime_with_synopsis.csv"

    if not path_anime.exists():
        raise FileNotFoundError(f"Fichier introuvable: {path_anime}")
    if not path_synopsis.exists():
        raise FileNotFoundError(f"Fichier introuvable: {path_synopsis}")

    anime = read_csv_smart(path_anime)
    synopsis = read_csv_smart(path_synopsis)

    # Harmonisation nom de colonne (source contient souvent la faute 'sypnopsis')
    if "sypnopsis" in synopsis.columns and "synopsis" not in synopsis.columns:
        synopsis = synopsis.rename(columns={"sypnopsis": "synopsis"})

    # Merge sur MAL_ID pour enrichir anime avec synopsis
    if "MAL_ID" in synopsis.columns:
        syn = synopsis[["MAL_ID", "synopsis"]].copy() if "synopsis" in synopsis.columns else synopsis[["MAL_ID"]].copy()
    else:
        syn = pd.DataFrame(columns=["MAL_ID", "synopsis"])

    df = anime.merge(syn, on="MAL_ID", how="left")

    # Suppression des doublons
    df = df.drop_duplicates()
    if "MAL_ID" in df.columns:
        df = df.drop_duplicates(subset=["MAL_ID"], keep="first")

    # Nettoyage UTF-8/Unicode des colonnes texte
    text_cols = df.select_dtypes(include=["object"]).columns.tolist()
    for col in text_cols:
        df[col] = df[col].map(clean_text_value)

    # Normalisation de types numeriques
    numeric_cols = [
        "Score",
        "Episodes",
        "Ranked",
        "Popularity",
        "Members",
        "Favorites",
        "Watching",
        "Completed",
        "On-Hold",
        "Dropped",
        "Plan to Watch",
        "Score-10",
        "Score-9",
        "Score-8",
        "Score-7",
        "Score-6",
        "Score-5",
        "Score-4",
        "Score-3",
        "Score-2",
        "Score-1",
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Dates/categoriels
    if "Aired" in df.columns:
        df["aired_start_year"] = parse_aired_start_year(df["Aired"])
    if "Premiered" in df.columns:
        df["premiered_season"] = (
            df["Premiered"]
            .astype(str)
            .replace({"Unknown": np.nan, "nan": np.nan})
            .fillna("Unknown")
            .astype("category")
        )
    if "Type" in df.columns:
        df["Type"] = df["Type"].fillna("Unknown").astype("category")
    if "Source" in df.columns:
        df["Source"] = df["Source"].fillna("Unknown").astype("category")

    # Imputation simple
    for col in ["Score", "Episodes", "Ranked"]:
        if col in df.columns:
            med = df[col].median()
            df[col] = df[col].fillna(med if not np.isnan(med) else 0)

    for col in ["Genres", "Studios", "Rating", "synopsis", "English name", "Japanese name"]:
        if col in df.columns:
            df[col] = df[col].fillna("Unknown")

    # Feature 1: score de popularite pondere
    # Normalise les members pour garder une echelle interpretable.
    if {"Score", "Members"}.issubset(df.columns):
        members = np.log1p(df["Members"].clip(lower=0))
        denom = float(members.max()) if float(members.max()) > 0 else 1.0
        df["weighted_popularity_score"] = (df["Score"].clip(lower=0, upper=10) * (members / denom)).round(4)
    else:
        df["weighted_popularity_score"] = np.nan

    # Feature 2: ratio dropped/completed
    if {"Dropped", "Completed"}.issubset(df.columns):
        denom = df["Completed"].replace(0, np.nan)
        df["dropped_completed_ratio"] = (df["Dropped"] / denom).replace([np.inf, -np.inf], np.nan).fillna(0).round(6)
    else:
        df["dropped_completed_ratio"] = np.nan

    # Feature 3: classification studio
    if "Studios" in df.columns:
        df["studio_class"] = classify_studios(df, "Studios").astype("category")
        df["primary_studio"] = (
            df["Studios"].astype(str).str.split(",").str[0].str.strip().replace("", "Unknown")
        )
    else:
        df["studio_class"] = "Niche"
        df["primary_studio"] = "Unknown"

    # Petite feature textuelle utile pour ELK
    if "synopsis" in df.columns:
        df["synopsis_length"] = df["synopsis"].astype(str).str.len().fillna(0).astype(int)

    # Exports gold
    out_csv = gold_dir / "anime_gold.csv"
    out_json = gold_dir / "anime_gold.json"
    df.to_csv(out_csv, index=False, encoding="utf-8")
    df.to_json(out_json, orient="records", force_ascii=False, indent=2)

    return df, out_csv, out_json


def main() -> None:
    # airflow/scripts/ -> projet a 2 niveaux au-dessus
    project_root = Path(__file__).resolve().parents[2]
    df, out_csv, out_json = build_gold_dataset(project_root)

    print("[OK] Data refinement termine.")
    print(f"Lignes: {len(df)} | Colonnes: {len(df.columns)}")
    print(f"CSV gold: {out_csv}")
    print(f"JSON gold: {out_json}")
    print(
        "[Features] weighted_popularity_score, dropped_completed_ratio, studio_class, primary_studio, synopsis_length"
    )
    # Trace compacte utilisable dans logs Airflow
    preview = df[["MAL_ID", "Name", "weighted_popularity_score", "dropped_completed_ratio", "studio_class"]].head(5)
    print("[Preview]")
    print(json.dumps(preview.to_dict(orient="records"), ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
