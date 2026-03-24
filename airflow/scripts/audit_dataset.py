"""
Script d'audit qualite (et exploration) du dataset AniData Lab.

Il charge les 3 CSV :
- anime.csv
- rating_complete.csv (colonnes: user_id, anime_id, rating)
- anime_with_synopsis.csv

Puis produit des sorties console (dimensions, types, valeurs manquantes, doublons, encodage, controles metier)
et genere 3 rapports HTML via ydata-profiling (si installe).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

# Profiling (optionnel)
try:
    from ydata_profiling import ProfileReport
except ImportError:  # pragma: no cover
    ProfileReport = None


BASE_DIR = Path(__file__).resolve().parents[2] / "data"  # dossier du script

# Chemins CSV attendus a la racine du projet (comme indique dans le contexte utilisateur)
PATH_ANIME = BASE_DIR / "anime.csv"
PATH_RATINGS = BASE_DIR / "rating_complete.csv"
PATH_SYNOPSIS = BASE_DIR / "anime_with_synopsis.csv"

# Ordre de test encodages (a adapter si besoin)
ENCODINGS_TO_TRY = ["utf-8", "utf-8-sig", "cp1251", "koi8-r", "utf-16", "cp932", "shift_jis"]
REPLACEMENT_CHAR = "�"
TITLE_COLUMNS = ["Name", "English name", "Japanese name"]

# Si tu vois des caracteres corrompus ou une erreur d'encodage, mets TEST_ENCODING=True
TEST_ENCODING = False
SAMPLE_ENCODING_NROWS = 5000


def _count_replacement_chars(df: pd.DataFrame, sample_cols: list[str] | None = None) -> int:
    """Compte le nombre de caracteres de remplacement '�' dans les colonnes textuelles (object)."""
    if sample_cols is None:
        sample_cols = df.select_dtypes(include="object").columns.tolist()

    total = 0
    for col in sample_cols:
        s = df[col].astype(str)
        total += int(s.str.contains(REPLACEMENT_CHAR, na=False).sum())
    return int(total)


def read_csv_smart(path: Path, test_encoding: bool = False) -> pd.DataFrame:
    """Lit un CSV avec encodage robuste (teste uniquement si necessaire)."""
    # Essai simple: utf-8
    try:
        df = pd.read_csv(path, encoding="utf-8", low_memory=False)
        if not test_encoding:
            return df
    except UnicodeDecodeError:
        # Si echec, on teste une liste d'encodages
        best_df: pd.DataFrame | None = None
        best_score: int | None = None

        for enc in ENCODINGS_TO_TRY:
            df_try = pd.read_csv(path, encoding=enc, nrows=SAMPLE_ENCODING_NROWS, low_memory=False)
            score = _count_replacement_chars(df_try)
            if best_score is None or score < best_score:
                best_score = score
                best_df = pd.read_csv(path, encoding=enc, low_memory=False)
        if best_df is None:
            # Fallback (normalement impossible)
            best_df = pd.read_csv(path, encoding="utf-8", low_memory=False)
        return best_df

    # Pas d'erreur d'encodage, mais on peut verifier la presence de caracteres de remplacement
    if test_encoding:
        df_sample = df.head(SAMPLE_ENCODING_NROWS)
        score_current = _count_replacement_chars(df_sample)
        if score_current > 0:
            best_df = None
            best_score = score_current
            for enc in ENCODINGS_TO_TRY:
                df_try_sample = pd.read_csv(path, encoding=enc, nrows=SAMPLE_ENCODING_NROWS, low_memory=False)
                score = _count_replacement_chars(df_try_sample)
                if score < best_score:
                    best_score = score
                    best_df = pd.read_csv(path, encoding=enc, low_memory=False)
            return best_df if best_df is not None else df

    return df


def audit_dataframe(
    df: pd.DataFrame,
    name: str,
    business_keys: list[str] | None = None,
) -> None:
    """Affiche un audit console simple (structure, types, missing, doublons)."""
    print(f"\n--- AUDIT DE {name} ---")
    print("Dimensions :", df.shape)

    print("\nTypes (dtypes) :")
    print(df.dtypes)

    print("\nApercu (head 5) :")
    print(df.head())

    print("\nDescribe (include=all) :")
    try:
        print(df.describe(include="all"))
    except Exception as e:  # pragma: no cover
        print(f"[WARN] describe a echoue pour {name}: {e}")

    print("\nValeurs manquantes (top 10) :")
    na_counts = df.isna().sum().sort_values(ascending=False)
    print(na_counts.head(10))

    print("\nValeurs manquantes (%) (top 10) :")
    na_pct = (df.isna().mean() * 100).sort_values(ascending=False)
    print(na_pct.head(10))

    print("\nDoublons complets :", int(df.duplicated().sum()))

    if business_keys:
        present_keys = [k for k in business_keys if k in df.columns]
        if present_keys:
            print(f"Doublons sur {present_keys} :", int(df.duplicated(subset=present_keys).sum()))
        else:
            print("[INFO] Aucune clé métier trouvée pour les doublons.")
    print()


def numeric_string_audit(df: pd.DataFrame, candidate_cols: list[str]) -> None:
    """Vérifie si des colonnes supposees numeriques sont lues en object.

    candidate_cols est interprete case-insensitive.
    """
    target_lc = {c.lower() for c in candidate_cols}

    # Map des colonnes du df vers celles candidates (case-insensitive)
    matched_cols = [c for c in df.columns if c.lower() in target_lc]
    for col in matched_cols:
        if df[col].dtype == "object":
            converted = pd.to_numeric(df[col], errors="coerce")
            invalid_as_nan = int(converted.isna().sum())
            print(f"[Num audit] {col}: dtype=object -> {invalid_as_nan} NaN apres to_numeric")


def get_first_existing_column(df: pd.DataFrame, candidates: list[str]) -> str | None:
    """Retourne le premier nom de colonne present dans df (case-insensitive)."""
    cand_lc = [c.lower() for c in candidates]
    for col in df.columns:
        if col.lower() in cand_lc:
            return col
    return None


def encoding_text_audit(df: pd.DataFrame, name: str, max_obj_cols: int = 50) -> None:
    """Recherche rapide d'artefacts d'encodage dans les colonnes object (caractere �)."""
    obj_cols = df.select_dtypes(include="object").columns.tolist()
    if not obj_cols:
        print(f"[INFO] {name}: aucune colonne textuelle (object).")
        return

    # On limite pour eviter un cout trop eleve si dataset large
    obj_cols = obj_cols[:max_obj_cols]
    sample = df.head(min(len(df), SAMPLE_ENCODING_NROWS))

    replacement_counts: dict[str, int] = {}
    for col in obj_cols:
        s = sample[col].astype(str)
        replacement_counts[col] = int(s.str.contains(REPLACEMENT_CHAR, na=False).sum())

    top = sorted(replacement_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    print(f"\n--- Audit encodage (caractere {REPLACEMENT_CHAR}) : {name} ---")
    print("Top colonnes par artefacts sur echantillon :")
    for col, c in top:
        print(f"- {col}: {c}")


def title_priority_audit(
    df: pd.DataFrame,
    title_cols: list[str] | None = None,
    weights: dict[str, float] | None = None,
    weighted_threshold: float = 0.8,
) -> None:
    """Audit de priorité des colonnes titre avec score pondéré de couverture utile."""
    if title_cols is None:
        title_cols = TITLE_COLUMNS
    if weights is None:
        weights = {"Name": 0.5, "English name": 0.3, "Japanese name": 0.2}

    print("\n--- Audit priorité titres (Name / English name / Japanese name) ---")
    coverages: dict[str, float] = {}
    for col in title_cols:
        if col not in df.columns:
            coverages[col] = 0.0
            print(f"- {col}: colonne absente")
            continue
        series = df[col].astype(str).str.strip()
        valid = (series != "") & (series.str.lower() != "unknown") & (series.str.lower() != "nan")
        cov = float(valid.mean())
        coverages[col] = cov
        print(f"- {col}: couverture utile={cov:.4%}")

    total_weight = sum(weights.get(c, 0.0) for c in title_cols) or 1.0
    weighted_cov = sum(coverages.get(c, 0.0) * weights.get(c, 0.0) for c in title_cols) / total_weight
    print(
        f"[Title priority] score pondéré={weighted_cov:.4%} | "
        f"seuil recommandé={weighted_threshold:.0%} | poids={weights}"
    )
    if weighted_cov < weighted_threshold:
        print("[WARN] Score pondéré sous le seuil: envisager suppression des colonnes titres faibles dans le gold.")
    else:
        print("[OK] Score pondéré conforme au seuil.")


def main() -> None:
    if not PATH_ANIME.exists():
        raise FileNotFoundError(f"Introuvable: {PATH_ANIME}")
    if not PATH_RATINGS.exists():
        raise FileNotFoundError(f"Introuvable: {PATH_RATINGS}")
    if not PATH_SYNOPSIS.exists():
        raise FileNotFoundError(f"Introuvable: {PATH_SYNOPSIS}")

    # ============
    # Chargement
    # ============
    anime = read_csv_smart(PATH_ANIME, test_encoding=TEST_ENCODING)
    rating_complete = read_csv_smart(PATH_RATINGS, test_encoding=TEST_ENCODING)
    anime_synopsis = read_csv_smart(PATH_SYNOPSIS, test_encoding=TEST_ENCODING)

    print("\n--- Dimensions après chargement ---")
    print("anime :", anime.shape)
    print("rating_complete :", rating_complete.shape)
    print("anime_with_synopsis :", anime_synopsis.shape)

    # ============
    # Exploration + audit console
    # ============
    print("\n--- anime.info() ---")
    print(anime.info())
    print("\n--- rating_complete.info() ---")
    print(rating_complete.info())
    print("\n--- anime_synopsis.info() ---")
    print(anime_synopsis.info())

    # Dans ton dataset, la cle primaire anime est souvent MAL_ID (et non anime_id)
    anime_key = get_first_existing_column(anime, ["anime_id", "MAL_ID"])
    synopsis_key = get_first_existing_column(anime_synopsis, ["anime_id", "MAL_ID"])

    audit_dataframe(anime, "anime", business_keys=[anime_key] if anime_key else None)
    audit_dataframe(rating_complete, "rating_complete", business_keys=["user_id", "anime_id"])
    audit_dataframe(anime_synopsis, "anime_with_synopsis", business_keys=[synopsis_key] if synopsis_key else None)

    # ============
    # Doublons métier (si colonnes presentes)
    # ============
    if anime_key:
        print(f"Doublons métier anime sur {anime_key} :", int(anime.duplicated(subset=[anime_key]).sum()))
    if synopsis_key:
        print(
            f"Doublons métier anime_with_synopsis sur {synopsis_key} :",
            int(anime_synopsis.duplicated(subset=[synopsis_key]).sum()),
        )

    if {"user_id", "anime_id"}.issubset(rating_complete.columns):
        print(
            "Doublons métier rating_complete sur (user_id, anime_id) :",
            int(rating_complete.duplicated(subset=["user_id", "anime_id"]).sum()),
        )

    # ============
    # Types incohérents (colonnes supposées numériques)
    # ============
    numeric_string_audit(anime, candidate_cols=["episodes", "score", "rank", "popularity", "members", "ranked"])
    numeric_string_audit(rating_complete, candidate_cols=["rating"])
    numeric_string_audit(anime_synopsis, candidate_cols=[])

    # ============
    # Encodage japonais (artefacts �)
    # ============
    encoding_text_audit(anime, "anime")
    encoding_text_audit(anime_synopsis, "anime_with_synopsis")
    title_priority_audit(anime)

    # ============
    # Contrôles métier (exemples)
    # ============
    score_col = get_first_existing_column(anime, ["score"])
    episodes_col = get_first_existing_column(anime, ["episodes"])
    if score_col:
        score_num = pd.to_numeric(anime[score_col], errors="coerce")
        out_of_range = anime[(score_num < 0) | (score_num > 10)]
        print(f"\n[Metier] score out of range (0..10) - lignes: {len(out_of_range)}")

    if episodes_col:
        ep_num = pd.to_numeric(anime[episodes_col], errors="coerce")
        out_ep = anime[ep_num < 0]
        print(f"[Metier] episodes < 0 - lignes: {len(out_ep)}")

    if "rating" in rating_complete.columns:
        rating_num = pd.to_numeric(rating_complete["rating"], errors="coerce")
        out_rating = rating_complete[(rating_num < 1) | (rating_num > 10)]
        print(f"[Metier] rating out of range (1..10) - lignes: {len(out_rating)}")

    # ============
    # Profiling automatique (HTML) - optionnel
    # ============
    if ProfileReport is None:
        print("\n[WARN] ydata-profiling n'est pas installe. Les rapports HTML ne seront pas generes.")
        print("Installe: pip install ydata-profiling")
        return

    print("\n[Profiling] Generating report for anime.csv ...")
    # Pattern demande (utile en notebook / env interactif)
    profile = ProfileReport(anime, title="Rapport anime", explorative=True)
    _ = profile.to_notebook_iframe()
    # Export HTML (utile depuis un script)
    profile.to_file(str(BASE_DIR / "rapport_anime.html"))

    print("[Profiling] Generating report for anime_with_synopsis.csv ...")
    profile_syn = ProfileReport(
        anime_synopsis,
        title="Rapport anime_with_synopsis",
        explorative=True,
    )
    _ = profile_syn.to_notebook_iframe()
    profile_syn.to_file(str(BASE_DIR / "rapport_anime_with_synopsis.html"))

    print("[Profiling] Generating report for rating_complete.csv (sample si necessaire) ...")
    n_max = 100_000
    if len(rating_complete) > n_max:
        rating_sample = rating_complete.sample(n_max, random_state=42)
    else:
        rating_sample = rating_complete

    profile_rating = ProfileReport(
        rating_sample,
        title="Rapport de profilage - rating_complete sample",
        explorative=True,
    )
    _ = profile_rating.to_notebook_iframe()
    profile_rating.to_file(str(BASE_DIR / "rapport_rating_complete_sample.html"))

    print("\nTermine.")


if __name__ == "__main__":
    main()

