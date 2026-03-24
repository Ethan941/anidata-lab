"""
🎌 AniData Lab — Nettoyage du dataset anime.csv
=================================================
Séance 2 — Mardi 24 mars 2026 — Matin (Partie 1/3)

Ce script applique toutes les corrections identifiées lors de l'audit :
  - Traitement des valeurs manquantes (classiques et déguisées)
  - Suppression des doublons
  - Correction des types de données
  - Normalisation des encodages et formats
  - Nettoyage des colonnes multi-valuées (genres, studios...)

Usage : python 03_nettoyage.py
Entrée : data/anime.csv
Sortie : output/anime_cleaned.csv
"""

import pandas as pd
import numpy as np
import os
import sys

# ============================================
# CONFIG
# ============================================
DATA_DIR = "data"
OUTPUT_DIR = "output"
os.makedirs(OUTPUT_DIR, exist_ok=True)

INPUT_FILE = os.path.join(DATA_DIR, "anime.csv")
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "anime_cleaned.csv")

# Couleurs terminal
class C:
    H = "\033[95m"; B = "\033[94m"; G = "\033[92m"
    W = "\033[93m"; F = "\033[91m"; BOLD = "\033[1m"; END = "\033[0m"

def titre(t):
    print(f"\n{C.BOLD}{C.H}{'='*60}\n  {t}\n{'='*60}{C.END}\n")

def step(t):
    print(f"\n{C.BOLD}{C.B}--- {t} ---{C.END}")

def ok(t):
    print(f"  {C.G}✅ {t}{C.END}")

def warn(t):
    print(f"  {C.W}⚠️  {t}{C.END}")

def info(t):
    print(f"  {C.B}ℹ️  {t}{C.END}")

def delta(before, after, label):
    diff = before - after
    print(f"  {C.G}✅ {label} : {before:,} → {after:,} ({diff:,} retirées, -{diff/before*100:.1f}%){C.END}")


# ============================================
# CHARGEMENT
# ============================================
titre("NETTOYAGE — anime.csv")

if not os.path.exists(INPUT_FILE):
    print(f"{C.F}❌ Fichier introuvable : {INPUT_FILE}{C.END}")
    sys.exit(1)

print("  Chargement du fichier brut...")
df_raw = pd.read_csv(INPUT_FILE)
ok(f"Fichier chargé : {df_raw.shape[0]:,} lignes × {df_raw.shape[1]} colonnes")

# Copie de travail (on ne touche jamais au raw)
df = df_raw.copy()
n_initial = len(df)

print(f"\n  Colonnes : {list(df.columns)}")


# ============================================
# ÉTAPE 1 — NORMALISATION DES NOMS DE COLONNES
# ============================================
step("Étape 1 : Normalisation des noms de colonnes")

# Mettre tous les noms en snake_case minuscule
old_cols = list(df.columns)
df.columns = (
    df.columns
    .str.strip()
    .str.lower()
    .str.replace(" ", "_")
    .str.replace("-", "_")
)
new_cols = list(df.columns)

renamed = [(o, n) for o, n in zip(old_cols, new_cols) if o != n]
if renamed:
    for old, new in renamed:
        info(f"  '{old}' → '{new}'")
    ok(f"{len(renamed)} colonne(s) renommée(s)")
else:
    ok("Noms de colonnes déjà normalisés")
#... (271lignes restantes)