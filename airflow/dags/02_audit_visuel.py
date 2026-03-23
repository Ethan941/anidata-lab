"""
🎌 AniData Lab — Audit visuel (génération de graphiques)
=========================================================
Séance 1 — Lundi 23 mars 2026 — Après-midi

Ce script génère des graphiques d'audit dans le dossier output/audit_charts/
Lancez-le APRÈS 01_audit_complet.py

Usage : python 02_audit_visuel.py
Prérequis : pip install pandas matplotlib seaborn
"""

import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use("Agg")  # Pas besoin d'affichage graphique
import seaborn as sns
import os

# ============================================
# CONFIG
# ============================================
DATA_DIR = "data"
OUTPUT_DIR = "output/audit_charts"
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Style des graphiques
plt.rcParams.update({
    "figure.facecolor": "#1B2545",
    "axes.facecolor": "#232E4A",
    "axes.edgecolor": "#5B8DEF",
    "axes.labelcolor": "#C8D6E5",
    "text.color": "#C8D6E5",
    "xtick.color": "#C8D6E5",
    "ytick.color": "#C8D6E5",
    "grid.color": "#2A3558",
    "font.family": "sans-serif",
    "font.size": 11,
})
PALETTE = ["#5B8DEF", "#4ECDC4", "#E94560", "#F39C12", "#2ECC71", "#9B59B6"]

print("🎌 AniData Lab — Génération des graphiques d'audit\n")

# ============================================
# CHARGEMENT
# ============================================
print("  Chargement de anime.csv...")
anime = pd.read_csv(os.path.join(DATA_DIR, "anime.csv"))
print(f"  ✅ {anime.shape[0]:,} lignes chargées\n")

chart_num = 0

def save_chart(name):
    global chart_num
    chart_num += 1
    filepath = os.path.join(OUTPUT_DIR, f"{chart_num:02d}_{name}.png")
    plt.savefig(filepath, dpi=150, bbox_inches="tight", facecolor="#1B2545")
    plt.close()
    print(f"  📊 Graphique {chart_num} sauvegardé : {filepath}")


# ============================================
# GRAPHIQUE 1 — Valeurs manquantes par colonne
# ============================================
print("\n--- Graphique 1 : Valeurs manquantes ---")
missing = anime.isnull().sum().sort_values(ascending=True)
missing = missing[missing > 0]

if len(missing) > 0:
    fig, ax = plt.subplots(figsize=(10, max(4, len(missing) * 0.4)))
    bars = ax.barh(missing.index, missing.values, color=PALETTE[2], alpha=0.85)
    ax.set_xlabel("Nombre de valeurs manquantes")
    ax.set_title("Valeurs manquantes par colonne — anime.csv", fontsize=14, fontweight="bold", color="white")
    for bar, val in zip(bars, missing.values):
        ax.text(bar.get_width() + 50, bar.get_y() + bar.get_height()/2,
                f"{val:,}", va="center", fontsize=9, color="#C8D6E5")
    ax.grid(axis="x", alpha=0.3)
    save_chart("missing_values")
else:
    print("  ℹ️  Pas de NaN classiques — on vérifie les NaN déguisés...")


# ============================================
# GRAPHIQUE 2 — NaN déguisés (score = 0, Unknown, etc.)
# ============================================
print("\n--- Graphique 2 : NaN déguisés ---")
disguised = {}
for col in anime.columns:
    if anime[col].dtype == object:
        count = anime[col].isin(["Unknown", "unknown", "N/A", "-", "None", ""]).sum()
        if count > 0:
            disguised[col] = count

# Vérifier score = 0
for col in anime.columns:
    if col.lower() == "score":
        score_data = pd.to_numeric(anime[col], errors="coerce")
        zeros = (score_data == 0).sum()
        if zeros > 0:
            disguised[f"{col} (= 0)"] = zeros
... (220lignes restantes)