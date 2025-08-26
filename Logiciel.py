import json
import pandas as pd
import re
import requests
from pathlib import Path
from typing import Optional

# === CONFIG ===
PARCELLES_FILE = Path("parcelles_a_aplatirTEST1.json")       # ton JSON d'entrée avec parcelles
SIREN_FILE = Path("sirens.txt")               # liste de siren extraits
ENTREPRISES_FILE = Path("entreprises.json")   # JSON sauvegardé des réponses API
OUTPUT_XLSX = Path("fusion_finale2.xlsx")      # Excel final fusionné

# ⚠️ Mets ta vraie clé API ici
API_KEY = "772efebfeb103f49bfc49b9e4b7fceede51850312d8a45af"
API_URL = "https://api.pappers.fr/v2/entreprise"

# Colonnes attendues dans l'Excel final
COLONNES_FINALES = [
    "siren","nom_entreprise","denomination","prenom","nom",
    "siege.numero_voie","siege.indice_repetition","siege.type_voie",
    "siege.libelle_voie","siege.complement_adresse",
    "siege.adresse_ligne_1","siege.adresse_ligne_2",
    "siege.code_postal","siege.ville","siege.pays","adresse_siege"
]


# --- UTILS ---

def normalize_siren(value) -> Optional[str]:
    """Nettoie le siren, garde 9 chiffres uniquement."""
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits.zfill(9) if digits else None


def load_json_file(path: Path):
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def save_json_file(data, path: Path):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# --- PARCELLES ---

def extract_parcelles(json_data) -> pd.DataFrame:
    """Extrait siren + adresse depuis le JSON des parcelles."""
    records = []
    for parcelle in json_data:
        adresse = parcelle.get("adresse", "").strip().lower()
        for prop in parcelle.get("proprietaires", []):
            siren = normalize_siren(prop.get("siren"))
            if siren:
                records.append({"siren": siren, "adresse": adresse})
    return pd.DataFrame(records).drop_duplicates()


# --- API ENTREPRISES ---

def fetch_api_for_siren(siren: str):
    """Appelle l'API Pappers pour un SIREN donné."""
    try:
        resp = requests.get(API_URL, params={"api_token": API_KEY, "siren": siren}, timeout=10)
        if resp.status_code == 200:
            return resp.json()
        else:
            print(f"⚠️ Erreur API {siren}: {resp.status_code}")
            return None
    except Exception as e:
        print(f"⚠️ Exception API {siren}: {e}")
        return None


def fetch_all_api(sirens, save_path: Path):
    """Boucle sur tous les SIREN et sauvegarde le JSON brut."""
    results = []
    for s in sirens:
        data = fetch_api_for_siren(s)
        if data:
            results.append(data)
    save_json_file(results, save_path)
    return results


# --- ENTREPRISES ---

def prepare_entreprises(api_results) -> pd.DataFrame:
    """Normalise les résultats API et garde les colonnes utiles."""
    if not api_results:
        return pd.DataFrame(columns=COLONNES_FINALES)

    df = pd.json_normalize(api_results)

    if "siren" not in df.columns:
        return pd.DataFrame(columns=COLONNES_FINALES)

    df["siren"] = df["siren"].apply(normalize_siren)

    # Adresse simplifiée
    if "siege.adresse_ligne_1" in df.columns:
        df["adresse_siege"] = (
            df["siege.adresse_ligne_1"].fillna("").astype(str).str.strip().str.lower()
            + " " + df.get("siege.code_postal", "").fillna("").astype(str)
            + " " + df.get("siege.ville", "").fillna("").astype(str).str.lower()
        )
    else:
        df["adresse_siege"] = ""

    colonnes_presentes = [c for c in COLONNES_FINALES if c in df.columns]
    if "adresse_siege" not in colonnes_presentes:
        colonnes_presentes.append("adresse_siege")

    return df[colonnes_presentes].drop_duplicates(subset=["siren", "adresse_siege"])


# --- MAIN ---

def main():
    # Charger parcelles
    parcelles_data = load_json_file(PARCELLES_FILE)
    df_parcelles = extract_parcelles(parcelles_data)

    # Sauvegarder liste SIREN
    df_parcelles["siren"].dropna().drop_duplicates().to_csv(SIREN_FILE, index=False, header=False)
    print(f"✅ Sirens extraits -> {SIREN_FILE}")

    # Charger ou appeler l'API
    if ENTREPRISES_FILE.exists():
        print("📂 Chargement du fichier entreprises JSON déjà existant...")
        api_results = load_json_file(ENTREPRISES_FILE)
    else:
        print("🌍 Appels API en cours...")
        sirens = df_parcelles["siren"].dropna().unique().tolist()
        api_results = fetch_all_api(sirens, ENTREPRISES_FILE)
        print(f"✅ Données API sauvegardées -> {ENTREPRISES_FILE}")

    # Préparer entreprises
    df_entreprises = prepare_entreprises(api_results)

    # Fusion
    fusion = pd.merge(
        df_parcelles,
        df_entreprises,
        on="siren",
        how="outer",
        suffixes=("_parcelles", "_entreprise"),
        indicator=True
    ).sort_values(by="siren")

    # Séparer les cas
    fusion_complete = fusion[fusion["_merge"] == "both"].drop(columns=["_merge"])
    seulement_parcelles = fusion[fusion["_merge"] == "left_only"].drop(columns=["_merge"])
    seulement_entreprises = fusion[fusion["_merge"] == "right_only"].drop(columns=["_merge"])

    # Export Excel
    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        fusion_complete.to_excel(writer, index=False, sheet_name="fusion")
        seulement_parcelles.to_excel(writer, index=False, sheet_name="seulement_parcelles")
        seulement_entreprises.to_excel(writer, index=False, sheet_name="seulement_entreprises")

    print("✅ Fichier Excel généré :", OUTPUT_XLSX.resolve())


if __name__ == "__main__":
    main()