#code2

import requests # Pour les requetes API
import argparse # Pour recuperer les arguments
import pandas as pd # Pour sauvegarder le fichier excel
#import json

API_KEY = "772efebfeb103f49bfc49b9e4b7fceede51850312d8a45af"

# Recupere le fichier des sirens et le fichier de sortie
parser = argparse.ArgumentParser(prog='Tunnasse', description='Fait de la thune')
parser.add_argument("siren_file")
# parser.add_argument("output_file", nargs='?', default='output.xlsx')
args = parser.parse_args()

result = []
registered = {}
with open(args.siren_file, 'r') as f:
	# Lit le fichier et boucle sur les sirens
	sirens = f.read()
	for siren in sirens.split('\n'):
		# Verifie que le siren n'a pas déjà été récuperé
		if siren in registered:
			result.append(registered[siren])
			continue

		# Appel API
		url = f"https://api.pappers.fr/v2/entreprise?api_token={API_KEY}&siren={siren}"
		print(url)
		data = requests.request('GET', url)

		# Recupere seulement les status codes 200
		if int(data.status_code / 100) == 2:
			result.append(data.json())
			registered[siren] = data.json()

elegant = json.dumps(result, indent=4, ensure_ascii=False)
print(elegant)


with open("ENTREPRISE75011PROP=1TESTUNIONTEXTTEST2.txt", "w", encoding="utf-8") as fichier:
   json.dump(result, fichier, indent=4, ensure_ascii=False)



# Sauvegarde le resultat en json
#df = pd.DataFrame(result)
#df.to_json('ENTREPRISE75011PROP=1TESTUNION.json', index=False)

# python3 CODEFINAL.py .\siren.txt 

###########################################################################################################

# Lire le contenu du fichier texte
with open('ENTREPRISE75011PROP=1TESTUNIONTEXTTEST2.txt', 'r', encoding='utf-8') as txt_file:
    contenu = txt_file.read()

# Charger le JSON à partir du texte
try:
    data = json.loads(contenu)  # Convertir le texte en JSON
except json.JSONDecodeError as e:
    print(f"Erreur de décodage JSON : {e}")
    exit()

# Sauvegarder dans un fichier JSON
with open('ENTREPRISE_ETAPE_3TEST2.json', 'w', encoding='utf-8') as json_file:
    json.dump(data, json_file, indent=4, ensure_ascii=False)

print("✅ Conversion terminée : ENTREPRISE_ETAPE_3.json créé avec succès !")

#.\API\test\sirentest.txt

################################################################################################################################

import requests # Pour les requetes API
import argparse # Pour recuperer les arguments
import pandas as pd # Pour sauvegarder le fichier excel
import json

# 1) Charger le fichier -------------------------------------------------------
# (Adaptez le chemin si besoin)
with open("ENTREPRISE_ETAPE_3TEST2.json", encoding="utf-8") as f:
    data = json.load(f)

# 2) Transformer en DataFrame + aplatir 'siege' -------------------------------
df = pd.json_normalize(data)           # « siege.xxx » devient déjà des colonnes
siege_cols = [c for c in df.columns if c.startswith("siege.")]

# 3) Conserver uniquement siren + colonnes du siège ---------------------------
# out = df[["siren","nom_entreprise", "denomination", "prenom", "nom", *siege_cols]].copy()
out = df[["siren","nom_entreprise", "denomination", "prenom", "nom", "siege.numero_voie", "siege.indice_repetition", "siege.type_voie", "siege.libelle_voie", "siege.complement_adresse","siege.adresse_ligne_1","siege.adresse_ligne_2", "siege.code_postal", "siege.ville", "siege.pays" ]]


# 4) (Optionnel) Enlever le préfixe "siege." pour plus de lisibilité ----------
# out.columns = [c.replace("siege.", "") for c in out.columns]

# 5) Sauvegarder le résultat --------------------------------------------------
#out.to_csv("entreprises_siege_flat.csv", index=False, encoding="utf-8")
out.to_json("entreprises_siege_comparaisonTEST2.json", orient="records", force_ascii=False, indent=2)

print("✅ Conversion terminée : entreprises_siege_comparaison.json créé avec succès !")


##########################################################################################################################

import json

def beautify_json(input_file: str, output_file: str):
    with open(input_file, 'r', encoding='utf-8') as f:
        data = json.load(f)

    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

    print(f"JSON beautifié sauvegardé dans {output_file}")

if __name__ == "__main__":
    beautify_json("Parcelle75011ori.json", "parcelles_a_aplatirTEST2.json") #MODIFIER NOM FICHIER PARCELLE!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

    print("✅ Conversion terminée : Parcelles_a_aplatir.json créé avec succès !")


################################################################################################################################


import json
import pandas as pd
import pathlib

# 1. Charger la source JSON
src = pathlib.Path("parcelles_a_aplatirTEST2.json")
with src.open(encoding="utf-8") as f:
    data = json.load(f)

# 2. Si 'resultats' est une clé, l'utiliser comme base
if isinstance(data, dict) and "resultats" in data:
    data = data["resultats"]

# 3. Filtrer les objets qui ont des propriétaires valides
data_filtré = [item for item in data if "proprietaires" in item and item["proprietaires"]]

# 4. Aplatir en extraire siren + adresse
df = pd.json_normalize(
    data_filtré,
    record_path=["proprietaires"],
    meta=["adresse"]
)[["adresse", "siren"]]

# 5. Nettoyage et suppression des doublons
df["adresse"] = df["adresse"].astype(str).str.strip().str.lower()
df["siren"] = df["siren"].astype(str).str.strip()
df = df.drop_duplicates(subset=["siren", "adresse"])

# 6. Export JSON élégant
df.to_json("parcelles_adresses_sirens_75011TEST2.json",
           orient="records",
           force_ascii=False,
           indent=2)

print("✅ JSON exporté avec succès : adresses_sirens_75011_V2.json")

#################################################################################################################################

import json
import pandas as pd
import re
from pathlib import Path
from typing import Optional

# === FICHIERS ===
PARCELLES_FILE = Path("parcelles_adresses_sirens_75011TEST2.json")
ENTREPRISE_FILE = Path("entreprises_siege_comparaisonTEST2.json")
OUTPUT_XLSX = Path("fusion_parcelles_entreprises_unique_siren_adresse75011TEST2.xlsx")


def normalize_siren(value) -> Optional[str]:
    """Garde uniquement les chiffres et pad à gauche pour avoir 9 caractères."""
    if pd.isna(value):
        return None
    digits = re.sub(r"\D", "", str(value))
    return digits.zfill(9) if digits else None


def load_json_file(path: Path) -> pd.DataFrame:
    """Charge un fichier JSON (liste d'objets) en DataFrame."""
    with open(path, encoding="utf-8") as f:
        data = json.load(f)
    return pd.json_normalize(data)


def prepare_parcelles(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if "siren" not in df.columns or "adresse" not in df.columns:
        raise ValueError(f"{source_name} doit contenir 'siren' et 'adresse'")
    df = df.copy()
    df["siren"] = df["siren"].apply(normalize_siren)
    df["adresse"] = df["adresse"].astype(str).str.strip().str.lower()
    df = df.drop_duplicates(subset=["siren", "adresse"])
    return df


def prepare_entreprises(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    if "siren" not in df.columns:
        raise ValueError(f"{source_name} ne contient pas de colonne 'siren'")

    df = df.copy()
    df["siren"] = df["siren"].apply(normalize_siren)

    # Construire l'adresse complète (ou adapter selon les colonnes présentes)
    df["adresse_siege"] = df["siege.adresse_ligne_1"].fillna("").astype(str).str.strip().str.lower()
    df["adresse_siege"] += " " + df["siege.code_postal"].fillna("").astype(str)
    df["adresse_siege"] += " " + df["siege.ville"].fillna("").astype(str).str.lower()

    df = df.drop_duplicates(subset=["siren", "adresse_siege"])
    return df


def main():
    # Chargement
    df_parcelles = load_json_file(PARCELLES_FILE)
    df_entreprises = load_json_file(ENTREPRISE_FILE)

    # Préparation
    df_parcelles = prepare_parcelles(df_parcelles, PARCELLES_FILE)
    df_entreprises = prepare_entreprises(df_entreprises, ENTREPRISE_FILE)

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