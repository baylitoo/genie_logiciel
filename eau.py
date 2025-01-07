import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from typing import List

class WaterQualityProcessor:
    def __init__(self, files: List[str], delimiter=","):
        self.files = files
        self.delimiter = delimiter
        self.data = None

    def load_files(self):
        """Charger et concaténer les fichiers d'analyse de l'eau."""
        all_data = []
        for file in self.files:
            df = pd.read_csv(file, delimiter=self.delimiter, encoding='ISO-8859-1', low_memory=False)
            all_data.append(df)
        self.data = pd.concat(all_data, ignore_index=True)

    def clean_data(self):
        """Nettoyer les colonnes et standardiser les codes INSEE."""
        self.data["inseecommune"] = self.data["inseecommune"].astype(str).str.zfill(5)
        # Convertir les colonnes à des types numériques
        self.data["plvconformitebacterio"] = pd.to_numeric(
            self.data["plvconformitebacterio"], errors="coerce"
        )
        self.data["plvconformitechimique"] = pd.to_numeric(
            self.data["plvconformitechimique"], errors="coerce"
        )

    def aggregate_by_commune(self):
        """Agréger les données par commune."""
        agg = self.data.groupby("inseecommune").agg({
            "plvconformitebacterio": "mean",
            "plvconformitechimique": "mean"
        }).reset_index()
        agg.rename(columns={
            "plvconformitebacterio": "bacterio_conformity",
            "plvconformitechimique": "chemical_conformity"
        }, inplace=True)
        return agg

    def merge_with_geo_data(self, geo_data: gpd.GeoDataFrame):
        """Fusionner les données d'eau avec les données géographiques."""
        water_data = self.aggregate_by_commune()
        geo_merged = geo_data.merge(water_data, left_on="codgeo", right_on="inseecommune", how="left")
        return geo_merged


# Utilisation de la classe
files = [
    r"data\CAP_PLV_202411.txt",
    r"data\CAP_RES_202411.txt",
    r"data\TTP_PLV_202411.txt",
    r"data\TTP_RES_202411.txt",
    r"data\UDI_PLV_202411.txt",
    r"data\UDI_RES_202411.txt",
]

processor = WaterQualityProcessor(files)
processor.load_files()
processor.clean_data()

# Charger le GeoJSON
geojson_path = "data/a-com2022.json"
try:
    communes_geo = gpd.read_file(geojson_path)
except FileNotFoundError:
    print(f"Le fichier {geojson_path} est introuvable.")
    exit()

geo_merged = processor.merge_with_geo_data(communes_geo)

# Visualiser les résultats
fig, ax = plt.subplots(1, 1, figsize=(12, 10))
geo_merged.plot(
    column="bacterio_conformity",
    cmap="Blues",
    legend=True,
    missing_kwds={"color": "lightgrey", "label": "Données manquantes"},
    ax=ax
)
plt.title("Conformité bactériologique de l'eau par commune")
plt.axis("off")
plt.show()
