import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from typing import List

class DataProcessor:
    def __init__(self, water_files: List[str], geojson_path: str, rent_data: pd.DataFrame):
        """
        Classe pour traiter les données sur la qualité de l'eau, les loyers, et les données géographiques.

        :param water_files: Liste des chemins vers les fichiers de données sur l'eau.
        :param geojson_path: Chemin vers le fichier GeoJSON des communes.
        :param rent_data: Données des loyers sous forme de DataFrame.
        """
        self.water_files = water_files
        self.geojson_path = geojson_path
        self.rent_data = rent_data
        self.geo_data = None
        self.water_data = None

    def load_water_data(self):
        """Charger et concaténer les données sur la qualité de l'eau."""
        all_data = []
        for file in self.water_files:
            df = pd.read_csv(file, delimiter=",", encoding='ISO-8859-1', low_memory=False)
            all_data.append(df)
        self.water_data = pd.concat(all_data, ignore_index=True)

    def clean_water_data(self):
        """Nettoyer et préparer les données sur l'eau."""
        # Standardiser les codes INSEE
        self.water_data["inseecommune"] = self.water_data["inseecommune"].astype(str).str.zfill(5)

        # Convertir les colonnes de conformité en valeurs binaires
        self.water_data["bacterio_conformity"] = self.water_data["plvconformitebacterio"].apply(lambda x: 1 if x == "C" else 0)
        self.water_data["chemical_conformity"] = self.water_data["plvconformitechimique"].apply(lambda x: 1 if x == "C" else 0)

    def load_geo_data(self):
        """Charger les données géographiques (GeoJSON)."""
        self.geo_data = gpd.read_file(self.geojson_path)
        self.geo_data["codgeo"] = self.geo_data["codgeo"].astype(str).str.zfill(5)

    def merge_data(self):
        """Fusionner les données de qualité de l'eau avec les données géographiques et de loyers."""
        # Agréger les données sur l'eau par commune
        water_agg = self.water_data.groupby("inseecommune").agg({
            "bacterio_conformity": "mean",
            "chemical_conformity": "mean"
        }).reset_index()

        # Fusion avec les données géographiques
        self.geo_data = self.geo_data.merge(water_agg, left_on="codgeo", right_on="inseecommune", how="left")

        # Agréger les données des loyers par commune
        rent_agg = self.rent_data.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        self.geo_data = self.geo_data.merge(rent_agg, left_on="codgeo", right_on="INSEE_C", how="left")

    def plot_water_quality(self, column: str, title: str):
        """Tracer la qualité de l'eau sur une carte."""
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        self.geo_data.plot(
            column=column,
            cmap="Blues",
            legend=True,
            missing_kwds={"color": "red", "label": "Données manquantes"},
            ax=ax
        )
        plt.title(title)
        plt.axis("off")
        plt.show()

    def analyze_correlation(self):
        """Analyser les corrélations entre la qualité de l'eau, les loyers et d'autres variables."""
        analysis_data = self.geo_data[["bacterio_conformity", "chemical_conformity", "loypredm2"]].dropna()
        correlation_matrix = analysis_data.corr()
        print("Matrice de corrélation :")
        print(correlation_matrix)

# Exemple d'utilisation
if __name__ == "__main__":
    water_files = ["data/CAP_PLV_202411.txt", "data/CAP_RES_202411.txt", "data/TTP_PLV_202411.txt", "data/TTP_RES_202411.txt", "data/UDI_PLV_202411.txt", "data/UDI_RES_202411.txt"]
    geojson_path = "data/a-com2022.json"
    rent_data = pd.read_csv("data\pred-app-mef-dhup.csv", delimiter=";", decimal=",", encoding="ISO-8859-1")

    processor = DataProcessor(water_files, geojson_path, rent_data)
    processor.load_water_data()
    processor.clean_water_data()
    processor.load_geo_data()
    processor.merge_data()
    processor.plot_water_quality("bacterio_conformity", "Conformité Bactériologique de l'Eau")
    processor.analyze_correlation()
