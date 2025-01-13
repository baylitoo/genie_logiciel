import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import seaborn as sns
from typing import List

class DataProcessor:
    def __init__(
        self,
        water_files: List[str],
        geojson_path: str,
        rent_files: List[str],
        pop_file: str = None,
        output_dir: str = "figures"
    ):
        """
        Classe pour traiter les données sur la qualité de l'eau, les loyers (plusieurs fichiers),
        les données géographiques, et la population.

        :param water_files: Liste des chemins vers les fichiers de données sur l'eau.
        :param geojson_path: Chemin vers le fichier GeoJSON des communes.
        :param rent_files: Liste des chemins vers les fichiers de loyers.
        :param pop_file: Chemin vers le fichier XLS contenant les données de population.
        :param output_dir: Répertoire où sauvegarder les figures générées.
        """
        self.water_files = water_files
        self.geojson_path = geojson_path
        self.rent_files = rent_files
        self.pop_file = pop_file
        self.output_dir = output_dir

        self.geo_data = None
        self.water_data = None
        self.pop_data = None
        self.rent_data = None

    def load_water_data(self):
        """Charger et concaténer les données sur la qualité de l'eau."""
        all_data = []
        for file in self.water_files:
            df = pd.read_csv(file, delimiter=",", encoding='ISO-8859-1', low_memory=False)
            all_data.append(df)
        self.water_data = pd.concat(all_data, ignore_index=True)

    def clean_water_data(self):
        """Nettoyer et préparer les données sur l'eau."""
        self.water_data["inseecommune"] = (
            self.water_data["inseecommune"].astype(str).str.zfill(5)
        )
        self.water_data["bacterio_conformity"] = self.water_data["plvconformitebacterio"].apply(
            lambda x: 1 if x == "C" else 0
        )
        self.water_data["chemical_conformity"] = self.water_data["plvconformitechimique"].apply(
            lambda x: 1 if x == "C" else 0
        )

    def load_geo_data(self):
        """Charger les données géographiques (GeoJSON)."""
        self.geo_data = gpd.read_file(self.geojson_path)
        self.geo_data["codgeo"] = self.geo_data["codgeo"].astype(str).str.zfill(5)

    def load_rent_data(self):
        """Charger et concaténer les données de loyers."""
        all_rent = []
        for rent_file in self.rent_files:
            df = pd.read_csv(rent_file, delimiter=";", decimal=",", encoding="ISO-8859-1")
            all_rent.append(df)

        concatenated = pd.concat(all_rent, ignore_index=True)
        self.rent_data = concatenated.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        self.rent_data.rename(columns={"loypredm2": "mean_loypredm2"}, inplace=True)

    def load_population_data(self):
        """Charger les données de population."""
        if not self.pop_file:
            print("Aucun fichier population spécifié.")
            return

        self.pop_data = pd.read_excel(self.pop_file)
        self.pop_data["codgeo"] = self.pop_data["codgeo"].astype(str).str.zfill(5)

    def merge_data(self):
        """Fusionner les données."""
        # Eau
        water_agg = self.water_data.groupby("inseecommune").agg({
            "bacterio_conformity": "mean",
            "chemical_conformity": "mean"
        }).reset_index()

        self.geo_data = self.geo_data.merge(
            water_agg,
            left_on="codgeo",
            right_on="inseecommune",
            how="left"
        )

        # Loyers
        self.geo_data = self.geo_data.merge(
            self.rent_data,
            left_on="codgeo",
            right_on="INSEE_C",
            how="left"
        )

        # Population
        if self.pop_data is not None:
            self.geo_data = self.geo_data.merge(
                self.pop_data[["codgeo", "p21_pop"]],
                on="codgeo",
                how="left"
            )

    def plot_map(self, column: str, title: str, filename: str, cmap="Blues"):
        """Tracer et sauvegarder une carte."""
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        self.geo_data.plot(
            column=column,
            cmap=cmap,
            legend=True,
            missing_kwds={"color": "lightgrey", "label": "Données manquantes"},
            ax=ax
        )
        plt.title(title)
        plt.axis("off")
        plt.savefig(f"{self.output_dir}/{filename}.png", dpi=300)
        plt.close()

    def plot_correlation_heatmap(self):
        """Tracer une heatmap des corrélations."""
        columns = ["bacterio_conformity", "chemical_conformity", "mean_loypredm2"]
        if self.pop_data is not None and "p21_pop" in self.geo_data.columns:
            columns.append("p21_pop")

        analysis_data = self.geo_data[columns].dropna()
        correlation_matrix = analysis_data.corr()

        plt.figure(figsize=(8, 6))
        sns.heatmap(correlation_matrix, annot=True, cmap="coolwarm", fmt=".2f")
        plt.title("Matrice de corrélation")
        plt.tight_layout()
        plt.savefig(f"{self.output_dir}/correlation_heatmap.png", dpi=300)
        plt.close()


# ============================
# Exemple d'utilisation
# ============================
if __name__ == "__main__":
    water_files = [
        "data/CAP_PLV_202411.txt",
        "data/CAP_RES_202411.txt",
        "data/TTP_PLV_202411.txt",
        "data/TTP_RES_202411.txt",
        "data/UDI_PLV_202411.txt",
        "data/UDI_RES_202411.txt"
    ]
    geojson_path = "data/a-com2022.json"
    rent_files = [
        "data/pred-app-mef-dhup.csv",
        "data/pred-app3-mef-dhup.csv",
        "data/pred-app12-mef-dhup.csv",
        "data/pred-mai-mef-dhup.csv"
    ]
    pop_file = "data/POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx"
    output_dir = "figures"

    processor = DataProcessor(
        water_files=water_files,
        geojson_path=geojson_path,
        rent_files=rent_files,
        pop_file=pop_file,
        output_dir=output_dir
    )

    # Charger et traiter les données
    processor.load_water_data()
    processor.clean_water_data()
    processor.load_geo_data()
    processor.load_rent_data()
    processor.load_population_data()
    processor.merge_data()

    # Générer les visualisations
    processor.plot_map("bacterio_conformity", "Conformité Bactériologique", "bacterio_conformity")
    processor.plot_map("chemical_conformity", "Conformité Chimique", "chemical_conformity")
    processor.plot_map("mean_loypredm2", "Loyers Moyens par Commune", "mean_rent", cmap="Oranges")
    if processor.pop_file:
        processor.plot_map("p21_pop", "Population Municipale (2021)", "population", cmap="Greens")

    # Générer la heatmap des corrélations
    processor.plot_correlation_heatmap()
