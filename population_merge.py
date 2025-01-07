import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from typing import List

class DataProcessor:
    def __init__(
        self,
        water_files: List[str],
        geojson_path: str,
        rent_data: pd.DataFrame,
        pop_file: str = None
    ):
        """
        Classe pour traiter les données sur la qualité de l'eau, les loyers, les données géographiques,
        et désormais la population par commune.

        :param water_files: Liste des chemins vers les fichiers de données sur l'eau.
        :param geojson_path: Chemin vers le fichier GeoJSON des communes.
        :param rent_data: Données des loyers sous forme de DataFrame.
        :param pop_file: Chemin vers le fichier XLS contenant les données de population.
        """
        self.water_files = water_files
        self.geojson_path = geojson_path
        self.rent_data = rent_data
        self.pop_file = pop_file

        self.geo_data = None
        self.water_data = None
        self.pop_data = None

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
        self.water_data["inseecommune"] = (
            self.water_data["inseecommune"].astype(str).str.zfill(5)
        )
        # Convertir les colonnes de conformité en valeurs binaires
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

    def merge_data(self):
        """
        Fusionner les données de qualité de l'eau avec les données géographiques et de loyers,
        puis (si disponible) ajouter la population.
        """
        # ----- Eau -----
        water_agg = self.water_data.groupby("inseecommune").agg({
            "bacterio_conformity": "mean",
            "chemical_conformity": "mean"
        }).reset_index()

        # Fusion avec la GeoDataFrame
        self.geo_data = self.geo_data.merge(
            water_agg,
            left_on="codgeo",
            right_on="inseecommune",
            how="left"
        )

        # ----- Loyers -----
        rent_agg = self.rent_data.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        self.geo_data = self.geo_data.merge(
            rent_agg,
            left_on="codgeo",
            right_on="INSEE_C",
            how="left"
        )

        # ----- Population (si fichier spécifié) -----
        if self.pop_data is not None:
            # On suppose que 'codgeo' est déjà standardisé dans pop_data
            # et que p21_pop contient la population la plus récente.
            self.geo_data = self.geo_data.merge(
                self.pop_data[["codgeo", "p21_pop"]],
                on="codgeo",
                how="left"
            )

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
        """
        Analyser les corrélations entre la qualité de l'eau, les loyers,
        la population (si disponible), et d'autres variables.
        """
        columns_of_interest = ["bacterio_conformity", "chemical_conformity", "loypredm2"]

        # Si on a effectivement chargé la population, on l'ajoute :
        if self.pop_data is not None and "p21_pop" in self.geo_data.columns:
            columns_of_interest.append("p21_pop")

        analysis_data = self.geo_data[columns_of_interest].dropna()
        correlation_matrix = analysis_data.corr()
        print("Matrice de corrélation :")
        print(correlation_matrix)

    def load_population_data(self):
        """
        Charger les données de population depuis un fichier XLS
        et les stocker dans self.pop_data. On suppose que le champ
        'codgeo' existe déjà et correspond à l'INSEE des communes.
        """
        if not self.pop_file:
            print("Aucun fichier population spécifié.")
            return

        # Lire le fichier XLS
        self.pop_data = pd.read_excel(self.pop_file)

        # Standardiser codgeo
        self.pop_data["codgeo"] = self.pop_data["codgeo"].astype(str).str.zfill(5)

        # (Optionnel) vous pouvez renommer la colonne de population la plus récente si besoin :
        # self.pop_data.rename(columns={"p21_pop": "population_2024"}, inplace=True)

        print("Population data loaded and cleaned.")


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
    rent_data = pd.read_csv("data/pred-app-mef-dhup.csv", delimiter=";", decimal=",", encoding="ISO-8859-1")
    pop_file = "data\POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx"  # <- Chemin vers votre fichier population

    processor = DataProcessor(water_files, geojson_path, rent_data, pop_file=pop_file)

    # 1. Charger et nettoyer les données sur l'eau
    processor.load_water_data()
    processor.clean_water_data()

    # 2. Charger les données géographiques
    processor.load_geo_data()

    # 3. Charger les données de population
    processor.load_population_data()

    # 4. Fusionner le tout
    processor.merge_data()

    # 5. Analyse de la qualité de l'eau (exemple)
    processor.plot_water_quality("bacterio_conformity", "Conformité Bactériologique de l'Eau")

    # 6. Analyser les corrélations (y compris la population)
    processor.analyze_correlation()









import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
from typing import List

class DataProcessor:
    def __init__(
        self,
        water_files: List[str],
        geojson_path: str,
        rent_files: List[str],
        pop_file: str = None
    ):
        """
        Classe pour traiter les données sur la qualité de l'eau, les loyers (plusieurs fichiers),
        les données géographiques, et la population.

        :param water_files: Liste des chemins vers les fichiers de données sur l'eau.
        :param geojson_path: Chemin vers le fichier GeoJSON des communes.
        :param rent_files: Liste des chemins vers les fichiers de loyers (4 catégories).
        :param pop_file: Chemin vers le fichier XLS contenant les données de population.
        """
        self.water_files = water_files
        self.geojson_path = geojson_path
        self.rent_files = rent_files
        self.pop_file = pop_file

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
        # Standardiser les codes INSEE
        self.water_data["inseecommune"] = (
            self.water_data["inseecommune"].astype(str).str.zfill(5)
        )
        # Convertir les colonnes de conformité en valeurs binaires
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
        """
        Charger et concaténer les données de loyers depuis plusieurs fichiers,
        et stocker la moyenne des 4 catégories dans `self.rent_data`.
        """
        all_rent = []
        for rent_file in self.rent_files:
            df = pd.read_csv(rent_file, delimiter=";", decimal=",", encoding="ISO-8859-1")
            all_rent.append(df)

        # Concaténer toutes les données
        concatenated = pd.concat(all_rent, ignore_index=True)

        # Agréger la moyenne des loyers par INSEE_C
        self.rent_data = concatenated.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        self.rent_data.rename(columns={"loypredm2": "mean_loypredm2"}, inplace=True)

    def load_population_data(self):
        """Charger les données de population depuis un fichier XLS."""
        if not self.pop_file:
            print("Aucun fichier population spécifié.")
            return

        self.pop_data = pd.read_excel(self.pop_file)
        self.pop_data["codgeo"] = self.pop_data["codgeo"].astype(str).str.zfill(5)
        print("Population data loaded and cleaned.")

    def merge_data(self):
        """
        Fusionner l'ensemble : eau, loyers, géographie, et population (si dispo).
        """
        # ----- Eau -----
        water_agg = (
            self.water_data
            .groupby("inseecommune")
            .agg({
                "bacterio_conformity": "mean",
                "chemical_conformity": "mean"
            })
            .reset_index()
        )

        self.geo_data = self.geo_data.merge(
            water_agg,
            left_on="codgeo",
            right_on="inseecommune",
            how="left"
        )

        # ----- Loyers -----
        # self.rent_data contient déjà la moyenne (mean_loypredm2)
        self.geo_data = self.geo_data.merge(
            self.rent_data,
            left_on="codgeo",
            right_on="INSEE_C",
            how="left"
        )

        # ----- Population -----
        if self.pop_data is not None:
            # Par exemple, on utilise ici la colonne 'p21_pop'
            self.geo_data = self.geo_data.merge(
                self.pop_data[["codgeo", "p21_pop"]],
                on="codgeo",
                how="left"
            )

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
        """
        Analyser les corrélations entre la qualité de l'eau, la moyenne des loyers,
        la population (si disponible), etc.
        """
        columns_of_interest = [
            "bacterio_conformity",
            "chemical_conformity",
            "mean_loypredm2",
        ]

        if self.pop_data is not None and "p21_pop" in self.geo_data.columns:
            columns_of_interest.append("p21_pop")

        analysis_data = self.geo_data[columns_of_interest].dropna()
        correlation_matrix = analysis_data.corr()
        print("Matrice de corrélation :")
        print(correlation_matrix)

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

    # Liste des 4 fichiers de loyers, un par catégorie (exemple)
    rent_files = [
    "data/pred-app-mef-dhup.csv",
    "data/pred-app3-mef-dhup.csv",
    "data/pred-app12-mef-dhup.csv",
    "data/pred-mai-mef-dhup.csv",
    ]

    pop_file = "data\POPULATION_MUNICIPALE_COMMUNES_FRANCE.xlsx" 

    processor = DataProcessor(
        water_files=water_files,
        geojson_path=geojson_path,
        rent_files=rent_files,
        pop_file=pop_file
    )

    # 1. Charger et nettoyer les données sur l'eau
    processor.load_water_data()
    processor.clean_water_data()

    # 2. Charger les données géographiques
    processor.load_geo_data()

    # 3. Charger la population
    processor.load_population_data()

    # 4. Charger les données de loyers (4 fichiers) et prendre la moyenne
    processor.load_rent_data()

    # 5. Fusionner le tout
    processor.merge_data()

    # 6. Tracer un exemple
    processor.plot_water_quality("bacterio_conformity", "Conformité Bactériologique de l'Eau")

    # 7. Analyser la corrélation
    processor.analyze_correlation()
