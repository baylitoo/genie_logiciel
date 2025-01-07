import pandas as pd
import chardet
import geopandas as gpd
import matplotlib.pyplot as plt
import folium
from typing import List

class RentDataProcessor:
    def __init__(self, geojson_path: str, rent_files: List[str]):
        self.geojson_path = geojson_path
        self.rent_files = rent_files
        self.geo_data = None
        self.rent_data = None
        self.merged_data = None

    def detect_encoding(self, filename: str, sample_size: int = 10000) -> str:
        """Detect the file encoding."""
        with open(filename, "rb") as f:
            raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        return result["encoding"]

    def load_rent_data(self) -> pd.DataFrame:
        """Load and merge rent data from all files."""
        all_data = []
        for file in self.rent_files:
            encoding = self.detect_encoding(file)
            data = pd.read_csv(
                file,
                sep=";",
                decimal=",",
                encoding=encoding
            )
            all_data.append(data)
        combined_data = pd.concat(all_data, ignore_index=True)
        return combined_data

    def load_geo_data(self) -> gpd.GeoDataFrame:
        """Load geographic data from a GeoJSON file."""
        geo_data = gpd.read_file(self.geojson_path)
        geo_data["codgeo"] = geo_data["codgeo"].astype(str).str.zfill(5)
        return geo_data

    def preprocess_data(self):
        """Load and preprocess the rent and geographic data."""
        self.geo_data = self.load_geo_data()
        self.rent_data = self.load_rent_data()

        # Standardize and clean rent data
        self.rent_data["INSEE_C"] = self.rent_data["INSEE_C"].astype(str).str.zfill(5)
        
        # Merge and calculate average rent per commune
        rent_avg = self.rent_data.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        
        # Merge with geographic data
        self.merged_data = self.geo_data.merge(
            rent_avg,
            left_on="codgeo",
            right_on="INSEE_C",
            how="left"
        )

    def visualize_static_map(self, output_path: str):
        """Generate a static map of rent data."""
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        ax.set_facecolor("#001f3f")
        self.merged_data.plot(
            column="loypredm2",
            cmap="YlOrRd",
            legend=True,
            missing_kwds={
                "color": "blue",
                "label": "Données manquantes"
            },
            ax=ax
        )
        plt.title("Carte des loyers moyens (€ par m²) par commune en France")
        plt.axis("off")
        plt.savefig(output_path)
        plt.show()

    def visualize_interactive_map(self, output_path: str):
        """Generate an interactive map of rent data."""
        m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)
        folium.Choropleth(
            geo_data=self.merged_data,
            data=self.merged_data,
            columns=["codgeo", "loypredm2"],
            key_on="feature.properties.codgeo",
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name="Loyer moyen (€ par m²)"
        ).add_to(m)
        m.save(output_path)

# Example Usage
geojson_path = "data/a-com2022.json"
rent_files = [
    "data/pred-app-mef-dhup.csv",
    "data/pred-app3-mef-dhup.csv",
    "data/pred-app12-mef-dhup.csv",
    "data/pred-mai-mef-dhup.csv",
]

processor = RentDataProcessor(geojson_path, rent_files)
processor.preprocess_data()
processor.visualize_static_map("static_rent_map.png")
processor.visualize_interactive_map("interactive_rent_map.html")
