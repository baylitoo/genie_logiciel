import pandas as pd
import geopandas as gpd
from sqlalchemy import create_engine, text
import chardet
import folium
import matplotlib.pyplot as plt

# crashes when i run on my laptop, i think it's because of the memory, connexion to the database is established but the data is not loaded.. debugged 
class MySQLWaterRentProcessor:
    def __init__(self, host, user, password, database):
        self.database = database
        self.engine = create_engine(f"mysql+mysqlconnector://{user}:{password}@{host}/{database}")

    def create_schema(self):
        schema_sql = f"""
        CREATE DATABASE IF NOT EXISTS {self.database};
        USE {self.database};

        CREATE TABLE IF NOT EXISTS commune (
            codgeo CHAR(5) NOT NULL,
            PRIMARY KEY (codgeo)
        ) ENGINE=InnoDB;

        CREATE TABLE IF NOT EXISTS water_data (
            id                       INT AUTO_INCREMENT,
            inseecommune             CHAR(5)      NOT NULL,
            plvconformitebacterio    VARCHAR(10)  NULL,
            plvconformitechimique    VARCHAR(10)  NULL,
            bacterio_conformity      TINYINT      NULL,
            chemical_conformity      TINYINT      NULL,
            PRIMARY KEY (id),
            CONSTRAINT fk_water_commune 
              FOREIGN KEY (inseecommune) 
              REFERENCES commune (codgeo)
              ON DELETE CASCADE
              ON UPDATE CASCADE
        ) ENGINE=InnoDB;

        CREATE TABLE IF NOT EXISTS rent_data (
            id               INT AUTO_INCREMENT,
            insee_c          CHAR(5)       NOT NULL,
            loypredm2        DECIMAL(10,2) NULL,
            mean_loypredm2   DECIMAL(10,2) NULL,
            PRIMARY KEY (id),
            CONSTRAINT fk_rent_commune 
              FOREIGN KEY (insee_c) 
              REFERENCES commune (codgeo)
              ON DELETE CASCADE
              ON UPDATE CASCADE
        ) ENGINE=InnoDB;

        CREATE TABLE IF NOT EXISTS merged_data (
            id                  INT AUTO_INCREMENT,
            insee_commune       CHAR(5)       NOT NULL,
            bacterio_conformity DECIMAL(4,3)  NULL,
            chemical_conformity DECIMAL(4,3)  NULL,
            mean_loypredm2      DECIMAL(10,2) NULL,
            PRIMARY KEY (id),
            CONSTRAINT fk_merged_commune 
              FOREIGN KEY (insee_commune)
              REFERENCES commune (codgeo)
              ON DELETE CASCADE
              ON UPDATE CASCADE
        ) ENGINE=InnoDB;
        """

        with self.engine.connect() as conn:
            for statement in schema_sql.strip().split(';'):
                st = statement.strip()
                if st:
                    conn.execute(text(st))
        print("Database schema created or verified successfully.")

    def detect_encoding(self, filename: str, sample_size: int = 10000) -> str:
        """Detect file encoding via chardet."""
        with open(filename, "rb") as f:
            raw_data = f.read(sample_size)
        result = chardet.detect(raw_data)
        return result["encoding"] if result["encoding"] else "utf-8"

    def populate_commune_stub(self, codgeos):
        """
        Insert minimal rows in 'commune' so that we can refer to them in water_data/rent_data.
        codgeos is a list of code communes you anticipate having in your CSVs.
        """
        df = pd.DataFrame({"codgeo": list(codgeos)})
        df.to_sql("commune", con=self.engine, if_exists="append", index=False)
        print(f"Inserted {len(df)} rows into 'commune' table.")

    def load_rent_data(self, rent_files: list):
        """Load rent data from multiple files -> store in 'rent_data' table."""
        all_data = []
        for file in rent_files:
            encoding = self.detect_encoding(file)
            data = pd.read_csv(file, sep=";", decimal=",", encoding=encoding, low_memory=False)
            all_data.append(data)
        combined_data = pd.concat(all_data, ignore_index=True)

        # Zero-pad the commune code
        combined_data["INSEE_C"] = combined_data["INSEE_C"].astype(str).str.zfill(5)

        # Compute mean if needed
        rent_avg = combined_data.groupby("INSEE_C")["loypredm2"].mean().reset_index()
        rent_avg.rename(columns={"INSEE_C": "insee_c", "loypredm2": "mean_loypredm2"}, inplace=True)

        # Insert into 'rent_data'
        rent_avg.to_sql("rent_data", con=self.engine, if_exists="replace", index=False)
        print("Rent data saved to 'rent_data' table.")

    def load_water_data(self, water_files: list):
        """Load water data from multiple files (PLV, RES, etc.) into 'water_data' table."""
        all_data = []
        for file in water_files:
            df = pd.read_csv(file, delimiter=",", encoding="ISO-8859-1", low_memory=False)
            all_data.append(df)
        water_df = pd.concat(all_data, ignore_index=True)

        water_df["inseecommune"] = water_df["inseecommune"].astype(str).str.zfill(5)
        water_df["bacterio_conformity"] = water_df["plvconformitebacterio"].apply(lambda x: 1 if x == "C" else 0)
        water_df["chemical_conformity"] = water_df["plvconformitechimique"].apply(lambda x: 1 if x == "C" else 0)

        water_df.to_sql("water_data", con=self.engine, if_exists="replace", index=False)
        print("Water data saved to 'water_data' table.")

    def preprocess_and_merge_data(self):
        """
        Aggregate water_data -> insert into merged_data, then join rent_data for the rent info.
        """
        with self.engine.begin() as conn:
            conn.execute(text("DELETE FROM merged_data;"))

            # Insert aggregated water data
            conn.execute(text("""
                INSERT INTO merged_data (insee_commune, bacterio_conformity, chemical_conformity)
                SELECT
                    inseecommune,
                    AVG(bacterio_conformity) AS avg_bact,
                    AVG(chemical_conformity) AS avg_chem
                FROM water_data
                GROUP BY inseecommune
            """))

            # Update rent info
            conn.execute(text("""
                UPDATE merged_data m
                JOIN rent_data r ON m.insee_commune = r.insee_c
                SET m.mean_loypredm2 = r.mean_loypredm2
            """))

        print("Merged data has been populated in 'merged_data' table.")


class GeoDataVisualizer:
    """
    We do NOT store big GeoJSONs in MySQL. We just read them locally and do the mapping. as they're needed only for visualization.
    """
    def __init__(self, geojson_path: str):
        self.geo_data = gpd.read_file(geojson_path)

    def visualize_static_map(self, column: str, title: str, output_path: str):
        fig, ax = plt.subplots(1, 1, figsize=(12, 10))
        self.geo_data.plot(
            column=column,
            cmap="YlOrRd",
            legend=True,
            missing_kwds={"color": "blue", "label": "Données manquantes"},
            ax=ax
        )
        plt.title(title)
        plt.axis("off")
        plt.savefig(output_path)
        plt.show()

    def visualize_interactive_map(self, column: str, output_path: str):
        m = folium.Map(location=[46.603354, 1.888334], zoom_start=6)
        folium.Choropleth(
            geo_data=self.geo_data,
            data=self.geo_data,  
            columns=["codgeo", column],
            key_on="feature.properties.codgeo",
            fill_color="YlOrRd",
            fill_opacity=0.7,
            line_opacity=0.2,
            legend_name=f"{column}"
        ).add_to(m)
        m.save(output_path)
        print(f"Interactive map saved to {output_path}")


# ----------------------------------------------------------------------
# Main script
# ----------------------------------------------------------------------
if __name__ == "__main__":
    # MySQL connection
    host = "localhost"
    user = "root"# username here
    password = "root" # put your password here
    database = "water_rent_quality"

    # Data files
    rent_files = [
        "data/pred-app-mef-dhup.csv",
        "data/pred-app3-mef-dhup.csv",
        "data/pred-app12-mef-dhup.csv",
        "data/pred-mai-mef-dhup.csv",
    ]
    water_files = [
        "data/CAP_PLV_202411.txt",
        "data/CAP_RES_202411.txt",
        "data/TTP_PLV_202411.txt",
        "data/TTP_RES_202411.txt",
        "data/UDI_PLV_202411.txt",
        "data/UDI_RES_202411.txt",
    ]
    geojson_path = "data/a-com2022.json"   

    processor = MySQLWaterRentProcessor(host, user, password, database)
    # If you ran the code in mysql, comment this line
    processor.create_schema()
    

    all_water = pd.concat([pd.read_csv(f, delimiter=",", encoding="ISO-8859-1") for f in water_files])
    all_water["inseecommune"] = all_water["inseecommune"].astype(str).str.zfill(5)
    codgeo_water = set(all_water["inseecommune"].unique())

    all_rent = pd.concat([
        pd.read_csv(f, sep=";", decimal=",", encoding=processor.detect_encoding(f)) 
        for f in rent_files
    ])
    all_rent["INSEE_C"] = all_rent["INSEE_C"].astype(str).str.zfill(5)
    codgeo_rent = set(all_rent["INSEE_C"].unique())

    all_codgeos = codgeo_water.union(codgeo_rent)
    # Insert them into 'commune'
    processor.populate_commune_stub(all_codgeos)

    # Now load data normally
    processor.load_rent_data(rent_files)
    processor.load_water_data(water_files)

    # Merge
    processor.preprocess_and_merge_data()

    # Visualization from local geojson
    visualizer = GeoDataVisualizer(geojson_path)
    visualizer.visualize_static_map(
        column="codgeo",
        title="Communes by codgeo",
        output_path="static_map.png"
    )
    visualizer.visualize_interactive_map(
        column="codgeo",
        output_path="interactive_map.html"
    )
