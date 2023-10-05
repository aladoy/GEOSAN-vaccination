# LIBRARIES
import pandas as pd
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps
import geopandas as gpd
import numpy as np

# CONNECT TO DB
pw = getpass.getpass()  # Ask for user password
engine = create_engine(
    "postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)
)  # Create SQLAlchemy engine
conn = ps.connect(
    "dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)
)  # Create a connection object
cursor = conn.cursor()  # Create a cursor object

# Open file with possible sites
sites = gpd.read_file(
    "data/CENTRES VACCINATION PC (CONFIDENTIAL)/sites_possibles.geojson"
)
sites.head()

# Use file "Planification_Communes_–_phase_1.pdf" and extract corresponding municipalities from GEOSAN DB
sql = (
    "SELECT DISTINCT num, name  FROM municipalities WHERE name "
    "IN ('Vallorbe', 'Cheseaux-sur-Lausanne', 'Yvonand', 'Saint-Légier-La Chiésaz', 'Aubonne', 'Grandson','Valbroye','Leysin','Coppet','Moudon','Penthalaz','Chardonne',"
    "'La Sarraz','Préverenges','Orbe','Vevey','Renens (VD)','Savigny','Avenches','Ormont-Dessus','Vully-les-Lacs','Ollon','Saint-Cergue',"
    "'Oron','Echallens','Aigle','Cossonay','Chavornay','Lucens','Bex','Bière')"
)
mun = pd.read_sql(sql, conn)

# Merge with sites_possibles
sites_mod = pd.merge(sites, mun, on="num", how="left")

print("Are all municipalities included?")
sites_mod[~sites_mod.name.isna()].shape[0] == mun.shape[0]

# Add Cheseaux-sur-Lausanne that was not on the list of possible sites but was finally included

# Extract info for Cheseaux
sql_cheseaux = (
    "SELECT num, name, geometry FROM municipalities WHERE name='Cheseaux-sur-Lausanne'"
)
cheseaux = gpd.read_postgis(sql_cheseaux, conn, geom_col="geometry")

# Extract info for Yvonand
sql_yvonand = "SELECT num, name, geometry FROM municipalities WHERE name='Yvonand'"
yvonand = gpd.read_postgis(sql_yvonand, conn, geom_col="geometry")


# Add two lines to the dataframe
sites_mod = sites_mod.append(
    {
        "num": cheseaux.num[0],
        "sites_possibles": 1,
        "geometry": cheseaux.geometry[0],
        "name": cheseaux.name[0],
    },
    ignore_index=True,
)
sites_mod = sites_mod.append(
    {
        "num": yvonand.num[0],
        "sites_possibles": 1,
        "geometry": yvonand.geometry[0],
        "name": yvonand.name[0],
    },
    ignore_index=True,
)

# Remove an entity from Valbroye which appears twice
sites_mod.drop(index=[14, 42], inplace=True)

# Compute centroid within each polygon (ensure that centroids are within)
sites_mod.geometry = sites_mod.representative_point()

# Add a column for visited
sites_mod["visited"] = np.where(sites_mod["name"].isna(), "no", "yes")

# Export to file
sites_mod.to_file("results/phase 1/sites_visited.geojson", driver="GeoJSON")
