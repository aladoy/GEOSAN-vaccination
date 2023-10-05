# LIBRAIRIES
# Basic
import pandas as pd
import statistics as s
import os

# Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps

# Spatial
import geopandas as gpd


# CONNECT TO DB
pw = getpass.getpass()  # Ask for user password
engine = create_engine(
    "postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)
)  # Create SQLAlchemy engine
conn = ps.connect(
    "dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)
)  # Create a connection object
cursor = conn.cursor()  # Create a cursor object


# LOAD GEOCODED VACCINATION DATA FROM GEOSAN DB
sql = "SELECT a.reli, a.geometry, a.nb_vacc_tot, b.ptot FROM \
(SELECT h.reli, h.geometry, SUM(v.nb_vacc) as nb_vacc_tot \
FROM inhabited_ha h LEFT JOIN (SELECT * FROM vaccination.vaccination_address WHERE note_geocoding != 'Geocoded at NPA centroid.') v ON ST_contains(h.geometry,v.geometry) \
GROUP BY h.reli, h.geometry) a \
INNER JOIN mgis_ha_2021 b ON a.reli=b.reli"
data = gpd.read_postgis(sql, conn, geom_col="geometry")

# DATA WRANGLING
# Fill with 0 for nb_vacc_tot=0 and perc_vacc=0
data["nb_vacc_tot"] = data.nb_vacc_tot.fillna(0)
# Compute percentage of vaccination
data["perc_vacc"] = data["nb_vacc_tot"] / data["ptot"]
# Correct percentage > 1
data.loc[data.perc_vacc > 1, "perc_vacc"] = 1

# COMPUTE RETARD DE VACCINATION
# Mean of percentage vaccinated
s.mean(data.perc_vacc)
data["pop_to_vaccinate"] = 0.4 * data.ptot - data.nb_vacc_tot
# Replace by 0 for negative values
data.loc[data.pop_to_vaccinate < 0, "pop_to_vaccinate"] = 0
data

# ADD OTHER COVARIATES
sql_mgis = "SELECT m.reli , iqmd, \
CAST(pnsubaf AS DOUBLE PRECISION)/CAST(ptot AS DOUBLE PRECISION) as rnsubaf, \
CAST(ploctur+plocscr+plocalb AS DOUBLE PRECISION)/CAST(ptot AS DOUBLE PRECISION) as rlocbalkans, \
1- (CAST(plocfra+ploceng+plocger AS DOUBLE PRECISION)/CAST(ptot AS DOUBLE PRECISION)) as nn_rlocfrukde \
FROM mgis_ha_2021 m INNER JOIN inhabited_ha i ON m.reli=i.reli"
sql_mgis = pd.read_sql(sql_mgis, conn)

# CORRECT RATIOS
sql_mgis.loc[sql_mgis.nn_rlocfrukde < 0, "nn_rlocfrukde"] = 0
sql_mgis.loc[sql_mgis.nn_rlocfrukde > 1, "nn_rlocfrukde"] = 1
sql_mgis.loc[sql_mgis.rlocbalkans > 1, "rlocbalkans"] = 1
sql_mgis.loc[sql_mgis.rnsubaf > 1, "rnsubaf"] = 1

# Join with RELIs
data = data.merge(sql_mgis, how="left", on="reli")
# Fill na with zero
data.loc[:, ["ptot", "iqmd", "rnsubaf", "rlocbalkans", "nn_rlocfrukde"]] = data.loc[
    :, ["ptot", "iqmd", "rnsubaf", "rlocbalkans", "nn_rlocfrukde"]
].fillna(value=0)

# EXPORT TO GEOPACKAGE FILE
path = "Vaccination mobile @ DGS/results/phase 2/vaccination_data_indicators_reli.gpkg"
try:
    if os.path.exists(path):
        os.remove(path)
    data.to_file(path, driver="GPKG")
    print("Sucess")
except:
    print("Error while saving data on disk")
