# VACCINATION_2-indicatorsRELI.py

# LIBRARIES
# Basic
import pandas as pd
import sys
import numpy as np
import os

# Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps

# Spatial
import geopandas as gpd

sys.path.append(r"./FUNCTIONS/")
import shortestPaths as sp

# CONNECT TO DB
pw = getpass.getpass()  # Ask for user password
engine = create_engine(
    "postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)
)  # Create SQLAlchemy engine
conn = ps.connect(
    "dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)
)  # Create a connection object
cursor = conn.cursor()  # Create a cursor object

# EXTRACT RELI FROM DATABASE
reli = gpd.read_postgis(
    "SELECT reli, geometry FROM inhabited_ha", conn, geom_col="geometry", crs=2056
)

# TOTAL POPULATION, PROPORTION OF +65Y/O, PROPORTION OF NON-FRENCH SPEAKERS, AND MEDIAN INCOME BY RELI
sql_mgis = "SELECT m.reli, ptot, CAST(p65m AS DOUBLE PRECISION)/CAST(ptot AS DOUBLE PRECISION) as rp65m, iqmd, \
1-(CAST(plocfra AS DOUBLE PRECISION)/CAST(ptot AS DOUBLE PRECISION)) as nn_rplocfra \
FROM mgis_ha_2021 m INNER JOIN inhabited_ha i ON m.reli=i.reli"
sql_mgis = pd.read_sql(sql_mgis, conn)
# Join with RELIs
reli = reli.merge(sql_mgis, how="left", on="reli")
# Fill na with zero
reli.loc[:, ["ptot", "rp65m", "iqmd", "nn_rplocfra"]] = reli.loc[
    :, ["ptot", "rp65m", "iqmd", "nn_rplocfra"]
].fillna(value=0)

# PROPORTION OF OLD PEOPLE (+65YRS) LIVING ALONE (OPLA) BY RELI
sql_opla = "SELECT p.reli, (CAST(o.nb_opla AS DOUBLE PRECISION) / CAST(p.nb_indiv AS DOUBLE PRECISION))  as opla FROM \
(SELECT r.reli, COUNT(s.index) as nb_opla FROM inhabited_ha r \
INNER JOIN (SELECT index, geometry FROM statpop_indiv WHERE countagrofpersonpermphh=1.0 \
AND (statpop_indiv.countofmaleover65perm=1 OR countofmaleover80perm=1 OR countoffemaleover65perm=1 OR countoffemaleover80perm=1)) s \
ON ST_Contains(r.geometry,s.geometry) GROUP BY reli) o \
RIGHT JOIN \
(SELECT r2.reli, COUNT(s2.index) as nb_indiv \
FROM inhabited_ha r2 INNER JOIN statpop_indiv s2 \
ON ST_Contains(r2.geometry,s2.geometry) GROUP BY reli) p \
ON o.reli=p.reli"
sql_opla = pd.read_sql(sql_opla, conn)
# Join with RELIs
reli = reli.merge(sql_opla, how="left", on="reli")
# Fill na with zero
reli["opla"] = reli.opla.fillna(value=0)

# DISTANCE TO NEAREST VACCINATION CENTER FROM HA CENTROID
# Extract RELI centroids from GEOSAN DB
reli_centroids = gpd.read_postgis(
    "SELECT reli, st_x(geometry) as x, st_y(geometry) as y, st_centroid(geometry) as geometry FROM inhabitinhabited_ha_centroided_ha_centroid",
    conn,
    geom_col="geometry",
)
# Import Vaccination Centers
centers = gpd.read_file(
    "Vaccination mobile @ DGS/data/CENTRES VACCINATION PC (CONFIDENTIAL)/210318_Vaccination_Centers.shp"
)
# Extract projected graph for road networks in VD (drive)
graph = sp.import_graph("Vaud,Switzerland", "drive", 2056)
# Add id and distance of nearest vaccination centers for each NPA centroid
reli_centroids_new = sp.compute_nearest_target_distance(reli_centroids, centers, graph)
# Rename columns
reli_centroids_new.rename(
    columns={
        "nearest_targ_id": "nrst_vacc_ctr_id",
        "nearest_targ_dist": "nrst_vacc_ctr_dist",
    },
    inplace=True,
)
reli_centroids.loc[
    reli_centroids.nrst_vacc_ctr_dist == "No reachable",
    ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"],
] = np.nan
reli_centroids_new["nrst_vacc_ctr_dist"] = reli_centroids_new[
    "nrst_vacc_ctr_dist"
].astype(
    float
)  # Convert to float
# Merge with RELIs
reli = reli.merge(
    reli_centroids_new[["reli", "nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]],
    how="left",
    on="reli",
)

# Deal with non reachable destinations
# First round
nn_reachable = tuple(reli[reli.nrst_vacc_ctr_dist.isna()].reli)
sql_nn_reachable = f"""
    SELECT reli, st_x(geometry) as x, st_y(geometry)-50 as y, st_centroid(geometry) as geometry
    FROM inhabited_ha_centroid
    WHERE reli IN {nn_reachable}
"""
sql_nn_reachable = gpd.read_postgis(sql_nn_reachable, conn, geom_col="geometry")
new_reachable = sp.compute_nearest_target_distance(sql_nn_reachable, centers, graph)
new_reachable = new_reachable[new_reachable.nearest_targ_dist != "No reachable"]
reli = pd.merge(
    reli,
    new_reachable[["reli", "nearest_targ_id", "nearest_targ_dist"]],
    how="left",
    on="reli",
)
reli["nrst_vacc_ctr_id"].fillna(reli["nearest_targ_id"], inplace=True)
reli["nrst_vacc_ctr_dist"].fillna(reli["nearest_targ_dist"], inplace=True)
reli.drop(["nearest_targ_id", "nearest_targ_dist"], axis=1, inplace=True)
# Second round
nn_reachable = tuple(reli[reli.nrst_vacc_ctr_dist.isna()].reli)
sql_nn_reachable = f"""
    SELECT reli, st_x(geometry)+50 as x, st_y(geometry)+50 as y, st_centroid(geometry) as geometry
    FROM inhabited_ha_centroid
    WHERE reli IN {nn_reachable}
"""
sql_nn_reachable = gpd.read_postgis(sql_nn_reachable, conn, geom_col="geometry")
new_reachable = sp.compute_nearest_target_distance(sql_nn_reachable, centers, graph)
new_reachable = new_reachable[new_reachable.nearest_targ_dist != "No reachable"]
reli = pd.merge(
    reli,
    new_reachable[["reli", "nearest_targ_id", "nearest_targ_dist"]],
    how="left",
    on="reli",
)
reli["nrst_vacc_ctr_id"].fillna(reli["nearest_targ_id"], inplace=True)
reli["nrst_vacc_ctr_dist"].fillna(reli["nearest_targ_dist"], inplace=True)
reli.drop(["nearest_targ_id", "nearest_targ_dist"], axis=1, inplace=True)
# Third round (manual fill)
nn_reachable = tuple(reli[reli.nrst_vacc_ctr_dist.isna()].reli)
sql_nn_reachable = f"""
    SELECT reli, st_x(geometry)-75 as x, st_y(geometry)-75 as y, st_centroid(geometry) as geometry
    FROM inhabited_ha_centroid
    WHERE reli IN {nn_reachable}
"""
sql_nn_reachable = gpd.read_postgis(sql_nn_reachable, conn, geom_col="geometry")
new_reachable = sp.compute_nearest_target_distance(sql_nn_reachable, centers, graph)
reli.loc[reli.reli == 50781374, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    11.0,
    1763.721,
]  # changing polygon corner(sql)
reli.loc[reli.reli == 54381506, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    13.0,
    6767.511,
]  # changing polygon corner(sql)
reli.loc[reli.reli == 51231442, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    4.0,
    4100.00,
]  # Google maps
reli.loc[reli.reli == 52951622, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    9.0,
    11270.594,
]  # Same as neighbor
reli.loc[reli.reli == 52961621, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    9.0,
    11270.594,
]  # Same as neighbor
reli.loc[reli.reli == 52961622, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    9.0,
    11270.594,
]  # Same as neighbor
reli.loc[reli.reli == 52971622, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    9.0,
    11270.594,
]  # Same as neighbor
reli.loc[reli.reli == 56651200, ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]] = [
    6.0,
    19800.00,
]  # Google maps


# NUMBER OF PUBLIC TRANSPORT STOPS AROUND HA CENTROID (400m buffer)
sql_ptstops = "SELECT s.reli, COUNT(pt.xtf_id) as nb_pt_stops \
FROM (SELECT reli, st_buffer(geometry, 400) as geometry FROM inhabited_ha_centroid) s \
LEFT JOIN public_transport_stops pt ON ST_contains(s.geometry, pt.geometry) \
GROUP BY s.reli"
sql_ptstops = pd.read_sql(sql_ptstops, conn)
# Join with RELIs
reli = reli.merge(sql_ptstops, how="left", on="reli")


# SAVE INDICATORS
output_path = "Vaccination mobile @ DGS/results/phase 2/"
if os.path.exists(output_path + "accessibility_indicators_reli.gpkg"):
    print("File already exists. Deleted.")
    os.remove(output_path + "accessibility_indicators_reli.gpkg")
reli.to_file(output_path + "accessibility_indicators_reli.gpkg", driver="GPKG")

# CLOSE CONNECTION WITH DB
conn.close()
