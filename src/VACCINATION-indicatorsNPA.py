# LIBRARIES
# Basic
import pandas as pd
import os
import subprocess
import sys
import numpy as np

# Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps

# Spatial
import geopandas as gpd

# User defined function
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

# EXTRACT NPA INFO FROM DATABASE
npa = pd.read_sql("SELECT lcid, zipcode, locality, ptot FROM npa", conn)

# PROPORTION OF SOCIOPROFESSIONAL CATEGORIES (CSP), AND MEDIAN INCOME BY NPA
sql_mg = "SELECT lcid, ciqmd, rp65m/100 as rp65m, rpcsp1/100 as rpcsp1, rpcsp2/100 as rpcsp2, rpcsp3/100 as rpcsp3 FROM npa"
sql_mg = pd.read_sql(sql_mg, conn)
# Join with NPAs
npa = npa.merge(sql_mg, how="left", on="lcid")

# PROPORTION OF SWISS, FOREIGNERS AND NATIONALITY CATEGORY BY NPA
sql_sp = "SELECT n.lcid, SUM(s.b19b21)/SUM(s.b19btot) as swiss, SUM(s.b19b26)/SUM(s.b19btot) as foreigners, \
SUM(s.b19b27)/SUM(s.b19btot) as ue, SUM(s.b19b28)/SUM(s.b19btot) as europe_nn_eu, \
SUM(s.b19b29)/SUM(s.b19btot) as non_europe \
FROM npa n INNER JOIN inhabited_ha_centroid s ON ST_Contains(n.geometry,s.geometry) \
GROUP BY lcid"
sql_sp = pd.read_sql(sql_sp, conn)
# Join with NPAs
npa = npa.merge(sql_sp, how="left", on="lcid")

# PROPORTION OF OLD PEOPLE (+65YRS) LIVING ALONE (OPLA) BY NPA
sql_opla = "SELECT p.lcid, (CAST(o.nb_opla AS DOUBLE PRECISION) / CAST(p.nb_indiv AS DOUBLE PRECISION))  as opla FROM \
(SELECT n.lcid, COUNT(s.index) as nb_opla FROM npa n \
INNER JOIN (SELECT index, geometry FROM statpop_indiv WHERE countagrofpersonpermphh=1.0 \
AND (statpop_indiv.countofmaleover65perm=1 OR countofmaleover80perm=1 OR countoffemaleover65perm=1 OR countoffemaleover80perm=1)) s \
ON ST_Contains(n.geometry,s.geometry) GROUP BY lcid) o \
INNER JOIN \
(SELECT n.lcid, COUNT(s.index) as nb_indiv \
FROM npa n INNER JOIN (SELECT index, geometry FROM statpop_indiv) s \
ON ST_Contains(n.geometry,s.geometry) GROUP BY lcid) p \
ON o.lcid=p.lcid"
sql_opla = pd.read_sql(sql_opla, conn)
# Join with NPAs
npa = npa.merge(sql_opla, how="left", on="lcid")
# Fill na with zero
npa["opla"] = npa.opla.fillna(value=0)

# NUMBER OF PUBLIC TRANSPORTS PER INHABITED HA
sql_pt = "SELECT b.lcid, CAST(a.nb_pt_stops AS DOUBLE PRECISION)/CAST(b.nb_reli AS DOUBLE PRECISION) as pt_stops FROM \
(SELECT n.lcid, COUNT(p.numero) as nb_pt_stops  FROM npa n INNER JOIN public_transport_stops p ON ST_Contains(n.geometry,p.geometry) GROUP BY n.lcid) a \
INNER JOIN \
(SELECT n.lcid, COUNT(s.reli) as nb_reli FROM npa n INNER JOIN inhabited_ha_centroid s ON ST_Contains(n.geometry, s.geometry) GROUP BY lcid) b \
ON a.lcid=b.lcid"
sql_pt = pd.read_sql(sql_pt, conn)
# Join with NPAs
npa = npa.merge(sql_pt, how="left", on="lcid")
# Fill na with zero
npa["pt_stops"] = npa.pt_stops.fillna(value=0)

# NUMBER OF DOCTOR'S OFFICES (CABINETS MÉDICAUX) DOING VACCINATION PER 1000 HABS
# Read excel file
doctoff = pd.read_excel(
    "Vaccination mobile @ DGS/data/CABINETS MEDICAUX VACCINATION DGS (CONFIDENTIAL)/200319_VAC_Cabinets-medicaux.xlsx"
)
# Compute number of doctor's office per zipcode
doctoff = (
    doctoff.groupby("Code postal")["Numéro suivi interne"].agg("count").reset_index()
)
doctoff.columns = ["zipcode", "nb_offices"]
# Join with NPAs to compute doctor's offices density
doctoff = (
    npa.groupby("zipcode")["ptot"]
    .agg("sum")
    .reset_index()
    .merge(doctoff, how="left", on="zipcode")
)
doctoff["dr_offices"] = doctoff.nb_offices * 1000 / doctoff.ptot
# Fill NAs with zero value
doctoff.fillna(0, inplace=True)
# Merge with LCID (upper level)
npa = npa.merge(doctoff[["zipcode", "dr_offices"]], how="left", on="zipcode")

# NUMBER OF VACCINATION CENTERS PER 1000 HABS
# Read excel file
centers = pd.read_excel(
    "Vaccination mobile @ DGS/data/CENTRES VACCINATION PC (CONFIDENTIAL)/210318_06_03_LI_Adresses-Centres-Vaccination-v2.xlsx"
)
# Compute number of centers per zipcode
centers = centers.groupby("npa")["id"].agg("count").reset_index()
# Join with NPAs to compute centers density
centers = (
    npa.groupby("zipcode")["ptot"]
    .agg("sum")
    .reset_index()
    .merge(centers, how="left", left_on="zipcode", right_on="npa")
)
centers["vacc_centers"] = centers.id * 1000 / centers.ptot
# Fill NAs with zero value
centers.fillna(0, inplace=True)
# Merge with LCID (upper level)
npa = npa.merge(centers[["zipcode", "vacc_centers"]], how="left", on="zipcode")

# NUMBER OF PHARMACIES DOING VACCINATION PER 1000 HABS
# Read excel file
pharma = pd.read_excel(
    "Vaccination mobile @ DGS/data/PHARMACIES VACCINATION DGS (CONFIDENTIAL)/210223_Tableau_pharmacies_auth_vaccination.xlsx",
    usecols="A:B",
)
# Rename columns
pharma.columns = ["zipcode", "city"]
# Compute number of centers per zipcode
pharma = pharma.groupby("zipcode")["city"].agg("count").reset_index()
# Join with NPAs to compute centers density
pharma = (
    npa.groupby("zipcode")["ptot"]
    .agg("sum")
    .reset_index()
    .merge(pharma, how="left", on="zipcode")
)
pharma["pharmacies"] = pharma.city * 1000 / pharma.ptot
# Fill NAs with zero value
pharma.fillna(0, inplace=True)
# Merge with LCID (upper level)
npa = npa.merge(pharma[["zipcode", "pharmacies"]], how="left", on="zipcode")

# NUMBER OF ALL KIND VACCINATION CENTERS (VACCINATION CENTERS, PHARMARCIES, DOCTOR'S OFFICES)
# Merge the count of doctor's offices, pharmarcies and vaccination centers per zipcode in a single dataframe
all_vacc_ctrs = (
    doctoff[["zipcode", "ptot", "nb_offices"]]
    .merge(centers[["zipcode", "id"]], how="inner", on="zipcode")
    .merge(pharma[["zipcode", "city"]], how="inner", on="zipcode")
)
all_vacc_ctrs["all_vacc_ctrs"] = (
    (all_vacc_ctrs["nb_offices"] + all_vacc_ctrs["id"] + all_vacc_ctrs["city"])
    * 1000
    / all_vacc_ctrs["ptot"]
)
# Merge with LCID (upper level)
npa = npa.merge(all_vacc_ctrs[["zipcode", "all_vacc_ctrs"]], how="left", on="zipcode")

# DISTANCE TO NEAREST VACCINATION CENTER FROM NPA CENTROID
# Extract NPA centroids from GEOSAN DB
npa_gdf = gpd.read_postgis(
    "SELECT lcid, st_x(st_centroid(geometry)) as x, st_y(st_centroid(geometry)) as y, st_centroid(geometry) as geometry FROM npa",
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
npa_gdf_new = sp.compute_nearest_target_distance(npa_gdf, centers, graph)
# Rename columns
npa_gdf_new.rename(
    columns={
        "nearest_targ_id": "nrst_vacc_ctr_id",
        "nearest_targ_dist": "nrst_vacc_ctr_dist",
    },
    inplace=True,
)
npa_gdf.loc[
    npa_gdf.nrst_vacc_ctr_dist == "No reachable",
    ["nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"],
] = np.nan
npa_gdf_new["nrst_vacc_ctr_dist"] = npa_gdf_new["nrst_vacc_ctr_dist"].astype(
    float
)  # Convert to float
# Merge with NPAs
npa = npa.merge(
    npa_gdf_new[["lcid", "nrst_vacc_ctr_id", "nrst_vacc_ctr_dist"]],
    how="left",
    on="lcid",
)

# NUMBER OF COVID-19 CASES PER 1000 HABS
cases = pd.read_csv(
    "Vaccination mobile @ DGS/data/COVID-19 DATA/EPICOVID personnes positives.csv",
    delimiter="\t",
)
# Remove cases that are not in Vaud
cases[~cases.zip.isin(tuple(npa.zipcode.unique()))]
cases = cases[cases.zip.isin(tuple(npa.zipcode.unique()))]
cases.columns = ["zipcode", "nb_cas"]  # Rename columns
# Join with NPAs to compute doctor's offices density
cases = (
    npa.groupby("zipcode")["ptot"]
    .agg("sum")
    .reset_index()
    .merge(cases, how="left", on="zipcode")
)
cases["covid19_cases"] = cases.nb_cas * 1000 / cases.ptot
# Merge with LCID (upper level)
npa = npa.merge(cases[["zipcode", "covid19_cases"]], how="left", on="zipcode")

# NUMBER OF COVID-19 VACCINATION PER 1000 HABS
vacc = pd.read_excel(
    "Vaccination mobile @ DGS/data/COVID-19 DATA/Extraction NPA des personnnes vaccinées - Vacovid.xlsx"
)
# Remove vaccinations that are not in Vaud
vacc[~vacc.NPA.isin(tuple(npa.zipcode.unique()))]
vacc = vacc[vacc.NPA.isin(tuple(npa.zipcode.unique()))]
vacc.columns = ["zipcode", "nb_vaccination"]  # Rename columns
# Join with NPAs to compute doctor's offices density
vacc = (
    npa.groupby("zipcode")["ptot"]
    .agg("sum")
    .reset_index()
    .merge(vacc, how="left", on="zipcode")
)
vacc["vaccination"] = vacc.nb_vaccination * 1000 / vacc.ptot
# Merge with LCID (upper level)
npa = npa.merge(vacc[["zipcode", "vaccination"]], how="left", on="zipcode")

# NUMBER OF INHABITANTS WITHIN A 5KM-RADIUS AROUND NPA CENTROID
sql_pop5km = "SELECT n.lcid, SUM(s.b19btot) as ptot5km FROM \
(SELECT lcid, ST_Buffer(st_centroid(geometry),5000) as geometry FROM npa) n \
INNER JOIN inhabited_ha_centroid s ON ST_Contains(n.geometry,s.geometry) \
GROUP BY n.lcid"
sql_pop5km = pd.read_sql(sql_pop5km, conn)
# Join with NPAs
npa = npa.merge(sql_pop5km, how="left", on="lcid")

# NUMBER OF 65+ WITHIN A 5KM-RADIUS AROUND NPA CENTROID
sql_65plus5km = "SELECT n.lcid, SUM(b19bm14+b19bm15+b19bm16+b19bm17+b19bm18+b19bm19+b19bw14+b19bw15+b19bw16+b19bw17+b19bw18+b19bw19) as p65plus5km FROM \
(SELECT lcid, ST_Buffer(st_centroid(geometry),5000) as geometry FROM npa) n \
INNER JOIN inhabited_ha_centroid s ON ST_Contains(n.geometry,s.geometry) \
GROUP BY n.lcid"
sql_65plus5km = pd.read_sql(sql_65plus5km, conn)
# Join with NPAs
npa = npa.merge(sql_65plus5km, how="left", on="lcid")

# FILL ALL ATTRIBUTES TO ZERO FOR LCID WITH PTOT=0
npa.loc[npa.ptot == 0, ~npa.columns.isin(["lcid", "zipcode"])] = 0

# EXTRACT NPA GEOMETRY FROM GEOSAN DB
npa_geom = gpd.read_postgis("SELECT lcid, geometry FROM npa", conn, geom_col="geometry")
# Merge with the indicators
npa_geom = npa_geom.merge(npa, how="left", on="lcid")

# SAVE INDICATORS
output_path = "Vaccination mobile @ DGS/results/indicators/"
if os.path.exists(output_path + "indicatorsNPA.gpkg"):
    print("File already exists. Deleted.")
    os.remove(output_path + "indicatorsNPA.gpkg")
npa_geom.to_file(output_path + "indicatorsNPA.gpkg", driver="GPKG")

# Difference between ptot and SUM(b19btot) by NPA
# "SELECT n.lcid, SUM(s.b19btot) as sptot, n.ptot FROM npa n INNER JOIN inhabited_ha_centroid s ON ST_Contains(n.geometry,s.geometry)  GROUP BY lcid"

# CLOSE CONNECTION WITH DB
conn.close()
