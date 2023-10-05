#LIBRARIES
#Basic
import pandas as pd
import os
import subprocess
import numpy as np
import sys
#Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps
#Spatial
import geopandas as gpd
#User defined function
sys.path.append(r'./FUNCTIONS/')
import shortestPaths as sp

#CONNECT TO DB
pw=getpass.getpass() #Ask for user password
engine=create_engine("postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)) #Create SQLAlchemy engine
conn=ps.connect("dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)) #Create a connection object
cursor=conn.cursor() #Create a cursor object

#EXTRACT RELI CENTROIDS FROM DATABASE
reli_gdf=gpd.read_postgis("SELECT reli, geometry FROM inhabited_ha_centroid",conn, geom_col='geometry')
reli=pd.read_sql("SELECT reli FROM inhabited_ha_centroid",conn)

#PROPORTION OF SOCIOPROFESSIONAL CATEGORIES (CSP), AND MEDIAN INCOME BY RELI
sql_mg="SELECT s.reli, n.ciqmd, n.rpcsp1/100 as rpcsp1, n.rpcsp2/100 as rpcsp2, n.rpcsp3/100 as rpcsp3 \
FROM inhabited_ha_centroid s LEFT JOIN npa n ON ST_Within(s.geometry,n.geometry)"
sql_mg=pd.read_sql(sql_mg,conn)
#Join with RELIs
reli=reli.merge(sql_mg,how='left',on='reli')

#PROPORTION OF +65Y/O, SWISS, FOREIGNERS AND NATIONALITY CATEGORY BY RELI
sql_sp="SELECT reli, b19btot, \
CAST((b19bm14+b19bm15+b19bm16+b19bm17+b19bm18+b19bm19+b19bw14+b19bw15+b19bw16+b19bw17+b19bw18+b19bw19) AS DOUBLE PRECISION)/b19btot as p65plus, \
CAST(b19b21 AS DOUBLE PRECISION)/b19btot as swiss, CAST(b19b26 AS DOUBLE PRECISION)/b19btot as foreigners, \
CAST(b19b27 AS DOUBLE PRECISION)/b19btot as ue, CAST(b19b28 AS DOUBLE PRECISION)/b19btot as europe_nn_eu, \
CAST(b19b29 AS DOUBLE PRECISION)/b19btot as non_europe FROM inhabited_ha_centroid"
sql_sp=pd.read_sql(sql_sp,conn)
#Replace by 1 if proportion > 1
sql_sp.loc[(sql_sp.p65plus > 1),'p65plus']=1
sql_sp.loc[(sql_sp.swiss > 1),'swiss']=1
sql_sp.loc[(sql_sp.foreigners > 1),'foreigners']=1
sql_sp.loc[(sql_sp.ue > 1),'ue']=1
sql_sp.loc[(sql_sp.europe_nn_eu	 > 1),'europe_nn_eu']=1
sql_sp.loc[(sql_sp.non_europe > 1),'non_europe']=1
#Join with RELIs
reli=reli.merge(sql_sp,how='left',on='reli')

#PROPORTION OF OLD PEOPLE (+65YRS) LIVING ALONE (OPLA) BY RELI
sql_opla="SELECT p.reli, (CAST(o.nb_opla AS DOUBLE PRECISION) / CAST(p.nb_indiv AS DOUBLE PRECISION))  as opla FROM \
(SELECT r.reli, COUNT(s.index) as nb_opla FROM statpop r \
INNER JOIN (SELECT index, geometry FROM statpop_indiv WHERE countagrofpersonpermphh=1.0 \
AND (statpop_indiv.countofmaleover65perm=1 OR countofmaleover80perm=1 OR countoffemaleover65perm=1 OR countoffemaleover80perm=1)) s \
ON ST_Contains(r.geometry,s.geometry) GROUP BY reli) o \
RIGHT JOIN \
(SELECT r2.reli, COUNT(s2.index) as nb_indiv \
FROM statpop r2 INNER JOIN statpop_indiv s2 \
ON ST_Contains(r2.geometry,s2.geometry) GROUP BY reli) p \
ON o.reli=p.reli"
sql_opla=pd.read_sql(sql_opla,conn)
#Join with RELIs
reli=reli.merge(sql_opla,how='left',on='reli')
#Fill na with zero
reli['opla']=reli.opla.fillna(value=0)

#EXTRACT RELI GEOMETRY FROM GEOSAN DB
reli_geom=gpd.read_postgis("SELECT reli, geometry FROM statpop",conn,geom_col='geometry')
#Merge with the indicators
reli_geom=reli_geom.merge(reli,how='left',on='reli')

#SAVE INDICATORS
output_path='Vaccination mobile @ DGS/results/indicators/'
if os.path.exists(output_path+'indicatorsRELI_socioeco.gpkg'):
    print('File already exists. Deleted.')
    os.remove(output_path+'indicatorsRELI_socioeco.gpkg')
reli_geom.to_file(output_path+'indicatorsRELI_socioeco.gpkg',driver='GPKG')

#CLOSE CONNECTION WITH DB
conn.close()

# #Extract ha in Lausanne from GEOSAN DB
# reli_lausanne=gpd.read_postgis("SELECT s.reli, st_x(s.geometry) as x, st_y(s.geometry) as y, s.geometry FROM inhabited_ha_centroid s INNER JOIN (SELECT * FROM municipalities WHERE name='Lausanne') m ON st_within(s.geometry,m.geometry)",conn, geom_col='geometry')
# #Import Vaccination Centers
# centers=gpd.read_file("Vaccination mobile @ DGS/data/CENTRES VACCINATION PC (CONFIDENTIAL)/210318_Vaccination_Centers.shp")
# #Extract projected graph for road networks in VD (drive)
# graph=sp.import_graph('Lausanne,Vaud,Switzerland','walk',2056)
# #Add id and distance of nearest vaccination centers for each NPA centroid
# reli_lausanne=sp.compute_nearest_target_distance(reli_lausanne,centers,graph)
# #Rename columns
# reli_lausanne.rename(columns = {'nearest_targ_id':'nrst_vacc_ctr_id', 'nearest_targ_dist':'nrst_vacc_ctr_dist'}, inplace = True)
# reli_lausanne.to_file('reli_lausanne.shp',driver='ESRI Shapefile')
# #EXTRACT RELI GEOMETRY FROM GEOSAN DB
# reli_geom=gpd.read_postgis("SELECT reli, geometry FROM statpop",conn,geom_col='geometry')
# #Merge with the indicators
# reli_geom=reli_geom.merge(reli,how='left',on='reli')
# reli_geom=reli_geom.merge(reli_lausanne[['reli','nrst_vacc_ctr_id','nrst_vacc_ctr_dist']],how='inner',on='reli')
# reli_geom.to_file('reli_lausanne.geojson',driver='GeoJSON')
# # Plot the shortest path
# fig, ax = ox.plot_graph_route(graph_proj, route)
