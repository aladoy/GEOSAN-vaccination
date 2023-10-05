#LIBRARIES
#Basic
import pandas as pd
#Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps
#Spatial
import geopandas as gpd

#CONNECT TO DB
pw=getpass.getpass() #Ask for user password
engine=create_engine("postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)) #Create SQLAlchemy engine
conn=ps.connect("dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)) #Create a connection object
cursor=conn.cursor() #Create a cursor object

#CREATE A SCHEMA FOR VACCINATION PROJECT (NAME=VACCINATION)
cursor.execute("CREATE SCHEMA IF NOT EXISTS vaccination AUTHORIZATION aladoy;")
conn.commit()

#FUNCTIONS
def import_data(dat, name, schema, pk, type_geom, idx_geom=False):
    print(dat.shape)
    print(dat.crs)
    dat.columns=map(str.lower,dat.columns) #convert columns to lower case
    dat.to_postgis(name, engine,schema=schema,if_exists='replace') #Add to postgis
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM {}.{}".format(schema,name))
    print("Number of rows in the table :", cursor.fetchone())
    cursor.execute("SELECT COUNT(*) FROM information_schema.columns where table_schema='{}' and table_name='{}'".format(schema,name))
    print("Number of columns in the table :", cursor.fetchall())
    if pk!='NULL':
        cursor.execute("ALTER TABLE {}.{} ADD PRIMARY KEY({});".format(schema,name,pk)) #Add PK
        conn.commit()
    if idx_geom==True:
        cursor.execute("CREATE INDEX idx_geom_{} ON {}.{} USING GIST(geometry);".format(name,schema,name)) #Add geometry index
        conn.commit()
    print('TABLE ', name, ' WAS SUCESSFULLY IMPORTED')

#IMPORT INDICATORS AT THE NPA LEVEL
npaInd=gpd.read_file(r"./Vaccination mobile @ DGS/results/indicators/indicatorsNPA.gpkg")
#Insert into GEOSAN DB
import_data(npaInd,'ind_npa','vaccination','lcid','POLYGON',idx_geom=True)

#IMPORT INDICATORS AT THE HA LEVEL
reliInd=gpd.read_file(r"./Vaccination mobile @ DGS/results/indicators/indicatorsRELI.gpkg")
#Insert into GEOSAN DB
import_data(reliInd,'ind_reli','vaccination','reli','POLYGON',idx_geom=True)

#CLOSE CONNECTION WITH DB
conn.close()
