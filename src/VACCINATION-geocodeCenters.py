#LIBRARIES
#Basic
import pandas as pd
import requests
#Spatial
import geopandas as gpd
from shapely.geometry import Point

#IMPORT VACCINATION CENTERS COORDINATES
#Read excel file
centers=pd.read_excel("Vaccination mobile @ DGS/data/CENTRES VACCINATION PC (CONFIDENTIAL)/210318_06_03_LI_Adresses-Centres-Vaccination-v2.xlsx")
#Create a field with full adress (street name, street number and locality)
centers['full_addr']=centers['rue'] + ' ' + centers['no_rue'].astype(str) + ' ' + centers['localite'].astype(str)

#GEOCODE CENTERS WITH GEOADMIN API
def easy_geocode(full_address):
    try:
        response = requests.get("https://api3.geo.admin.ch/rest/services/api/SearchServer?layer=ch.bfs.gebaeude_wohnungs_register&searchText="+full_address+"&type=locations&origins=address,zipcode&sr=2056")
        y=response.json()['results'][0]['attrs']['x']
        x=response.json()['results'][0]['attrs']['y']
    except:
        y=np.nan
        x=np.nan
    return x,y

#EXTRACT X/Y COORDINATES IN NEW COLUMNS
centers['x'],centers['y']=zip(*centers['full_addr'].map(easy_geocode))

#CREATE GEODATAFRAME
#Create a geometry column using Shapely
centers=centers.assign(geometry=centers.apply(lambda row: Point(row.x, row.y),axis=1))
#Convert to geodataframe
centers=gpd.GeoDataFrame(centers, geometry=centers.geometry, crs={'init': 'epsg:2056'})

#SAVE TO FILE
centers.to_file('Vaccination mobile @ DGS/data/CENTRES VACCINATION PC (CONFIDENTIAL)/210318_Vaccination_Centers.shp',driver='ESRI Shapefile')
