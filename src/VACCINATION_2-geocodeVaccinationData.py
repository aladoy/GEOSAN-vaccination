#LIBRAIRIES
#Basic
import pandas as pd
import sys
from pandarallel import pandarallel
import os
#Database
import getpass
from sqlalchemy import create_engine
import psycopg2 as ps
#User defined function
sys.path.append(r'./FUNCTIONS/')
import geocoding_utils as g
#Spatial
import geopandas as gpd
from shapely.geometry import Point


#IMPORT DATA

#NPA
npa=pd.read_csv(r'Vaccination mobile @ DGS/data/NPA 2021/PLZO_CSV_LV95.csv', delimiter=';',encoding='iso-8859-1')
npa.shape
#Remove accents in municipalities
npa['Ortschaftsname']=npa.Ortschaftsname.map(g.strip_accents)
#Convert municipalities to upper case
npa['Ortschaftsname']=npa.Ortschaftsname.map(str.upper)
#Keep only VD
#npa=npa[npa['Kantonskürzel']=='VD']
#Select only essential columns
npa=npa[['Ortschaftsname','PLZ','E','N','Kantonskürzel']]
npa.head(5)

#REGBL
vd_addr=pd.read_csv(r'Vaccination mobile @ DGS/data/REGBL 2021/VD.csv', delimiter=';')
vd_addr.shape
vd_addr.head(5)
#Data wrangling
vd_addr=g.regbl_wrangling(vd_addr)

#VACCINATION
#Load file
data=pd.read_excel(r'Vaccination mobile @ DGS/data/PERSONNES VACCINEES (CONFIDENTIAL)/210609_06_03_LI_COVID19_Extract-Vacovid-Phase2.xlsx')
data.shape
data.reset_index(drop=False,inplace=True)
data.columns=['index','rue','npa','ville']


#DATA CLEANING

#Remove ville with NaN values after checking the addresses are not in Switzerland
print('Number of rows with ville=NaN: ' + str(data[data.ville.isna()].shape[0]))
data=data[~data.ville.isna()]
#Remove mobile vaccination
print('Number of rows corresponding to "Mobile": '+str(data[data.ville=='Mobile'].shape[0]))
data=data[data.ville!='Mobile']
#Remove SDF
print('Number of rows corresponding to homeless people: ' +str(data[data.ville=='*** SANS DOMICILE FIXE'].shape[0]))
data=data[data.ville!='*** SANS DOMICILE FIXE']
#Remove Reprise de BHIS (?)
print('Number of rows corresponding to REPRISE DE BHIS: ' +str(data[data.ville.str.startswith('*** REPRISE DE BHIS')==True].shape[0]))
data=data[data.ville!='*** REPRISE DE BHIS (CF. 3EME LIGNE ADR.']
#Correct manually some rows
data.loc[13028,['npa','ville']]=[1095,'Lutry']
data.loc[[172283,226198],'npa']=1680
data.loc[303741,'npa']=1030
data.loc[47535,'npa']=1004
data.loc[145164,'npa']=1895
data.loc[3334,'npa']=1024
data.loc[1672,'ville']='Saint-Aubin-Sauges'
data.loc[120673,'ville']='VILLENEUVE'
data.loc[144240,'ville']='Corseaux'
data.loc[160746,'ville']='CLARENS'
data.loc[142203,'ville']='LAUSANNE'
data.loc[122709,'ville']='CHATEL-SAINT-DENIS'
data.loc[242171,'ville']='GLION-SUR-MONTREUX'
data.loc[211961,'ville']='CHAVANNES-PRES-RENENS'
data.loc[289466,'ville']='GLION'
data.loc[253074,['npa','ville']]=[1000,'Lausanne']
data.loc[187358,['npa','ville']]=[1852,'Roche']
data.loc[26304,['npa','ville']]=[1290,'CHAVANNNES DES BOIS']
data.loc[7395,'ville']='Founex'
data.loc[95036,'ville']='Sainte-Croix'
data.loc[96211,'ville']='Préverenges'
data.loc[96237,'ville']='Renens'
data.loc[323453,'ville']='Payerne'
data.loc[168948,['npa','ville']]=[1164,'BUCHILLON']
data.loc[50688,['npa','ville']]=[1092,'BELMONT-SUR-LAUSANNE']
data.loc[212902,['rue','ville']]=['chemin de la rosalette 33','Corsier-sur-Vevey']
data.loc[15685,'npa']=1091
data.loc[26047,'npa']=1063
data.loc[31309,'npa']=1628
data.loc[33525,'npa']=1802
data.loc[54829,'npa']=1009
data.loc[61796,'npa']=1295
data.loc[90403,'npa']=1295
data.loc[185720,'npa']=1884
data.loc[213297,'npa']=1299
data.loc[221281,'npa']=1700
data.loc[227118,'npa']=1025
data.loc[278689,'npa']=1950
data.loc[281427,'npa']=1350
data.loc[328264,'npa']=1110
data.loc[77,['npa','ville']]=[1814,'LA TOUR-DE-PEILZ']
data.loc[8036,'npa']=1090
data.loc[126276,'npa']=1095
data.loc[[8484,264273],'npa']=1860
data.loc[12511,'npa']=1020
data.loc[16683,['npa','ville']]=[1052,'LE MONT-SUR-LAUSANNE']
data.loc[43573,'npa']=1148
data.loc[47901,'npa']=1814
data.loc[51177,'npa']=3960
data.loc[53248,'npa']=1450
data.loc[54813,'npa']=1091
data.loc[55064,'npa']=1071
data.loc[55119,'npa']=1446
data.loc[48568,'npa']=1083
data.loc[151961,'npa']=3013
data.loc[247872,'npa']=3011
data.loc[96252,'npa']=1920
data.loc[32378,'npa']=1277
data.loc[[134850,144487],'npa']=1279
data.loc[136128,'npa']=1270
data.loc[151064,'npa']=1040
data.loc[160973,'npa']=1070
data.loc[181223,['npa','ville']]=[1018,'LAUSANNE']
data.loc[188158,'npa']=1030
data.loc[201159,'npa']=1022
data.loc[[232663,239382],'npa']=1260
data.loc[247913,'npa']=1024
data.loc[263492,'npa']=1046
data.loc[46587,['npa','ville']]=[1304,'DIZY']
data.loc[320036,'npa']=1023
data.loc[233936,'ville']='GRYON'
data.loc[323088,'npa']=1400
data.loc[207317,'rue']='Chemin de Gravernay 19'
data.loc[254941,'rue']='Rue de Lausanne 8'
data.loc[326048,['npa','ville']]=[1090,'LUTRY']
data.loc[332201,'npa']=1295
data.loc[data.ville=='YVERDON',['npa','ville']]=[1400,'YVERDON-LES-BAINS']
data.loc[(~data.npa.isin(npa.PLZ)) & (data.ville=='YVERDON-LES-BAINS'),'npa']=1400
data.loc[(~data.npa.isin(npa.PLZ)) & (data.ville=='MONTAGNY-CHAMARD'),['npa','ville']]=[1442,'MONTAGNY-PRES-YVERDON']
#Remove distribution / case postale
data=data[~(data.rue.str.contains('case postale',case=False) | (data.rue.str.contains('CASE POSTAL',case=False)) | (data.ville.str.contains(' Dist',case=False)) | (data.rue.str.contains('CP ',case=False)))]
data=data[~(data.rue.str.contains('POSTE RESTANTE',case=False) | data.rue.str.contains('BP ',case=False) | data.rue.str.contains('boite postal',case=False) | data.rue.str.contains('boîte postal',case=False))]
data=data[~(data.npa.isin([1211,1001,1002,1014,1200]))]
print('Number of rows in Zone de distribution / Cases postales: ' + str(345075-data.shape[0]))
#Remove rows from foreign / unknown country
data=data[~(data.ville.str.startswith('***')==True)]
data=data[~((data.npa.str.contains('-'))  & (~data.npa.isin(['Saint-Cièrges','chavannes-près-renens','St-Sulpice'])))]
data=data[data.ville!='FRANCE']
data=data[~((data.npa.str.contains('[A-Za-z]',regex=True)) & (~data.npa.isin(['lavigny','Lausanne','1030Q','1010 Lausanne','1005 Lausanne','1008 Prilly','route de berne 307','MIES','Ecublens','1860A','St-Sulpice','lausanne','1011 Lausanne','Ch. de Potex 6','Rolle','chavannes-près-renens','1264 St Cergue','Saint-Cièrges','Lutry','1295 Mies','1000 Lausanne 22','pully'])))] #Characters in NPA
data=data[~(data.ville.str.contains('^[0-9]',regex=True) & (~data.index.isin([261019,253074,24693,128463,174012,188225,191307])))] #Digits in Ville
data=data[data.npa!='?']
data=data[(data.npa!='...') & (data.npa!='.')]
data=data[~data.index.isin([135675,129831,272375,278158])]
data=data.loc[~data.index.isin(data[(data.npa.str.contains(' ')) & (~data.npa.isin(['1295 Mies','1264 St Cergue','1000 Lausanne 22','1011 Lausanne','1008 Prilly','1005 Lausanne','1010 Lausanne']))].index)]
data=data[data.ville!='INCONNU']
#Swap NPA and Ville for given rows
idx_toswap=[117742,134691,165117,168009,200909,229413,230023,230162,230391,230513,231063,243471,314320]
data.loc[idx_toswap] = data.loc[idx_toswap].rename(columns={'npa':'ville','ville':'npa'})
#Spit npa into npa + ville for specific rows
idx_tosplit=[49445,161326,191009,262264,267020,273570]
data.loc[idx_tosplit,'ville']=data.loc[idx_tosplit].npa.str.split(' ',1,expand=True)[1].str.strip()
data.loc[idx_tosplit,'npa']=data.loc[idx_tosplit].npa.str.split(' ',1,expand=True)[0]
#Spit ville into npa + ville for specific rows
idx_tosplit=[24693,128463,174012,188225,191307,261019]
data.loc[idx_tosplit,'npa']=data.loc[idx_tosplit].ville.str.split(' ',1,expand=True)[0]
data.loc[idx_tosplit,'ville']=data.loc[idx_tosplit].ville.str.split(' ',1,expand=True)[1].str.strip()
#Assign npa=1011 to CHUV
data.loc[data.npa==1011,['rue','ville']]=['CHUV','LAUSANNE']
#Remove trailing digits in ville
idx_tostrip=data[data.ville.str.contains('[0-9]$',regex=True)==True].index
data.loc[data.index.isin(idx_tostrip),['ville']]=data[data.index.isin(idx_tostrip)].ville.str.replace('\d+', '').str.strip()
#Remove other rows that are from foreign / unknown cities
data=data[~(data.ville.str.contains('[0-9]',regex=True)==True)]
data=data[~(data.ville.str.contains('France')==True)]
data=data[~(data.ville.str.contains(',')==True)]
#Remove ville in integer
def is_str(v):
    return type(v) is str
data=data[~(data.ville.map(is_str)==False)]
data=data[~(data.rue.map(is_str)==False)]

#CONVERT NPA TO FLOAT
data['npa']=pd.to_numeric(data.npa)

#Continue to remove rows from foreign / unknown countries
data=data[data.npa>999]
data=data[data.npa<9999]
data=data[data.npa.isin(npa.PLZ)]
print('Number of rows with foreign / unkwown country: ' + str(343430-data.shape[0]))
#KEEP ONLY ADDRESSES IN VAUD
npa=npa[npa['Kantonskürzel']=='VD']
print('Number of rows outside VD: ' +str(data[~data.npa.isin(npa.PLZ)].shape[0]))
data=data[data.npa.isin(npa.PLZ)]
#REMOVE HOTELS/RESIDENCES
print('Number of rows in rooms/hotels: ' +str(data[(data.rue.str.contains('chambre')==True)].shape[0]))
data=data[~(data.rue.str.contains('chambre',case=False)==True)]

#STRIP ACCENTS IN VILLE + UPPER
data['ville']=data.ville.map(str.upper).map(g.strip_accents)
#Correct few countries
idx_swissremove=data[data.ville.str.contains('SUISSE')].index
data.loc[idx_swissremove,['ville']]=data.loc[idx_swissremove].ville.str.split(' - ',expand=True)[0].str.strip()

#Correct some cities
data.loc[data.ville=='LAVEY-LES-BAINS','ville']='LAVEY-MORCLES'
data.loc[(data.ville.str.contains('-LES-BAINS')) & (~data.ville.isin(vd_addr.GDENAME.unique())),'ville']='YVERDON-LES-BAINS'
data.loc[data.npa==1801,'ville']='CHARDONNE'
data.loc[data.ville=='YVERDON','ville']='YVERDON-LES-BAINS'
data.loc[(data.ville=="L'ORIENT") | (data.ville=="ORIENT"),'ville']='LE CHENIT'

#Remove foreign cities
data=data[~data.ville.str.contains('GLANE')]
data=data[~data.ville.str.contains('GENEVE')]
data=data[~data.ville.isin(['SEGNY','CESSEY','CESSY','DIVONNE LES BAINS','GEX','SOFIA','LJUBLJANA','ETTERBEEK BELGIQUE','VERSOIX','DOMMARTIN','BUCAREST','LISBOA','SAO PAULO','CESSY FRANCE','ZURICH','GENEVE'])]
#TO IMPROVE:data[~data.ville.isin(vd_addr.GDENAME.unique())].ville.unique()


#DATA WRANGLING BEFORE GEOCODING

#Split address in house number / street name
data['deinr'],data['strname'] = zip(*data['rue'].map(g.split_address))

#Remove c/o from deinr
idx_deinrco=data[data.deinr.str.contains('c/o')==True].index
data.loc[idx_deinrco,['deinr']]=data.loc[idx_deinrco].deinr.str.split(',',expand=True)[0].str.strip()
idx_deinrco2=data[data.deinr.str.contains('c/o')==True].index
data.loc[idx_deinrco2,['deinr']]=data.loc[idx_deinrco2].deinr.str.split(' ',expand=True)[0].str.strip()

#Remove étage from deinr
idx_deinret=data[data.deinr.str.contains('étage',case=False)==True].index
data.loc[idx_deinret,['deinr']]=data.loc[idx_deinret].deinr.str.split(',',expand=True)[0].str.strip()

#Remove c/o from strname
idx_strco=data[(data.strname.str.startswith('c/o')==True)].index
data.loc[idx_strco,['strname']]=data.loc[idx_strco].strname.str.split(',',expand=True)[1].str.strip()
idx_strco2=data[(data.strname.str.contains('c/o',case=False)==True)].index
data.loc[idx_strco2,['strname']]=data.loc[idx_strco2].strname.str.split(',',expand=True)[0].str.strip()

#Remove extra part in deinr
indexcomma=data[data.deinr.str.contains(',')==True].index
data.loc[indexcomma,['deinr']]=data.loc[indexcomma].deinr.str.split(',',expand=True)[0].str.strip()
idx_deinrextra=data[data.deinr.str.contains('(.*[a-z]){4}')==True].index
data.loc[idx_deinrextra,['deinr']]=data.loc[idx_deinrextra].deinr.str.split(' ',expand=True)[0].str.strip()
idx_deinrextra2=data[data.deinr.str.contains('(.*[a-z]){4}')==True].index
data.loc[idx_deinrextra2,['deinr']]=data.loc[idx_deinrextra2].deinr.str.split('-',expand=True)[0].str.strip()
#Manual fill
data.loc[120266,'deinr']=5
data.loc[147129,'deinr']='2A'

#Remove strname that are nan
data=data[~data.strname.isna()]
#Remove strname that are c/o and not ems
data=data[~(data.strname.str.contains('c/o',case=False)==True) & (~data.strname.str.contains('EMS',case=False)==True)]

#Remove deinr that only contains etage
data=data[~(data.deinr.str.contains('étage',case=False)==True)]

#Remove EMS in deinr
data[data.deinr.str.contains(',')==True].deinr.str.contains('^[0-9]').all()

# #REMOVE EMS, FONDATION, RESIDENCE
# data=data[~(data.rue.str.contains('ems ',case=False))]
# data=data[~((data.rue.str.contains('residence ',case=False))| (data.rue.str.contains('résidence ',case=False)))]
# data=data[~(data.rue.str.contains('fondation ',case=False))]
# print('Number of EMS/residence/fondation removed: ' + str(326885-data.shape[0]))
# #Remoce c/o in deinr
# idx_coremove=data[data.deinr.str.contains('c/o')==True].index
# data.loc[idx_coremove,'deinr']=data.loc[idx_coremove].deinr.str.split('c/o',expand=True)[0].str.strip().str.replace(',','')
# #Remove ,... in deinr
# idx_commaremove=data[data.deinr.str.contains(',')==True].index
# data.loc[idx_commaremove,'deinr']=data.loc[idx_commaremove].deinr.str.split(',',expand=True)[0].str.strip()

#STRIP ACCENTS IN STRNAME + UPPER
data['strname']=data.strname.map(str.upper).map(g.strip_accents)

#Remve space in deinr + convert to lower case
data['deinr']=data.deinr.str.replace(" ","").str.lower()


#GEOCODING

#GROUP BY ADDRESS
data_addr=data.groupby(by=['ville','npa','strname','deinr']).count().reset_index(drop=False)[['ville','npa','strname','deinr','index']]
data_addr.reset_index(drop=False,inplace=True)
data_addr.columns=['idx','ville','npa','strname','deinr','nb_vacc']
data_addr['deinr']=data_addr['deinr'].astype(str)
data_addr.shape

#LAUNCH GEOCODING (PARALLEL PROCESSING)
pandarallel.initialize(nb_workers=11) #Initialize parallel processing

%time data_addr['gkode'],data_addr['gkodn'],data_addr['note_geocoding']=zip(*data_addr.parallel_apply(lambda row: g.get_coords(row,vd_addr),axis=1))

#Manual correction for biggest nb_vacc
data_addr.loc[71764,['gkode','gkodn','note_geocoding']]=[2537134.0,1151526.0,'Geocoded at street.']

#Print data that could not be geocode
data_addr[data_addr.note_geocoding.str.startswith('Geocoded at NPA centroid')].sort_values('nb_vacc',ascending=False).head(20)
#Print number of addresses in each category
data_addr.groupby(by='note_geocoding').count()

#CONVERT DATAFRAME TO GEODATAFRAME
#Create a geometry column using Shapely
data_addr=data_addr.assign(geometry=data_addr.apply(lambda row: Point(row.gkode, row.gkodn),axis=1))

#Convert to geodataframe
data_addr=gpd.GeoDataFrame(data_addr, geometry=data_addr.geometry, crs={'init': 'epsg:2056'})

#Add lat/lon coordinates in new columns
data_addr['lon']=data_addr.to_crs({'init': 'epsg:4326'}).geometry.x
data_addr['lat']=data_addr.to_crs({'init': 'epsg:4326'}).geometry.y


#SAVE RESULTS

#EXPORT TO GEOPACKAGE FILE
path='Vaccination mobile @ DGS/results/phase 2/vaccination_data_geocoded_points.gpkg'
try:
    if os.path.exists(path):
        os.remove(path)
    data_addr.to_file(path,driver='GPKG')
    print('Sucess')
except:
    print('Error while saving data on disk')

#CONNECT TO DB
pw=getpass.getpass() #Ask for user password
engine=create_engine("postgresql+psycopg2://aladoy:{}@localhost/geosan".format(pw)) #Create SQLAlchemy engine
conn=ps.connect("dbname='geosan' user='aladoy' host='localhost' password='{}'".format(pw)) #Create a connection object
cursor=conn.cursor() #Create a cursor object

#CREATE TABLE IN GEOSAN DB
data_addr.to_postgis('vaccination_address',engine,schema='vaccination',if_exists='replace')
