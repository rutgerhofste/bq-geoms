
# coding: utf-8

# [View in Colaboratory](https://colab.research.google.com/github/rutgerhofste/bq-geoms/blob/master/Y2018M09D11_RH_GADM_V01.ipynb)

# 
# Python 3 runtime
# 
# ## create GADM derived table in bigquery. 
# Use case: Spatial Join in Bigquery
# 
# Question on stack exchange
# 
# Notebook is Google Colab compatible.
# 
# This script will process the global geopackage. Please note that geopandas handles data in memory so make sure your VM is sufficiently large to handle the level.

# In[1]:

# Select level 0 (country) - 5 (commune). See https://gadm.org/index.html for 
# more information.
LEVEL = 0


# In[2]:

#!pip install shapely geopandas descartes sqlalchemy pydrive psycopg2


# In[3]:

# Version 3.6 Date accessed 2018 09 11
# Compressed Size = 1.2 GB 
# Uncompressed Size =  3.5 GB

get_ipython().system('rm -r /volumes/data/Y2018M09D11_RH_GADM_V01/input_data')
get_ipython().system('mkdir -p /volumes/data/Y2018M09D11_RH_GADM_V01/input_data')


# In[4]:

url = "https://biogeo.ucdavis.edu/data/gadm3.6/gadm36_levels_gpkg.zip"
url = "https://biogeo.ucdavis.edu/data/gadm3.6/gpkg/gadm36_NLD_gpkg.zip"


# In[5]:

get_ipython().system('wget {url} -P /volumes/data/Y2018M09D11_RH_GADM_V01/input_data')


# In[6]:

import os
import fiona
import geopandas as gpd

from google.cloud import bigquery
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.client import GoogleCredentials


# In[9]:

files = os.listdir("/volumes/data/Y2018M09D11_RH_GADM_V01/input_data")


# In[10]:

file_name = files[0]


# In[11]:

get_ipython().system("unzip '/volumes/data/Y2018M09D11_RH_GADM_V01/input_data/{file_name}' -d /volumes/data/Y2018M09D11_RH_GADM_V01/input_data/")


# In[12]:

layer = "level{:01.0f}".format(LEVEL)


# In[ ]:

gdf = gpd.read_file("/volumes/data/Y2018M09D11_RH_GADM_V01/input_data/gadm36_levels.gpkg",layer=layer)


# In[ ]:

gdf.head()


# In[ ]:

gdf["geometry_wkt"] = gdf["geometry"].apply(lambda x: x.to_wkt())


# In[ ]:

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/.google.json"
client = bigquery.Client()


# In[ ]:

# Google Drive ID of the google bigquery creds.
# change to your own credentials.
BQ_CREDS_ID = "1JzjuosHbtV7mzE0f85ZTXsNc2X4p8BFK"
BQ_PROJECT_ID = "aqueduct30"

OUTPUT_DATASET = "spatial_test" #create dataset first if not exist.
OUTPUT_TABLE_NAME = "gadm_{}_wkt".format(layer)


# In[ ]:

destination_table = "{}.{}".format(OUTPUT_DATASET,OUTPUT_TABLE_NAME)


# In[ ]:

df = gdf.drop("geometry",axis=1)


# In[ ]:

df.shape


# In[ ]:

def 


# In[ ]:




# In[ ]:




# In[ ]:

df.to_gbq(destination_table=destination_table,
          project_id = BQ_PROJECT_ID,
          if_exists="replace",
          chunksize=10)


# Now the data is in WKT on bigquery. You can visualize using:
# https://bigquerygeoviz.appspot.com/
# 
# the last step would be to convert the WKT to geometry on bigquery. 
# SELECT *, ST_GeogFromText(geometry_wkt) AS g FROM `aqueduct30.spatial_test.gadm_levelx_wkt`
# 
# please note that that the hexagons will be in Geopraphy instead of geometry!
# 
# happy mapping.
# 
# 

# In[ ]:



