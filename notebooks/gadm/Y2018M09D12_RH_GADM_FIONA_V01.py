
# coding: utf-8

# In[1]:

""" Convert GeoPackage to Bigquery Compatible Format

Approach:

read geospatial datatype
for each feature
    convert geometry to bigquery compatible string (wkt, geojson string)
    store converted geometry, id and type in same dictionary as properties
    write dictionary to csv file on GCP

Future: Use Apache Beam to make the approach serverless.

TODO:

allow direct upload to GCS instead of writing to VM.  
Add other geospatial datastypes.
Add multiprocessing (or Apache Beam if I want to make it fancy)

"""

SCRIPT_NAME = "Y2018M09D12_RH_GADM_FIONA_V01"
OUTPUT_VERSION = 8

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

ec2_input_path = "/volumes/data/%s/input_V%0.2d" %(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/%s/output_V%0.2d" %(SCRIPT_NAME,OUTPUT_VERSION)


# In[2]:

import os
import fiona
import pandas as pd
from shapely.geometry import shape


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[4]:

filename_zip = "gadm36_gpkg.zip" 
filename_gpkg = "gadm36.gpkg"
url = "https://biogeo.ucdavis.edu/data/gadm3.6/%s" %filename_zip
file_path_zip = "%s/%s" %(ec2_input_path, filename_zip)
file_path_gpkg = "%s/%s" %(ec2_input_path, filename_gpkg)


# In[5]:

get_ipython().system('wget {url} -P {ec2_input_path}')


# In[6]:

get_ipython().system('unzip {file_path_zip} -d {ec2_input_path}')


# In[7]:

def process_feature(f):
    """ Process Feature
    Args:
        f (dictionary) : input fiona feature.
    Returns:
        d (dictionary) : geometry converted to geoJSON and added to dict.    
    """
    geom = shape(f["geometry"])    
    wkt = geom.wkt
    d = f["properties"]
    d["id"] = f["id"]
    d["type"] = f["type"]
    d["geometry"] = wkt
    df = pd.DataFrame([d])
    output_file_path = "{}/{}_{}".format(ec2_output_path,layername,'id{}.csv'.format(f["id"]))
    df.fillna(-9999)
    print(df.shape)
    df.to_csv(output_file_path, index=False)
    return d


# In[8]:

with fiona.drivers():
    for layername in fiona.listlayers(file_path_gpkg):
        print(layername)
        with fiona.open(file_path_gpkg, layer=layername) as src:
            # src is an iterator and might get parallelized. 
            for f in src: #TODO: use Multiprocessing
                d = process_feature(f)
            


# In[ ]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# Now the data is in WKT on bigquery. You can visualize using: https://bigquerygeoviz.appspot.com/
# 
# the last step would be to convert the WKT to geometry on bigquery. SELECT *, ST_GeogFromText(geometry) AS g FROM aqueduct30.spatial_test.gadm_levelx_wkt
# 
# please note that that the hexagons will be in Geopraphy instead of geometry!
# 
# happy mapping.
