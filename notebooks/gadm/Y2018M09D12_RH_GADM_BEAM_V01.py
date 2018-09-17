
# coding: utf-8

# In[1]:

""" Convert GeoPackage to Bigquery Compatible Format

"""



SCRIPT_NAME = "Y2018M09D12_RH_GADM_BEAM_V01"
OUTPUT_VERSION = 1

ec2_input_path = "/volumes/data/%s/input_V%0.2d" %(SCRIPT_NAME,OUTPUT_VERSION)


# In[67]:

import os
import fiona
import geojson
from shapely.geometry import shape

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import GoogleCloudOptions, StandardOptions


# In[3]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')


# In[8]:

filename_zip = "gadm36_NLD_gpkg.zip"
filename_gpkg = "gadm36_NLD.gpkg"
url = "https://biogeo.ucdavis.edu/data/gadm3.6/gpkg/%s" %filename_zip
file_path_zip = "%s/%s" %(ec2_input_path, filename_zip)
file_path_gpkg = "%s/%s" %(ec2_input_path, filename_gpkg)


# In[5]:

get_ipython().system('wget {url} -P {ec2_input_path}')


# In[6]:

get_ipython().system('unzip {file_path_zip} -d {ec2_input_path}')


# In[ ]:

layer = 


# In[61]:

def process_feature(f):
    """ Process Feature
    Args:
        f (dictionary) : input fiona feature.
    Returns:
        d (dictionary) : geometry converted to geoJSON and added to dict.    
    """
    geom = shape(f["geometry"])    
    wkt = geom.wkt
    f["geometry"] = wkt
    return f


# In[62]:

with fiona.drivers():
    for layername in fiona.listlayers(file_path_gpkg):
        with fiona.open(file_path_gpkg, layer=layername) as src:
            # src is an iterator and might get parallelized. 
            for f in src:
                d = process_feature(f)
            


# In[63]:




# In[ ]:



