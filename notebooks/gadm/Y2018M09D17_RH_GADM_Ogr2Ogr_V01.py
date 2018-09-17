
# coding: utf-8

# In[1]:

""" Convert GeoPackage to Bigquery Compatible Format

Approach:

read geospatial datatype

ogr2ogr to converto to csv file

TODO:

allow direct upload to GCS instead of writing to VM.  
Add other geospatial datastypes.
Add multiprocessing (or Apache Beam if I want to make it fancy)

"""

SCRIPT_NAME = "Y2018M09D17_RH_GADM_Ogr2Ogr_V01"
OUTPUT_VERSION = 1

GCS_OUTPUT_PATH = "gs://aqueduct30_v01/{}".format(SCRIPT_NAME)

ec2_input_path = "/volumes/data/%s/input_V%0.2d" %(SCRIPT_NAME,OUTPUT_VERSION)
ec2_output_path = "/volumes/data/%s/output_V%0.2d" %(SCRIPT_NAME,OUTPUT_VERSION)


# In[2]:

get_ipython().system('rm -r {ec2_input_path}')
get_ipython().system('mkdir -p {ec2_input_path}')
get_ipython().system('rm -r {ec2_output_path}')
get_ipython().system('mkdir -p {ec2_output_path}')


# In[3]:

filename_zip = "gadm36_gpkg.zip" 
filename_gpkg = "gadm36.gpkg"
url = "https://biogeo.ucdavis.edu/data/gadm3.6/%s" %filename_zip
file_path_zip = "%s/%s" %(ec2_input_path, filename_zip)
file_path_gpkg = "%s/%s" %(ec2_input_path, filename_gpkg)


# In[4]:

get_ipython().system('wget {url} -P {ec2_input_path}')


# In[5]:

get_ipython().system('unzip {file_path_zip} -d {ec2_input_path}')


# In[6]:

ec2_output_file_path = "{}/gadm36.csv".format(ec2_output_path)


# In[7]:

command = "/opt/anaconda3/envs/python35/bin/ogr2ogr -f 'CSV' -lco GEOMETRY=AS_WKT {} {}/{}".format(ec2_output_file_path,ec2_input_path,filename_gpkg)


# In[8]:

command


# In[9]:

import subprocess


# In[10]:

response = subprocess.check_output(command,shell=True)


# In[11]:

get_ipython().system('gsutil -m cp -r {ec2_output_path} {GCS_OUTPUT_PATH}')


# In[ ]:

end = datetime.datetime.now()
elapsed = end - start
print(elapsed)


# Previous Runs:  
# 
