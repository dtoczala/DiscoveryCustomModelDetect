#!/usr/bin/env python
# coding: utf-8

# # Notebook for finding Custom Models Applied to Discovery collections on the IBM Cloud
# 
# **NOTE:**
# This notebook was initially based upon a Python notebook provided by Dan Toczala.
# 
# Further work to extend some of the concepts was done by the folowing contributors:
# 
# D. Toczala (dtoczala@us.ibm.com)
# 

# In[1]:


#########################################
##########                     ##########
##########  NOTEBOOK BLOCK 1   ##########
##########                     ##########
#########################################

# Only run this cell if you don't have watson_developer_cloud installed
# You can specify the latest verion of watson_developer_cloud (1.0.0 as of November 20, 2017)
get_ipython().system('pip install --upgrade ibm-watson>=3.0.3')
get_ipython().system('pip install --upgrade watson-developer-cloud>=2.10.0')


# In[2]:


#########################################
##########  NOTEBOOK BLOCK 2   ##########
#########################################

#Import utilities
import json
import sys
import codecs
import re
import time
import requests
import os
import re
from os.path import join, dirname
from datetime import datetime
from datetime import timedelta
import pandas as pd
import numpy as np
from botocore.client import Config
import ibm_boto3
from ibm_watson import DiscoveryV1
from ibm_watson import IAMTokenManager
#TOXTOXTOX
#from watson_developer_cloud import DiscoveryV1
#from watson_developer_cloud import IAMTokenManager
#
import ibm_db
from ibm_db import fetch_assoc


# In[3]:


#########################################
##########  NOTEBOOK BLOCK 3   ##########
#########################################

#DEBUG = True
DEBUG = False
#DEV_ENVIRONMENT=False
DEV_ENVIRONMENT=True
#
UNDERSCORE = '_'
QUOTE = '"'
#
# Set Initial IBM Cloud and COS parameters
credentials = {
#    'IAM_URL': 'https://iam.bluemix.net/identity/token',
    'IAM_URL': 'https://iam.cloud.ibm.com/identity/token',
# Discovery parameters
    'DISCOVERY_URL': 'https://gateway.watsonplatform.net/discovery/api',
    'DISCOVERY_APIKEY': 'xXsZTxxxxx4hMiMgxxxxxxJQ-xxxuEkxxxxxNxfixxxU',
    'DISCOVERY_ENVIRONMENT': '6xxxxxxb-fxx4-4xx2-axxc-xxxbexxxxxfx',
    'SOME_COLLECTION': '0xxxxxx1-8xx8-4xxf-axxb-3xxxcb4xxxx9'
}
#
# Get current date and time
#
myDatetime = datetime.now()
CURRENT_MONTH = datetime.now().strftime('%m')
CURRENT_YEAR  = datetime.now().strftime('%Y')
CURRENT_DAY = datetime.now().strftime('%d')

if (DEBUG):
    print (myDatetime, "   Month - ", CURRENT_MONTH, "   Year - ", CURRENT_YEAR)
myDatetime = re.sub(r'\s',UNDERSCORE,str(myDatetime))
goodDatetime,junk = myDatetime.split('.')


# # Notebook Specific Code for testing

# In[4]:


#########################################
##########  NOTEBOOK BLOCK 4   ##########
#########################################
#
# Build a unique doc ID
#
def getDocId(filename):
    #
    # Add a GUID to the end
    #
    guid = str(datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
#    filename = filename+guid
    #
    # Build a unique Doc id
    #
    docId = re.sub('\s+','-',filename)
    docId = re.sub('\.+','-',docId)    
    docId = re.sub('\'+','-',docId)
    docId = re.sub('\:+','-',docId)
    #
    # end of loop to get files
    #
    return docId

#
# Given a pointer to an Discovery instance and a collection ID, ingest document, and get back JSON result
#
def ingestPDFFile(discovery,environment_id,collection_id,filename,processor):
    MAX_ATTEMPTS = 3
    tries = 0
    response = ""
    #
    # Build a unique Doc id
    #
    docId = getDocId(filename)
    #
    # Build metadata object
    #
    myTimestamp = str(datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ'))
    if (DEBUG):
        print ("Timestamp - "+myTimestamp)
    #
    metaObj = {
#        "timestamp": str(datetime.now().strftime('%Y-%m-%d %H:%M:%S')),
        "ingest_timestamp": myTimestamp,
        "swiped_amount" : 0.0,
        "processor":processor
        }
    metaStr = json.dumps(metaObj)
    #
    try:
        with open(os.path.join(os.getcwd(), './', filename), 'rb') as fileObj:
            # reset cursor to beginning of file
            fileObj.seek(0)
            response = discovery.update_document(environment_id,
                                                 collection_id,
                                                 docId,
                                                 file=fileObj,
                                                 filename=filename,
                                                 file_content_type='application/pdf',
                                                 metadata=metaStr,
                                                ).get_result()
        # end of WITH
        print ("Ingest of "+filename)
        if (DEBUG):
            print(json.dumps(response, indent=2))
    except:
        print(filename+" was not found. Response was - "+str(response))
        response = ""
        
    return response

#
# Given a pointer to an Discovery instance and a collection ID, ingest document, and get back JSON result
#
def ingestPDFFileAndReturnContents(discovery,environment_id,collection_id,filename):
    MAX_ATTEMPTS = 60
    tries = 0
    #
    # Build a unique Doc id
    #
    docId = getDocId(filename)
    #
    try:
        with open(os.path.join(os.getcwd(), './', filename), 'rb') as fileObj:
            # reset cursor to beginning of file
            fileObj.seek(0)
            response = discovery.update_document(environment_id,
                                                 collection_id,
                                                 docId,
                                                 file=fileObj,
                                                 filename=filename,
                                                 file_content_type='application/pdf',
                                                ).get_result()
            # end of WITH
        #    
        docID=response['document_id']
        filter_str='id::'+docID
        #
        # Check the status of the collection - wait until it is ready
        #
        tries = 0
        #
        while True:
            try:
                response = discovery.get_document_status(environment_id=environment_id,
                                                         collection_id=collection_id,
                                                         document_id=docID
                                                        ).get_result()
                sys.stdout.write('.')
                if (response['status']!='available'):
                    tries += 1
                    if tries < MAX_ATTEMPTS:
                        time.sleep(1)
                        continue
                    break
                break
            except:
                if tries < MAX_ATTEMPTS:
                    tries += 1
                    time.sleep(1)
                    continue
                break
            break        
        # end of try
        #
        # Now query the doc and get back the JSON representation of it
        #
        tries = 0
        #
        while True:
            try:
                response = discovery.query(environment_id=environment_id,
                                           collection_id=collection_id,
                                           filter=filter_str
                                          ).get_result()
            except:
                if tries < MAX_ATTEMPTS:
                    tries += 10
                    time.sleep(1)
                    continue
                break
            break        
        # end of WHILE
    
    except:
        print(filename+" was not found.")
        response = ""
        
    return response

#
# Pull al ist of all occurances of a "key" value from a JSON object
# regardless of where the key value is
#

def extract_JSON_values(obj, key):
    """Pull all values of specified key from nested JSON."""
    arr = []

    def extract(obj, arr, key):
        """Recursively search for values of key in JSON tree."""
        if isinstance(obj, dict):
            for k, v in obj.items():
                if isinstance(v, (dict, list)):
                    extract(v, arr, key)
                elif k == key:
                    arr.append(v)
        elif isinstance(obj, list):
            for item in obj:
                extract(item, arr, key)
        return arr

    results = extract(obj, arr, key)
    return results


# # NOTEBOOK ONLY CODE - Go and grab all of your data
# 
# First pull down all of the PDF files from the COS bucket.
# 

# In[5]:


#########################################
##########  NOTEBOOK BLOCK 60  ##########
#########################################

#DEBUG = True
DEBUG=False
#
# Set the default Discovery instance settings
#
discoveryVersion='2019-03-25'
dicoveryUrl=credentials['DISCOVERY_URL']
discoveryIAMApiKey=credentials['DISCOVERY_APIKEY']
discoveryIAMUrl=credentials['IAM_URL']
discovery_env = credentials['DISCOVERY_ENVIRONMENT']
some_collection = credentials['SOME_COLLECTION']
#
# Create an object for your Discovery instance
# Make sure to update the version to match your WDS workspace
#
fileDiscovery = DiscoveryV1(version='2019-03-25',
                        url=dicoveryUrl,
                        iam_apikey=discoveryIAMApiKey
                        )
#
# Now try grabbing your environment
#
environments = fileDiscovery.list_environments().get_result()
if (DEBUG):
    print(json.dumps(environments,indent=2))
    
#
# Now try grabbing your collections in a list
#
collections = fileDiscovery.list_collections(
    environment_id=discovery_env).get_result()
if (DEBUG):
    print(json.dumps(collections,indent=2))    
    
#
# Now try grabbing your configurations
#
configurations = fileDiscovery.list_configurations(
    environment_id=discovery_env).get_result()
if (DEBUG):
    print(json.dumps(configurations,indent=2))

#
# Loop through erach configuration and get details about it
#
for configuration in configurations['configurations']:
    #
    # Grab configuration details
    #
    config_id = configuration['configuration_id']
    config_name = configuration['name']
    config_details = fileDiscovery.get_configuration(
        environment_id=discovery_env,
        configuration_id=config_id).get_result()
    #
    if (DEBUG):
        print("\nCONFIGURATION")
        print(json.dumps(config_details,indent=2))
    #
    # Grab enrichments (if any)
    #
    try:
        config_enrichments = config_details['enrichments']
    except:
        config_enrichments = []
    #
    # Loop thru the enrichments
    #
    for enrichment in config_enrichments:
        #
        # Grab models (if any)
        #
        config_models = extract_JSON_values(enrichment,'model')
        #
        #
        #
        for config_model in config_models:
            #
            # Find the matching Collection in the collection array
            #
            for collection in collections['collections']:
                #
                # Get the collection id, colection name, and configuration id
                #
                collection_id = collection['collection_id']
                collection_name = collection['name']
                coll_config_id = collection['configuration_id']
                #
                # Did we match up the configuration ID - if so, we found a collection with a custom model
                #
                if ((coll_config_id == coll_config_id) and (config_model != 'contract')):
                    print ("******")
                    print (config_name," with Config id - ",coll_config_id,"   Model - ",config_model)
                    print ("Collection ",collection_name,"  with Collection ID ",collection_id)
                    print (" ")

#
# All done?
#


# In[ ]:




