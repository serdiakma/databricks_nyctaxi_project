# Databricks notebook source
# importing prerequisite libraries
import urllib.request
import shutil
import os
import json
import pandas as pd

# COMMAND ----------

# Target URL of the public csv file to download
url = 'https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv'

# Open a connection to the remote URL and fetch the Parquet file as a stream
response = urllib.request.urlopen(url)

# Create the destination directory for storing the downloaded parquet file
dir_path = '/Volumes/nyctaxi/00_landing/data_sources/lookup'
os.makedirs(dir_path, exist_ok=True)

# Define the full local path (including filename) where the file will be saved
local_path = "/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv"

# Write the contens of the response stream to the specified local file path
with open(local_path, 'wb') as f:
    shutil.copyfileobj(response, f)