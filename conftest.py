import subprocess

import os
import json
from pyspark.sql import SparkSession

import sys
project_path = os.path.dirname(__file__)

with open(project_path+'/Config/config.json','r') as f:
    config_file_data = json.loads(f.read())

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]

#
spark = SparkSession.builder \
    .master("local") \
    .config("spark.jars", '/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/hadoop-azure-3.3.6.jar') \
    .getOrCreate()