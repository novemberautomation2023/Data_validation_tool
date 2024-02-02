
import logging
import os
import json

import pkg_resources

logging.basicConfig(filename="newfile.log",
                    level=logging.INFO, #NDIWEC
                    filemode='w',
                    format='%(asctime)s:%(levelname)s:%(message)s')
logger = logging.getLogger()

def read_data(format,path,spark, delimiter=None, multiline=None, sql_path=None,database=None):
    if format.lower() == 'csv':
        if delimiter is None:
            df = spark.read.option("header", True).option("delimiter",",").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)
        elif delimiter=='|':
            df = spark.read.option("header", True).option("delimiter", "|").csv(path)
            logger.info("CSV file has read successfully from the below path" + path)

    elif format.lower() == 'json':
        if multiline is None:
            df = spark.read.json(path)
            logger.info("Json file has read successfully from the below path" + path)

        elif multiline == True:
            df = spark.read.option("multiline", True).json(path)
            logger.info("Json file has read successfully from the below path" + path)

    elif format.lower() == 'parquet':
        df = spark.read.parquet(path)
        logger.info("parquet file has read successfully from the below path" + path)

    elif format.lower() == 'avro':
        df = spark.read.avro(path)
        logger.info("Avro file has read successfully from the below path" + path)

    elif format.lower() == 'table':
        conf_file_path = pkg_resources.resource_filename('Config', 'config.json')
        with open(conf_file_path,'r') as f:
            config_data = json.loads(f.read())[database]
        if sql_path is not None:
            sql_path = pkg_resources.resource_filename('Transformations_queries', sql_path)
            with open(sql_path, "r") as file:
                sql_query = file.read()
        print(sql_query)
        print(config_data)
        df = spark.read.format("jdbc"). \
            option("url", config_data['url']). \
            option("user", config_data['user']). \
            option("password", config_data['password']). \
            option("query", sql_query). \
            option("driver", config_data['driver']).load()
    else:
        logger.critical("File format is not found ")
    return df














