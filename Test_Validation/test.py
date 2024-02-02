
import json
with open("/Users/harish/PycharmProjects/Data_validation_tool/Config/config.json", 'r') as f:
    config_data = json.loads(f.read())['oracle_db']
print(config_data)

from Utility.Database_Read_Functions import db_read
from Utility.read_data import read_data
from Utility.General_Purpose_Functions import count_validation,duplicate , \
     Null_value_check,Uniquess_check,records_present_only_in_source,\
     records_present_only_in_target, data_compare

from pyspark.sql import SparkSession
import pandas as pd
import json
import openpyxl
from pyspark.sql.functions import collect_set
from pyspark.sql import SparkSession
import pandas as pd
import json

from pyspark.sql.types import StructType


spark = SparkSession.builder.master("local").appName("test_execution")\
     .config("spark.jars", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.driver.extraClassPath", "/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .config("spark.executor.extraClassPath","/Users/harish/Downloads/spark-3.4.1-bin-hadoop3/jars/ojdbc8-21.5.0.0.jar") \
     .getOrCreate()


source = spark.read.option("header", True).csv('/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/Contact_info.csv')

source.printSchema()

with open("/Users/harish/PycharmProjects/Data_Automation_project/schem/contact_info_schema.json", 'r') as f:
    schema = StructType.fromJson(json.load(f))

print(schema)

source_with_ext_schema = spark.read.schema(schema).option("header", True).csv('/Users/harish/PycharmProjects/Data_validation_tool/Source_Files/Contact_info.csv')
source_with_ext_schema.printSchema()


dataframe= spark.read.format('csv').option("header", True).load("dbfs:/FileStore/shared_uploads/kattubadinovember2023@gmail.com/SourceFiles/Contact_info_t.csv")
Null_columns = "identifier, surname"
def Null_value_check(dataframe, Null_columns,Out):
    target_count = dataframe.count()
    print(Null_columns)
    Null_columns = Null_columns.split(",")
    print(Null_columns)
    for column in Null_columns:
        Null_df = dataframe.select(count(when(col(column).contains('None') | \
                                        col(column).contains('NULL') | \
                                        col(column).contains('Null') | \
                                        (col(column) == '') | \
                                        col(column).isNull() | \
                                        isnan(column), column
                                        )).alias("Null_value_count"))
        # dataframe.createOrReplaceTempView("dataframe")
        # Null_df = spark.sql(f"select count(*) source_cnt from dataframe where {column} is null")
        cnt = Null_df.collect()
        print(cnt)

        if cnt[0]['Null_value_count']>0:
            print(f"{column} columns has Null values")
            Null_df.show(10)
            #write_output(4, "Null_value_check", "NA", target_count, "fail", cnt[0][0], Out)
        else:
            print("No null records present")
            #write_output(4, "Null_value_check", "NA", target_count, "pass", 0, Out)