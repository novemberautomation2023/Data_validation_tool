import pkg_resources

from Utility.Database_Read_Functions import db_read
from Utility.read_data import read_data
from Utility.General_Purpose_Functions import *

from pyspark.sql import SparkSession
import pandas as pd
import json
import openpyxl
from pyspark.sql.functions import collect_set


spark = SparkSession.builder.master("local").appName("test_execution").getOrCreate()


template_path = pkg_resources.resource_filename('Config', 'Master_Test_Template.xlsx')

Test_cases = pd.read_excel(template_path)
run_test_case = Test_cases.loc[(Test_cases.execution_ind=='Y')]

print(run_test_case)

print(run_test_case.columns)

df = spark.createDataFrame(run_test_case)

validations = df.groupBy('source', 'source_type',
       'source_db_name', 'source_transformation_query_path', 'target',
       'target_type', 'target_db_name', 'target_transformation_query_path',
       'key_col_list', 'null_col_list', 'unique_col_list').agg(collect_set('validation_Type').alias('validation_Type'))

validations.show(truncate=False)

validations = validations.collect()

print(validations)

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


for row in validations:
    print(row['source'])
    if row['source_type'] == 'table':
        source = read_data(row['source_type'], row['source'], spark=spark, database=row['target_db_name'],sql_path=row['target_transformation_query_path'])
    else:
        source_path = pkg_resources.resource_filename('Source_Files', row['source'])
        source = read_data(row['source_type'], source_path, spark)

    if row['target_type'] == 'table':
        print(row['target_type'], row['target'], row['target_db_name'])
        target = read_data(row['target_type'], row['target'], spark=spark, database=row['target_db_name'],sql_path=row['target_transformation_query_path'])
    else:
        target_path = pkg_resources.resource_filename('Source_Files', row['source'])
        target = read_data(row['target_type'], target_path, spark)
    source.show(n=2)
    target.show(n=2)
    for validation in row['validation_Type']:
        print(validation)
        if validation == 'count_validation':
            count_validation(source, target, Out)
        elif validation == 'duplicate':
            duplicate(target,row['key_col_list'], Out)
        elif validation == 'Null_value_check':
            Null_value_check(target, row['null_col_list'], Out)
        elif validation == 'Uniquess_check':
            Uniquess_check(target, row['unique_col_list'], Out)

        elif validation == 'records_present_only_in_source':
            records_present_only_in_source(source, target, row['key_col_list'], Out)

        elif validation == 'records_present_only_in_target':
            records_present_only_in_target(source, target, row['key_col_list'], Out)

        elif validation == 'data_compare':
            data_compare(source, target, row['key_col_list'], Out)



df = pd.DataFrame(Out)

df.to_csv("summary.csv")

spark.createDataFrame(df).show()