# from Utility.General_Purpose_Functions import count_validation,duplicate ,Null_value_check,Uniquess_check,records_present_only_in_source,records_present_only_in_target, data_compare
# from Utility.Database_Read_Functions import db_read
import datetime
import json
import os
import sys
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode
from pyspark.sql import SparkSession
#from Utility.File_Read_functions import read_file
#from Utility.Database_Read_Functions import db_read
from pyspark.sql.functions import abs,count, when, isnan, isnull, col, trim
import datetime
import json
import sys

import pandas as pd

from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import Window
from pyspark.sql import functions as F
from pyspark.sql.functions import explode_outer, concat, col, \
    trim,to_date, lpad, lit, count,max, min, explode
from pyspark.sql.types import IntegerType
import os
import smtplib
from itertools import chain
from string import Template
from pyspark.sql.column import Column
from pyspark.sql.functions import create_map, isnull, col, when, lit, abs
from pyspark.sql import types as t
from subprocess import PIPE, Popen
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication





def db_read(url,username, password,query,driver,spark):
    df= spark.read.format("jdbc"). \
        option("url", url). \
        option("password", password). \
        option("user", username). \
        option("query", query). \
        option("driver", driver).load()
    return df

def db_write(df,url,username, password,table,driver,spark):
    df.write.mode("overwrite") \
        .format("jdbc") \
        .option("url", url) \
        .option("driver", driver) \
        .option("dbtable", table) \
        .option("user", username) \
        .option("password", password) \
        .save()

def kafka_read(spark):
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "host1:port1,host2:port2") \
        .option("subscribe", "topic1") \
        .option("includeHeaders", "true") \
        .load()
    return df













def count_validation(sourceDF, targetDF,Out):
    source_count = sourceDF.count()
    target_count = targetDF.count()
    if source_count == target_count:
        print("Source count and target count is matching and count is", source_count)
        write_output(1,"Count_validation",source_count,target_count,"pass", 0,Out)
    else:
        print("Source count and taget count is not matching and difference is",source_count-target_count)
        write_output(1, "Count_validation", source_count, target_count, "fail", source_count - target_count, Out)


def duplicate(dataframe, key_column,Out):
    dup_df = dataframe.groupBy(key_column).count().filter('count>1')
    target_count = dataframe.count()
    if dup_df.count()>0:
        print("Duplicates present")
        dup_df.show(10)
        write_output(2, "duplicate", "NA", target_count, "Fail", dup_df.count(), Out)
    else:
        print("No duplicates")
        write_output(2, "duplicate", "NA", target_count, "pass", 0, Out)


def Uniquess_check(dataframe, unique_column,Out):
    target_count = dataframe.count()
    for column in unique_column:
        dup_df = dataframe.groupBy(column).count().filter('count>1')
        if dup_df.count()>0:
            print(f"{column} columns has duplicate")
            dup_df.show(10)
            write_output(3, "Uniqueness", "NA", target_count, "Fail", dup_df.count(), Out)
        else:
            print("All records has unique records")
            write_output(3, "Uniqueness", "NA", target_count, "Pass", 0, Out)

def Null_value_check(dataframe, Null_columns,Out):
    target_count = dataframe.count()
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

        if cnt[0][0]>=1:
            print(f"{column} columns has Null values")
            Null_df.show(10)
            write_output(4, "Null_value_check", "NA", target_count, "fail", cnt[0][0], Out)


        else:
            print("No null records present")
            write_output(4, "Null_value_check", "NA", target_count, "pass", 0, Out)




def records_present_only_in_target(source,target,keyList,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("SourceCount is null").count()
    print("Key column record present in target but not in Source :" + str(count))
    source_count =source.count()
    target_count = target.count()
    if count > 0:
        count_compare.filter("SourceCount is null").show()
        write_output(5, "records_present_only_in_target", source_count, target_count, "fail", source_count - target_count, Out)
    else:
        print("No extra records present in source")
        write_output(5, "records_present_only_in_target", source_count, target_count, "Pass", 0, Out)


def records_present_only_in_source(source,target,keyList,Out):
    srctemp = source.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "SourceCount")
    tartemp = target.select(keyList).groupBy(keyList).count().withColumnRenamed("count", "TargetCount")
    count_compare = srctemp.join(tartemp, keyList, how='full_outer')
    count = count_compare.filter("TargetCount is null").count()
    source_count = source.count()
    target_count = target.count()
    print("Key column record present in Source but not in target :" + str(count))
    if count > 0:
        count_compare.filter("TargetCount is null").show()
        write_output(6, "records_present_only_in_source", source_count, target_count, "fail",
                     source_count - target_count, Out)

    else:
        print("No extra records present")
        write_output(6, "records_present_only_in_target", source_count, target_count, "Pass", 0, Out)


def data_compare( source, target,keycolumn,Out):
    for colname in source.columns:
        source = source.withColumn(colname, trim(col(colname)))

    for colname in target.columns:
        target = target.withColumn(colname, trim(col(colname)))
    columnList = source.columns
    for column in columnList:
        if column not in keycolumn:
            temp_source= source.select(keycolumn, column).withColumnRenamed(column,"source_"+column)
            temp_target=target.select(keycolumn, column).withColumnRenamed(column,"target_"+column)
            temp_join = temp_source.join(temp_target,keycolumn,how='full_outer')
            temp_join.withColumn("comparison", when(col('source_'+column) == col("target_"+column),\
                                                    "True" ).otherwise("False")).show()

def compare(source, target,countQA, keyList,Out):
    sourceDaraFrame = source
    targetDataFrame = target
    for colname in sourceDaraFrame.columns:
        sourceDaraFrame = sourceDaraFrame.withColumn(colname, trim(col(colname)))

    for colname in targetDataFrame.columns:
        targetDataFrame = targetDataFrame.withColumn(colname, trim(col(colname)))
    match_stats = []
    sampleCount = 10
    columnList = sourceDaraFrame.columns
    subStringMap = {}
    Summary = {"Column": [], "Total": [], "Matchcount": [], "Mismatchcount": [], "Mismatchcountper": []}
    report = "Column_Name" + "\t" + "Match_count" + "\t" + "mismatch_count" + "\t" + "Mismatch_percentage"
    for column in columnList:
        try:
            subString = subStringMap.__getitem__(column)
        except:
            subString = ''
        if column not in keyList:
            matchcount, a, b = run_compare_for_column(keyList, column, sourceDaraFrame, targetDataFrame, sampleCount,
                                                      subString)
            mismatchcount = countQA - matchcount
            mismatchcountper = mismatchcount * 100 / float(countQA)
            Summary['Column'].append(column)
            Summary['Total'].append(countQA)
            Summary['Matchcount'].append(matchcount)
            Summary['Mismatchcount'].append(mismatchcount)
            Summary['Mismatchcountper'].append(mismatchcountper)
    Summary = pd.DataFrame(Summary)
    if Summary.Mismatchcount.sum() > 0:
        write_output(5,"Datavalidation", countQA, countQA,"FAIL",Summary.Mismatchcount.sum(),Out)
    else:
        write_output(5, "Datavalidation", countQA, countQA, "PASS", 0, Out)
    return Summary

def get_dataset (keyList, keyDict, dataframe):
    var_dict = {}
    condition = ''
    #print keyDict
    i=0
    for key, val in keyDict.items():
        if i > 0:
            condition = str(key) + " == '" + str(val) + "' and " + condition
            #print("Condition inside if " , condition)
        else:
            condition = str(key) + " == '" + str(val) + "'"
            #print("Condition inside elif " ,condition)
        i = i + 1
    var_dict.__setitem__('condition', condition)
    var_dict.__setitem__('dataframe', [k for k,v in locals().items() if v == dataframe][0])
    command = '''$dataframe.filter("$condition").show(20, False)'''
    #print(command)
    command = Template(command).substitute(var_dict)
    #print(command)
    eval(command)
    print("\n\n")

def run_compare_for_column(keyList, column, sourceDataFrame, targetDataFrame, sampleCount, substring, tolerance=None):
    print("Validation for column - " + column )
    var_dict = {}
    var_dict.__setitem__('keyList', keyList)
    var_dict.__setitem__('column', column)
    var_dict.__setitem__('sourceDataFrame', [k for k, v in locals().items() if v == sourceDataFrame][0])
    var_dict.__setitem__('targetDataFrame', [k for k, v in locals().items() if v == targetDataFrame][0])
    var_dict.__setitem__('substring', substring)
    var_dict.__setitem__('samplecount', sampleCount)
    var_dict.__setitem__('tolerance', tolerance)
    if tolerance is None:
        command = '''$sourceDataFrame.join($targetDataFrame, $keyList, how="fullouter").filter((($sourceDataFrame.$column.isNotNull()) &
         ($targetDataFrame.$column.isNotNull()) | ($sourceDataFrame.$column.isNull()) & ($targetDataFrame.$column.isNull()))).select("''' + ('" , "').join(keyList) + '''", $sourceDataFrame.$column, $targetDataFrame.$column,
         F.when(trim($sourceDataFrame.$column$substring) == trim($targetDataFrame.$column$substring),"True").
         otherwise("False").alias('Diff_$column')).filter("Diff_$column == False").count()'''

    else:
        command = '''$sourceDataFrame.join($targetDataFrame, $keyList, how="fullouter").filter((($sourceDataFrame.$column.isNotNull()) &
         ($targetDataFrame.$column.isNotNull()) | ($sourceDataFrame.$column.isNull()) & ($targetDataFrame.$column.isNull()))).select("''' + ('" , "').join(keyList) + '''", $sourceDataFrame.$column, $targetDataFrame.$column,
         F.when(trim($sourceDataFrame.$column$substring) == trim($targetDataFrame.$column$substring),"True").
         otherwise("False").alias('Diff_$column')).count()'''
    command = Template(command).substitute(var_dict)
    count = eval(command)
    Mismatchcount = count
    print("Data is not matching for "+str(Mismatchcount) + " records" + "\n")
    if count > 0:
        if tolerance is None:
            command = '''$sourceDataFrame.join($targetDataFrame, $keyList, how="fullouter").filter((($sourceDataFrame.$column.isNotNull()) &
         ($targetDataFrame.$column.isNotNull()) | ($sourceDataFrame.$column.isNull()) & ($targetDataFrame.$column.isNull()))).select("''' + ('" , "').join(keyList) + '''", $sourceDataFrame.$column, $targetDataFrame.$column,
         F.when(trim($sourceDataFrame.$column$substring) == trim($targetDataFrame.$column$substring),"True").
         otherwise("False").alias('Diff_$column')).filter("Diff_$column == False")'''
        else:
            command = '''$sourceDataFrame.join($targetDataFrame, $keyList, how="fullouter").filter((($sourceDataFrame.$column.isNotNull()) &
                     ($targetDataFrame.$column.isNotNull()) | ($sourceDataFrame.$column.isNull()) & ($targetDataFrame.$column.isNull()))).select("''' + (
                '" , "').join(keyList) + '''", $sourceDataFrame.$column, $targetDataFrame.$column,
                     F.when(trim($sourceDataFrame.$column$substring) == trim($targetDataFrame.$column$substring),"True").
                     otherwise("False").alias('Diff_$column))'''
        command = Template(command).substitute(var_dict)
        sampleData= eval(command)
        #sampleData.columns=[[sampleData.columns[0],'Source','Target','dfii']]
        sampleData.show(10)
        #sampleData.show(sampleCount, False)
        print("Sample mismatch records " + ",".join(keyList) )
        print('-----------------------------------------------')
        keyListdata = sampleData.select(keyList).first().asDict()
        print('Source dataFrame details')
        get_dataset(keyList,keyListdata, sourceDataFrame)
        print("Target dataFrame details")
        get_dataset(keyList, keyListdata, targetDataFrame)

    if tolerance is None:
        command = '''$sourceDataFrame.join($targetDataFrame, $keyList, how="fullouter").filter((($sourceDataFrame.$column.isNotNull()) & 
        ($targetDataFrame.$column.isNotNull()) | ($sourceDataFrame.$column.isNull()) & ($targetDataFrame.$column.isNull()))).select("''' + ('" , "').join(keyList) + '''", $sourceDataFrame.$column, $targetDataFrame.$column,F.when(trim($sourceDataFrame.$column$substring) == trim($targetDataFrame.$column$substring),"True").
        otherwise("False").alias('Diff_$column')).filter("Diff_$column == True").count()'''
        command = Template(command).substitute(var_dict)
        #print(command)
        count = eval(command)
        matched_count = count
        #print("Data is not exactly matching for " + str(matched_count))
    return matched_count,Mismatchcount, matched_count+Mismatchcount

def write_output(TC_ID,Test_Case_Name,Number_of_source_Records,Number_of_target_Records,Status,Number_of_failed_Records,Out):
    Out["TC_ID"].append(TC_ID)
    Out["test_Case_Name"].append(Test_Case_Name)
    Out["Number_of_source_Records"].append(Number_of_source_Records)
    Out["Number_of_target_Records"].append(Number_of_target_Records)
    Out["Status"].append(Status)
    Out["Number_of_failed_Records"].append(Number_of_failed_Records)


















#Spark session creation
spark = SparkSession.builder \
    .master("local") \
    .config("spark.jars", 'jars/hadoop-azure-3.3.6.jar') \
    .config("spark.jars", 'jars/ojdbc11.jar') \
    .getOrCreate()

with open('Config/config.json','r') as f:
    config_file_data = json.loads(f.read())

Out = {"TC_ID":[], "test_Case_Name":[], "Number_of_source_Records":[], "Number_of_target_Records":[], "Number_of_failed_Records":[],"Status":[]}
schema= ["TC_ID", "test_Case_Name", "Number_of_source_Records", "Number_of_target_Records", "Number_of_failed_Records","Status"]


print(config_file_data['contact_info'])

path= config_file_data['contact_info']['source_file']
format = config_file_data['contact_info']['source_file_type']
db_Address = config_file_data['contact_info']['db_Address']
db_port= config_file_data['contact_info']['db_Port']
db_Username= config_file_data['contact_info']['db_Username']
db_Password= config_file_data['contact_info']['db_Password']
db_driver= config_file_data['contact_info']['db_driver']
raw_query = config_file_data['contact_info']['raw_query']
bronze_query = config_file_data['contact_info']['bronze_query']
silver_query= config_file_data['contact_info']['silver_query']
Key_column = config_file_data['contact_info']['Key_column']

#Reading source1

Raw= db_read(db_Address,db_Username,db_Password,raw_query,db_driver,spark)
contact_info_bronze_actual= db_read(db_Address,db_Username,db_Password,bronze_query,db_driver,spark)

Raw.createOrReplaceTempView("Raw")

contact_info_bronze_expected = spark.sql(
    """ select
    cast(Identifier as decimal(10)) Identifier,
    upper(Surname) Surname,
    upper(given_name) given_name,
    upper(middle_initial) middle_initial,
    suffix,
    Primary_street_number,
    primary_street_name,
    city,
    state,
    zipcode,
    Primary_street_number_prev,
    primary_street_name_prev,
    city_prev,
    state_prev,
    zipcode_prev,
    translate(Email,'FUCK','****') email,
    translate(Phone,'+-','') phone,
    rpad(birthmonth,8,'0') birthmonth
    from raw
    """
)

#
# count_validation(contact_info_bronze_expected,contact_info_bronze_actual,Out=Out)
# duplicate(contact_info_bronze_actual,Key_column,Out=Out)
# records_present_only_in_source(contact_info_bronze_expected,contact_info_bronze_actual,Key_column,Out)
# records_present_only_in_target(contact_info_bronze_expected,contact_info_bronze_actual,Key_column,Out)
# Null_value_check(contact_info_bronze_actual,Key_column,Out)
# Uniquess_check(contact_info_bronze_actual,Key_column,Out)
# data_compare(contact_info_bronze_expected,contact_info_bronze_actual,'Identifier',Out)
# Summary = pd.DataFrame(Out)
#
#
# Summary = spark.createDataFrame(Summary)
# Summary.show()
# Summary.write.csv("Output/Summary", mode='overwrite', header="True")
#
# Summary.write.mode("overwrite") \
#     .format("jdbc") \
#     .option("url", "jdbc:oracle:thin:@//localhost:1521/freepdb1") \
#     .option("driver", "oracle.jdbc.driver.OracleDriver") \
#     .option("dbtable", "contact_info_raw") \
#     .option("user", "scott") \
#     .option("password", "tiger") \
#     .save()