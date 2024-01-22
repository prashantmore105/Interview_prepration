# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 17:06:13 2023

@author: prashantm5
"""

import findspark
findspark.init()
import os
import sys
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
from pyspark.sql.functions import sum,avg,max
from pyspark.sql.functions import *
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
print(spark)
simpleData = [("shiva",),("Prachuraa",)]
schema = ["name"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.show()
vowlel_list=['a','e','i','o','u']
spark.sql('create table test_1 (name string,count integer)')
for i in df.collect():
    print("name is ========")
    print(i['name'])
    str =i['name']
    count=0
    for letter in str :
        for vow in vowlel_list:
            if letter ==vow :
                count+=1
                #print(count)
            df1=spark.sql('insert into test1 values (str ,count)')
    print("this is final count for "+str)
    print("count is ")
    print(count)
    #df1=df.withColumn('count', lit(count)) 
    df1.show()          
                
        