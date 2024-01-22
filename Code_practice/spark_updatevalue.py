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
simpleData = [(1,)]
schema = ["value"]
df = spark.createDataFrame(data=simpleData, schema = schema)
df.show()
df1=df.withColumn('value', lit(2))
df1.show()
                
        