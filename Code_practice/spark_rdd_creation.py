# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 12:20:03 2023

@author: prashantm5
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import expr
#Create spark session
# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
      
print(spark)  


dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
rdd=spark.sparkContext.parallelize(dataList)  
print(rdd)
dataColl=rdd.collect()
print(dataColl)
#rdd.take(2).foreach(println())
#data = [("Banana",1000,"USA"), ("Carrots",1500,"USA"), ("Beans",1600,"USA"), \
#      ("Orange",2000,"USA"),("Orange",2000,"USA"),("Banana",400,"China"), \
#      ("Carrots",1200,"China"),("Beans",1500,"China"),("Orange",4000,"China"), \
#      ("Banana",2000,"Canada"),("Carrots",2000,"Canada"),("Beans",2000,"Mexico")]

#columns= ["Product","Amount","Country"]
#df = spark.createDataFrame(data = data, schema = columns)
#df.printSchema()
#df.show(truncate=False)