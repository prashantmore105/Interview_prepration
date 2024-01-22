# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 12:04:30 2023

@author: prashantm5
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count
from pyspark.sql.functions import explode
# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
      
print(spark)  
#df=spark.read.option("header",True).csv("C:/Users/prashantm5.MUMBAI1/Downloads/dataframequestion.csv")
arrayData = [
        ('James',['Java','Scala'],{'hair':'black','eye':'brown'}),
        ('Michael',['Spark','Java',None],{'hair':'brown','eye':None}),
        ('Robert',['CSharp',''],{'hair':'red','eye':''}),
        ('Washington',None,None),
        ('Jefferson',['1','2'],{})]

#df = spark.createDataFrame(data=arrayData, schema = ['name','knownLanguages','properties'])
#df.printSchema()
#df.show()
#df1=df.select("name",explode("knownLanguages"))

#df1.show()

data=[('a',[1,2] ),('b',[3,4,5]),('c',[6])]
df = spark.createDataFrame(data=data, schema = ['c1','c2'])
df.printSchema()
df.show()

def constr(s):
	val = s.split('|')
	return val
df1 = df.select("c1", explode("c2"))
#df1=df.select(df.c1,explode(df.c2))
df1.show()
#df1=df.select(col(c1), explode(col(c2)))
#df1 = df.select(df.c1, explode((df.c2)))
#df = df.select(df.c1, explode(df.c2).split('|'))
#df1 = df.select(col(c1), explode(col(c2).split('|')))

#df1.show()