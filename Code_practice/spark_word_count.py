# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 12:00:12 2023

@author: prashantm5
"""
from pyspark.sql import SparkSession ,SparkContext
from pyspark.sql.functions import col,isnan, when, count
conf = SparkConf() #Declare spark conf variable\
conf.setAppName("Read-and-write-data-to-Hive-table-spark")
sc = SparkContext.getOrCreate(conf=conf)
rdd = sc.textFile('C:/Users/prashantm5.MUMBAI1/Downloads/test.text')
words = rdd.flatmap(lambda line: line.split(' '))
print(words)
#wordcount = words.map(word: (word,1)).reduceByKey(lambda a,b:a+b)
#wordcount.saveAsTextFile('path')