# -*- coding: utf-8 -*-
"""
Created on Mon Dec 18 11:29:44 2023

@author: prashantm5
"""

from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
 
#Main module to execute spark code
if __name__ == '__main__':
    conf = SparkConf() #Declare spark conf variable\
    conf.setAppName("Read-and-write-data-to-Hive-table-spark")
    sc = SparkContext.getOrCreate(conf=conf)
 
    #Instantiate hive context class to get access to the hive sql method  
    hc = HiveContext(sc)
 
    #Read hive table in spark using .sql method of hivecontext class
    df = hc.sql("select * from default.hive_read_write_demo")
 
    #Display the spark dataframe values using show method
    df.show(10, truncate = False)
    



hc=HiveContext(sc)    
df=hc.sql('select * from table1')