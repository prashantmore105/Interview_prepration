# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.


"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col,isnan, when, count
# Create SparkSession 
spark = SparkSession.builder \
      .master("local[1]") \
      .appName("SparkByExamples.com") \
      .getOrCreate() 
      
print(spark)  
df=spark.read.option("header",True).csv("C:/Users/prashantm5.MUMBAI1/Downloads/userdata1.csv")

#df.show()
#print(df.columns)

def fun_renamedf(df):
    for x in df.columns :
        df1= df.withColumnRenamed('LAST_NAME', 'FAMILY_NAME')
    return df1        
    
df1=fun_renamedf(df)
df1.show()
for c in df1.columns :
    print("count of null columns")
    #df1.select([count(when(isnan(c) | col(c).isNull(), c))]).show()

print("=====next loop===")
#df1.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in df1.columns]
#   ).show()

#for c in df1.columns:
  #df1.select([count(when (col(c).contains('NULL') | col(c).contains('None') ,c))]).show()
 #df1.select(count(when( col(c).contains('None') |  col(c).contains('NULL') | col(c).isNull() ))).show()
#  df1.select(col(c).isNull()).show()
    #df1.select(count(when col(c).contains('None') | col(c).contains('NULL') ),c )
                     
                     #| col(c) == '' | col(c).isNull()) ,c)
               

# Create RDD from parallelize    
#dataList = [("Java", 20000), ("Python", 100000), ("Scala", 3000)]
#rdd=spark.sparkContext.parallelize(dataList)   

# Python program to read
# json file
 
#import json
#import array
 
# Opening JSON file
#f = open('C:/Users/prashantm5.MUMBAI1/Downloads/clinical_2022-11-22_1697447655.5713136.json')

#a="{base_url}/reporting/clinical/patients/{page_number}/{no_of_records_per_page}?startDateTime={startDate}%2000:00:00&endDateTime={startDate}%2011:59:59', ' {base_url}/reporting/clinical/patients/{page_number}/{no_of_records_per_page}?startDateTime={startDate}%2012:00:00&endDateTime={startDate}%2023:59:59"
#b= "2000:00:00"
#c="2023:59:59"

#if b in a:
#    print("=======found=======")
# returns JSON object as 
# a dictionary
#data = json.load(f)
#print("===========================---------")
#total_pages=data["totalPages"]
#print(total_pages) 
#totalElements=data["totalElements"]
#print(total_pages-1) 
#if (total_pages -1) > 0 :
#    print("yessss")
#print(data["totalElements"]) 
# Iterating through the json
# list
#my_input=data['content']
#json_string = json.dumps(data)
#for i in data['content']:
#    print(i)
    #my_input.extend(i) 
# Closing file
#print("printing array=================")
#print(my_input)
#print("array printing done============")
#f.close()


