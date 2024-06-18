from datetime import datetime
import logging
from pyspark.sql.functions import trim
from pyspark.sql import SparkSession, Row
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType 

def input_file_ingestion():
            target_table = "usm_prod.med_rpr_cnfz.GRPR_FCLTY_OUTPT_TBL_MEDCR"
            file_loc = "s3a://cds-ingest-raw-prod/med-rpr/nas/grpout/date=2024-06-12/input_merged.gz"
            _schema = spark.table(target_table).schema 

            df = spark.read.format("csv").schema(_schema).option("header", False).option("sep", "\x01").load(file_loc) 

            print("Reference file is read")
            df.write.format("delta").mode('overwrite').saveAsTable(target_table)
            print('Data load complete!') 

input_file_ingestion()

 
