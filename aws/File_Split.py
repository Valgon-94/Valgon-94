from pip._internal.cli.cmdoptions import src
from pyspark.sql import SparkSession
import sys
import json
import os
from math import ceil

source_filename = sys.argv[1]
target_filename = sys.argv[2]
base_dir = "s3://8k-vigneshwar-de/"

source = base_dir + source_filename
target = base_dir + "output/" + target_filename


sz = os.path.getsize(src)
actual_size = (((sz/1024)/1024)/1024) + 2 # checking the size in GB
print(actual_size)

num_part = ceil(actual_size)
print(num_part)

'''
source = "dbfs:/FileStore/shared_uploads/jeyamvicky476@gmail.com/Weather_report_kaggle_2.csv"
target = "dbfs:/FileStore/shared_uploads/jeyamvicky476@gmail.com/output/Weather_report_kaggle_op_5.parquet"
'''
app_name = "vigneshwar_spark_session"

spark_builder = (
    SparkSession
        .builder
        .appName(app_name))

spark_sess = spark_builder.getOrCreate()
df = spark_sess.read.load(source, format="csv", inferschema="true", header="true")

#df.repartition(5).write.partitionBy("Summary", "Daily_Summary").parquet(target, mode="overwrite")
df.repartition(int(num_part)).write.parquet(target, mode="overwrite")
spark_sess.stop()
