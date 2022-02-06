from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
# .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
temporaryGcsBucket = "temporarygcsbucket1"
spark.conf.set('temporaryGcsBucket', "temporaryGcsBucket1")

# For now, reading airLine data in csv format from GCP bucket
df = spark.read.format("csv").options(header='True', inferSchema='True', delimiter=',') \
    .load("gs://bucket_27122021/data/travalData/airLineData/Airline_Details.csv")

# For now, converting csv to parquet format, writing to GCP bucket, to make expected input to start transformation.
df.write.mode("overwrite").parquet("gs://bucket_27122021/data/travalData/airLineData/parquet/airLineData.parquet")

# actual transformation will start from here.
# reading airLine data as parquet format from GCP bucket and dropping unwanted columns
df1 = spark.read.parquet("gs://bucket_27122021/data/travalData/airLineData/parquet/airLineData.parquet") \
    .drop('CreatedTimestamp', 'UpdatedTimestamp')
print("df1 : ***********")
print("before cleansing airLine row count : ")
print(df1.count())
df1.show()

# AirlineID,AirlineName,Carrier,TailNum
# doing null check on airLine data
df2 = df1.filter(col("AirlineID").isNotNull() & col("AirlineName").isNotNull() & col("Carrier").isNotNull() &
                 col("TailNum").isNotNull())
print("df2 : ***********")
df2.show()

# df3 = df2.dropDuplicates(["AirlineID", "AirlineName", "Carrier", "TailNum"])
# when, considering all columns in the table for distinct then,  dropDuplicates() or specific columns then, above line

# Removing duplicates - by combining/considering all columns
df31 = df2.dropDuplicates()
print("df31 : ***********")
df31.show()

# DeDuping using row_number if we have deciding column to pick one
win1 = Window.partitionBy('AirlineID', 'AirlineName', 'Carrier').orderBy(col('TailNum').desc())
df3 = df2.withColumn('rn', row_number().over(win1)).where('rn = 1').drop('rn')
print("df3 : ***********")
df3.show()

# df3 = df2.withColumn('startDate', date_format(current_timestamp(), 'yyyy-MM-dd'))
# current_date()

# Adding StartDate & EndDate columns as below
df4 = df31.withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", (unix_timestamp() + 999999999).cast('timestamp'))
print("df4 : ***********")
df4.show(truncate=False)

print("after cleansing airLine row count : ")
print(df4.count())
# Saving the data to BigQuery
df4.write.format('bigquery') \
    .option('table', 'airLineLoadData1.airLineLoadData ') \
    .partitionBy('AirlineID') \
    .mode("overwrite") \
    .save()
