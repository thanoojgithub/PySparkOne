from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

temporaryGcsBucket = "temporarygcsbucket1"
spark.conf.set('temporaryGcsBucket', temporaryGcsBucket)

df1 = spark.read.format("csv").options(header='True', inferSchema='True', delimiter=',') \
    .load("gs://bucket_27122021/data/cancellationData/Cancellation_Details.csv")

# For now, converting csv to parquet format, writing to GCP bucket, to make expected input to start transformation.
df1.write.mode("overwrite").parquet("gs://bucket_27122021/data/cancellationData/parquet")

# actual transformation will start from here.
# reading airLine data as parquet format from GCP bucket and dropping unwanted columns
df1_1 = spark.read.parquet("gs://bucket_27122021/data/cancellationData/parquet") \
    .drop('CreatedTimestamp', 'UpdatedTimestamp')

df1_2 = df1_1.filter(col("CancellationCode").isNotNull() & col("CancellationDesc").isNotNull())

df2 = spark.read.format("csv").options(header='True', inferSchema='True', delimiter=',') \
    .load("gs://bucket_27122021/data/cancellationData/Cancellation_Details_Jan012022.csv")

# For now, converting csv to parquet format, writing to GCP bucket, to make expected input to start transformation.
df2.write.mode("overwrite").parquet("gs://bucket_27122021/data/cancellationData/Jan2022/parquet")

# actual transformation will start from here.
# reading airLine data as parquet format from GCP bucket and dropping unwanted columns
df2_1 = spark.read.parquet("gs://bucket_27122021/data/cancellationData/Jan2022/parquet") \
    .drop('CreatedTimestamp', 'UpdatedTimestamp')

df2_2 = df2_1.filter(col("CancellationCode").isNotNull() & col("CancellationDesc").isNotNull())

ccDF1_3 = df1_2.withColumn("StartDate", to_timestamp(lit("2021-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("EndDate", to_timestamp(lit("9999-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss'))

ccDF2_3 = df2_2.withColumn("StartDate", to_timestamp(lit("2022-01-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("EndDate", to_timestamp(lit("9999-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss'))

ccDF1_4 = ccDF1_3.filter(col('EndDate') != '9999-12-31 23:59:59')
ccDF1_4.show(truncate=False)
ccDF1_5 = ccDF1_3.filter(col('EndDate') == '9999-12-31 23:59:59') \
    .withColumn("EndDate",
                when(col('CancellationCode').isin(
                    ccDF2_3.select(col('CancellationCode')).rdd.flatMap(lambda x: x).collect()),
                    to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss")).otherwise(col('EndDate')))

ccDF1_5.printSchema()
ccDF1_5.show(truncate=False)
ccDF1_6 = ccDF1_4.unionByName(ccDF1_5).unionByName(ccDF2_3).orderBy(col('CancellationCode'), col('EndDate'))
ccDF1_6.printSchema()
ccDF1_6.show(truncate=False)
ccDF1_6.write.format('bigquery') \
    .option('table', 'cancellationData1.cancellationData') \
    .partitionBy('CancellationCode') \
    .mode("overwrite") \
    .save()
