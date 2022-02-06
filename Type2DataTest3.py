from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, TimestampType

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ccDF1 = spark.read.option("header", "true").csv(
    "/home/thanooj/Downloads/data_cancellationData_Cancellation_Details.csv") \
    .withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", lit(None).cast(TimestampType()))
# ccDF1.printSchema()
ccDF1.show(truncate=False)

ccDF2 = spark.read.option("header", "true")\
    .csv("/home/thanooj/Downloads/data_cancellationData_Cancellation_Details_Jan012022.csv") \
    .withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", lit(None).cast(TimestampType()))
# ccDF2.printSchema()
ccDF2.show(truncate=False)

ccDF21_updated = ccDF2.join(ccDF1, ccDF1.CancellationCode == ccDF2.CancellationCode, "inner") \
    .where(trim(ccDF1.CancellationDesc) != trim(ccDF2.CancellationDesc)) \
    .select(ccDF2.CancellationCode, ccDF2.CancellationDesc, ccDF2.StartDate, ccDF2.EndDate)
ccDF21_updated.show(truncate=False)

