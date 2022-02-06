from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StringType, TimestampType

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

ccData1 = [('A', 'Due to heavy rainfall'), ('B', 'Due to heavy snowfall'), ('C', 'Due to some technical issue')]

# TODO - Schema creation
ccColumns = ["CancellationCode", "CancellationDesc"]
# (unix_timestamp() + 999999999).cast('timestamp')
ccDF1 = spark.createDataFrame(data=ccData1, schema=ccColumns).withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", lit(None).cast(TimestampType()))
ccDF1.printSchema()
ccDF1.show(truncate=False)

ccData2 = [('A', 'Due to heavy rainfall'), ('B', 'Due to heavy sky-fall'), ('D', 'Due to bad weather')]

ccDF2 = spark.createDataFrame(data=ccData2, schema=ccColumns).withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", lit(None).cast(TimestampType()))
ccDF2.printSchema()
ccDF2.show(truncate=False)

ccDF21_updated = ccDF2.join(ccDF1, ccDF1.CancellationCode == ccDF2.CancellationCode, "inner") \
    .where(ccDF1.CancellationDesc != ccDF2.CancellationDesc) \
    .select(ccDF2.CancellationCode, ccDF2.CancellationDesc, ccDF2.StartDate, ccDF2.EndDate)
ccDF21_updated.show(truncate=False)

ccDF21_new = ccDF2.join(ccDF1, ccDF1.CancellationCode == ccDF2.CancellationCode, "leftanti")
ccDF21_new.show(truncate=False)

ccDF11_updated1 = ccDF1.where(col('CancellationCode')
                              .isin(ccDF21_updated
                                    .select(col('CancellationCode')).rdd.flatMap(lambda x: x).collect())) \
    .withColumn("EndDate", current_timestamp())
ccDF11_updated1.show(truncate=False)
ccDF1_latest1 = ccDF1.filter(~col('CancellationCode')
                             .isin(ccDF21_updated.select(col('CancellationCode')).rdd.flatMap(lambda x: x).collect()))
ccDF1.show(truncate=False)

ccDF_final = ccDF1_latest1.union(ccDF11_updated1).union(ccDF21_new).union(ccDF21_updated) \
    .orderBy(col('CancellationCode'), col('EndDate').asc_nulls_last())
ccDF_final.show(truncate=False)
