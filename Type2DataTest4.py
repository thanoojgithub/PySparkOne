from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from configparser import ConfigParser

config = ConfigParser()
# schema check , duplicate check = Y/N , null check = empty/columns
spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

config.read('config/cc_config.ini')
print(config.sections())
ccColumns_1 = config['ccSchema']['ccColumns']
ccData1 = [('A', 'Due to heavy rainfall'), ('B', 'Due to heavy snowfall'), ('C', 'Due to some technical issue'),
           ('D', 'Due to bad weather')]

# TODO - Schema creation
print(ccColumns_1)
ccColumns = ccColumns_1.split(",") if ccColumns_1 else []
print(print(ccColumns_1))

# (unix_timestamp() + 999999999).cast('timestamp')
ccDF1 = spark.createDataFrame(data=ccData1, schema=ccColumns) \
    .withColumn("StartDate", to_timestamp(lit("2021-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("EndDate", to_timestamp(lit("9999-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss'))
ccDF1.printSchema()
print(ccDF1.columns)
ccColumns_schema_check = all(elem in ccColumns for elem in ccDF1.columns)
print("ccColumns_schema_check")
print(ccColumns_schema_check)
ccDF1.show(truncate=False)
ccDF1.createOrReplaceTempView("ccDF1")
ccData2 = [('A', 'Due to heavy rainfall'), ('B', 'Due to heavy sky-fall'), ('E', 'Due to bad radio signals')]
ccDF2 = spark.createDataFrame(data=ccData2, schema=ccColumns) \
    .withColumn("StartDate", to_timestamp(lit("2022-01-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss')) \
    .withColumn("EndDate", to_timestamp(lit("9999-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss'))
ccDF2.printSchema()
ccDF2.show(truncate=False)
print(ccDF2.select(col('CancellationCode')).rdd.flatMap(lambda x: x).collect())
ccDF11 = ccDF1.filter(col('EndDate') != '9999-12-31 23:59:59')
ccDF11.show(truncate=False)
ccDF12 = ccDF1.filter(col('EndDate') == '9999-12-31 23:59:59') \
    .withColumn("EndDate", when(col('CancellationCode').isin(ccDF2.select(col('CancellationCode'))
                                                             .rdd.flatMap(lambda x: x).collect()),
                                to_timestamp(current_timestamp(), "yyyy-MM-dd HH:mm:ss")).
                otherwise(col('EndDate')))

ccDF12.printSchema()
ccDF12.show(truncate=False)
ccDF_11_12_2 = ccDF11.unionByName(ccDF12).unionByName(ccDF2).orderBy(col('CancellationCode'), col('EndDate'))
ccDF_11_12_2.printSchema()
ccDF_11_12_2.show(truncate=False)
