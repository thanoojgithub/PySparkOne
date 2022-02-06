from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import *


spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()

data = [('James', '', None, '1991-04-01', 'M', 3000), ('Michael', 'Rose', '', '2000-05-19', 'M', 4000),
        ('Robert', '', 'Williams', None, 'M', 4000), ('Maria', 'Anne', 'Jones', '1967-12-01', 'F', 4000),
        ('Maria', 'Anne', 'Jones', '1967-12-02', 'F', 4000), ('Jen', 'Mary', 'Brown', '1980-02-17', 'F', -1)]

columns = ["firstname", "middlename", "lastname", "dob", "gender", "salary"]
df = spark.createDataFrame(data=data, schema=columns)
df.write.mode("overwrite").parquet("/tmp/out/sourceData.parquet")


df1 = spark.read.parquet("/tmp/out/sourceData.parquet")
print("df1 : ***********")
df1.show()

df2 = df1.filter(col("firstname").isNotNull() & col("middlename").isNotNull() & col("lastname").isNotNull() &
                 col("dob").isNotNull())
print("df2 : ***********")
df2.show()

# df3 = df2.dropDuplicates(["firstname", "middlename", "lastname", "dob"])
# when, considering all columns in the table for distinct then,  dropDuplicates() or specific columns then, above line
df31 = df2.dropDuplicates()
print("df31 : ***********")
df31.show()
# DeDuping using row_number if we have deciding column to pick one
win1 = Window.partitionBy('firstname', 'middlename', 'lastname').orderBy(col('dob').desc())
df3 = df2.withColumn('rn', row_number().over(win1)).where('rn = 1').drop('rn')
print("df3 : ***********")
df3.show()

# df3 = df2.withColumn('startDate', date_format(current_timestamp(), 'yyyy-MM-dd'))
# current_date()
df4 = df3.withColumn("startDate", current_timestamp())\
    .withColumn("endDate", (unix_timestamp() + 999999999).cast('timestamp'))
print("df4 : ***********")
df4.show(truncate=False)



