from pyspark.sql import SparkSession
from pyspark.sql.functions import col, isnan, when, count

spark = SparkSession \
    .builder \
    .master('yarn') \
    .appName('spark-travel-analysis') \
    .getOrCreate()

# Load data from BigQuery.
sourceDF = spark.read.parquet("gs://source")
sourceDF.createOrReplaceTempView('sourceData')

# sourceDF.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in sourceDF.columns]).show()
spark.sql("select * from sourceData where AIRLN_ID NOT EMPTY AND AIRLN_ID IS NOT NULL,"
          "AIRLN_NM,AIRLN_CARR,TAIL_NO,AIRPT_ID,"
          "AIRPT_NM,CTY_MKT_ID,CTY_NM,ST_NM,AIRLN_CANC_CD,AIRLN_CANC_DESC")
# Perform word count.
word_count = spark.sql('SELECT word, SUM(word_count) AS word_count FROM words GROUP BY word')
word_count.show()
word_count.printSchema()

# Saving the data to BigQuery
word_count.write.format('bigquery').mode("overwrite") \
    .option('table', 'optimal-analogy-336004:wordcount_dataset.wordcount_output ') \
    .save()
