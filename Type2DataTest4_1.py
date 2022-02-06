from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Window
from configparser import ConfigParser


def count_check_fn(df, count_check_tmp):
    if count_check_tmp.upper() == "Y":
        count_val = df.count()
        print("Count :", count_val)
        if count_val > 0:
            return True
        else:
            return False
    else:
        return True


def null_check(df):
    null_check_flag = config['ccSchema']['null_check']
    print("null_check_flag : ", null_check_flag)
    null_check_columns_1 = config['ccSchema']['null_check_columns']
    null_check_columns = null_check_columns_1.split(",") if null_check_columns_1 else []
    columns_schema_check = all(elem in df.columns for elem in null_check_columns)
    if (null_check_flag.upper() == "Y") & columns_schema_check:
        df1 = df
        for c in null_check_columns:
            df1 = df.filter(col(c).isNotNull())
        return df1
    else:
        return df


# sdf = sdf.na.fill(fill_values)

def duplicate_check(df):
    duplicate_check_flag = config['ccSchema']['duplicate_check']
    print("duplicate_check_flag : ", duplicate_check_flag)
    duplicate_check_columns_1 = config['ccSchema']['duplicate_check_columns']
    duplicate_check_columns = duplicate_check_columns_1.split(",") if duplicate_check_columns_1 else []
    columns_schema_check = all(elem in df.columns for elem in duplicate_check_columns)
    if (duplicate_check_flag.upper() == "Y") & columns_schema_check:
        print("Before deduping :", df.count())
        # dftmp = df.dropDuplicates()
        duplicate_check_columns_order_by_column = config['ccSchema']['duplicate_check_columns_order_by_column']
        win1 = Window.partitionBy(duplicate_check_columns).orderBy(col(duplicate_check_columns_order_by_column).desc())
        df1 = df.withColumn('rn', row_number().over(win1)).where('rn = 1').drop('rn')
        print("After deduping :", df1.count())
        return df1
    else:
        return df


def schema_check(df):
    print(df.columns)
    columns_schema_validation_1 = config['ccSchema']['ccColumns_schema_validation_flag']
    if columns_schema_validation_1.upper == "Y":
        columns_schema_validation_2 = columns_schema_validation_1.split(",") if columns_schema_validation_1 else []
        print(columns_schema_validation_2)
        columns_schema_check = all(elem in df.columns for elem in columns_schema_validation_2)
        print("columns_schema_check : ", columns_schema_check)
        return columns_schema_check
    else:
        return True


if __name__ == "__main__":
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
    print(ccColumns_1)

    # (unix_timestamp() + 999999999).cast('timestamp')
    ccDF1 = spark.createDataFrame(data=ccData1, schema=ccColumns) \
        .withColumn("StartDate", to_timestamp(lit("2021-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss')) \
        .withColumn("EndDate", to_timestamp(lit("9999-12-31 23:59:59"), 'yyyy-MM-dd HH:mm:ss'))
    ccDF1.printSchema()
    count_check = config['ccSchema']['count_check']

    if count_check_fn(ccDF1, count_check):
        if schema_check(ccDF1):
            ccDF1_1 = null_check(ccDF1)
            ccDF1_2 = duplicate_check(ccDF1_1)
            ccDF1_2.show(truncate=False)
