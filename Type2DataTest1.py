from pyspark.sql import SparkSession
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('pyspark-test-run') \
    .getOrCreate()
emp1 = [(1, "Smith", -1, "2018", "10", "M", 3000),
        (2, "Rose", 1, "2010", "20", "M", 4000),
        (3, "Williams", 1, "2010", "10", "M", 1000),
        (4, "Jones", 2, "2005", "10", "F", 2000),
        (5, "Brown", 2, "2010", "40", "", -1),
        (6, "Brown", 2, "2010", "50", "", -1)
        ]
empColumns = ["emp_id", "name", "superior_emp_id", "year_joined",
              "emp_dept_id", "gender", "salary"]

empDF1 = spark.createDataFrame(data=emp1, schema=empColumns).withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", (unix_timestamp() + 999999999).cast('timestamp'))
empDF1.printSchema()
empDF1.show(truncate=False)

emp2 = [(1, "Smith1", -1, "2018", "10", "M", 3000),
        (12, "Rose", 1, "2010", "20", "M", 4000),
        (13, "Williams", 1, "2010", "10", "M", 1000),
        (4, "Jones", 2, "2005", "10", "F", 2000),
        (15, "Brown", 2, "2010", "40", "", -1),
        (6, "Brown", 2, "2011", "50", "", -1)
        ]

empDF2 = spark.createDataFrame(data=emp2, schema=empColumns)\
    .withColumn("StartDate", current_timestamp()) \
    .withColumn("EndDate", (unix_timestamp() + 999999999).cast('timestamp'))
empDF2.printSchema()
empDF2.show(truncate=False)

empDF21_new = empDF2.join(empDF1, empDF1.emp_id == empDF2.emp_id, "leftanti") \

empDF21_new.show(truncate=False)
empDF1.createOrReplaceTempView("empDF1")
empDF2.createOrReplaceTempView("empDF2")

# emp_id|name    |superior_emp_id|year_joined|emp_dept_id|gender|salary
empDF21_updated = spark.sql("select e2.* from empDF1 e1 join empDF2 e2 on e1.emp_id = e2.emp_id "
                            "where e1.name != e2.name OR e1.name != e2.name "
                            "OR e1.superior_emp_id != e2.superior_emp_id "
                            "OR e1.year_joined != e2.year_joined OR e1.emp_dept_id != e2.emp_dept_id "
                            "OR e1.gender != e2.gender "
                            "OR e1.salary != e2.salary")
empDF21_updated.createOrReplaceTempView("empDF21_updated")
empDF21_updated.show(truncate=False)
spark.sql("select e1.emp_id, e1.name, e1.superior_emp_id, e1.year_joined, e1.emp_dept_id, e1.gender, e1.salary, "
          "e1.StartDate, current_timestamp as EndDate from empDF1 e1 where emp_id in "
          "(select emp_id from empDF21_updated)").show(truncate=False)
