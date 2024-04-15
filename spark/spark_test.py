from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import IntegerType

spark = (    
            SparkSession
            .builder
            .getOrCreate()
)

sc = spark.sparkContext

data = [
    [1, 'Gosho' ,10000],
    [2, 'Pesho', 20000],
    [3, 'Maria', 30000],
    [4, 'Ivan', 40000],
    [5, 'Dragan', 50000],
]
    

# RDD
employees_rdd = sc.parallelize(data)


employees_rdd.collect()

# SPARK DF
employees_df =  employees_rdd.toDF(
        ['id', 'name', 'salary']
)
employees_df = employees_df.orderBy(
        F.col('salary'), 
        ascending=False
) 
employees_df.show(3)
