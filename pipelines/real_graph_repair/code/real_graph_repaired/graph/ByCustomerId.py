from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from real_graph_repaired.config.ConfigStore import *
from real_graph_repaired.udfs.UDFs import *

def ByCustomerId(spark: SparkSession, left: DataFrame, right: DataFrame, ) -> DataFrame:
    return left\
        .alias("left")\
        .join(right.alias("right"), (col("left.customer_id") == col("right.customer_id")), "inner")\
        .select(col("left.first_name").alias("first_name"), col("left.last_name").alias("last_name"), col("left.customer_id").alias("customer_id"), col("right.amount").alias("amount"))
