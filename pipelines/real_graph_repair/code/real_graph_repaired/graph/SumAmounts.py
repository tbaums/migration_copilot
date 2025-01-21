from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from real_graph_repaired.config.ConfigStore import *
from real_graph_repaired.udfs.UDFs import *

def SumAmounts(spark: SparkSession, inDF: DataFrame) -> DataFrame:
    df1 = inDF.groupBy(col("customer_id"))

    return df1.agg(
        first(col("first_name")).alias("first_name"), 
        first(col("last_name")).alias("last_name"), 
        sum(col("amount")).alias("amount")
    )
