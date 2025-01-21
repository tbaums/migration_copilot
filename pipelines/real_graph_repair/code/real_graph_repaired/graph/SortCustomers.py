from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from real_graph_repaired.config.ConfigStore import *
from real_graph_repaired.udfs.UDFs import *

def SortCustomers(spark: SparkSession, inDF: DataFrame) -> DataFrame:
    return inDF.orderBy(col("customer_id").asc())
