from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from prophecy.utils import *
from prophecy.libs import typed_lit
from real_graph_repaired.config.ConfigStore import *
from real_graph_repaired.udfs.UDFs import *

def Report(spark: SparkSession, inDF: DataFrame):
    inDF.write\
        .option("header", True)\
        .option("sep", ",")\
        .mode("overwrite")\
        .option("separator", ",")\
        .option("header", True)\
        .csv("/Volumes/michael/demo/demo_migration_copilot/migration_copilot_report.csv")
