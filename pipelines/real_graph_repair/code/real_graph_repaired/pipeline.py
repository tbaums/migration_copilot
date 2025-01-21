from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
from real_graph_repaired.config.ConfigStore import *
from real_graph_repaired.udfs.UDFs import *
from prophecy.utils import *
from real_graph_repaired.graph import *

def pipeline(spark: SparkSession) -> None:
    df_Customers = Customers(spark)
    df_SortCustomers = SortCustomers(spark, df_Customers)
    df_Orders = Orders(spark)
    df_SortOrders = SortOrders(spark, df_Orders)
    df_ByCustomerId = ByCustomerId(spark, df_SortCustomers, df_SortOrders)
    df_SumAmounts = SumAmounts(spark, df_ByCustomerId)
    df_ByAmounts = ByAmounts(spark, df_SumAmounts)
    Report(spark, df_ByAmounts)

def main():
    spark = SparkSession.builder\
                .config("spark.default.parallelism", "4")\
                .config("spark.sql.legacy.allowUntypedScalaUDF", "true")\
                .enableHiveSupport()\
                .appName("Prophecy Pipeline")\
                .getOrCreate()
    Utils.initializeFromArgs(spark, parse_args())
    spark.conf.set("prophecy.metadata.pipeline.uri", "pipelines/real_graph_repair")
    registerUDFs(spark)
    
    MetricsCollector.instrument(spark = spark, pipelineId = "pipelines/real_graph_repair", config = Config)(pipeline)

if __name__ == "__main__":
    main()
