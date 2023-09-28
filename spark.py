import findspark

findspark.init()
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("CSVICEBERG") \
    .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.3.0") \
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkSessionCatalog") \
    .config("spark.sql.catalog.spark_catalog.type", "hive") \
    .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.local.type", "hadoop") \
    .config("spark.sql.catalog.local.warehouse", r"D:\iceberg_warehouse") \
    .config("spark.sql.defaultCatalog", "local") \
    .config("write.parquet.compression-codec", "uncompressed") \
    .getOrCreate()

if __name__ == '__main__':
    df = spark.sql("SELECT  * FROM local.test.cn16").show()
    df2 = spark.read.option("Header",True).csv(r"C:\Users\Admin\test_pyspark\test.csv").show()

