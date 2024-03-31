from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder.\
        appName("test").\
        master("yarn").\
        config("spark.sql.shuffle.partitions", 2).\
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse").\
        config("hive.metastore.uris", "thrift://node1:9083").\
        enableHiveSupport().\
        getOrCreate()
    sc = spark.sparkContext

    spark.sql("show tables").show()