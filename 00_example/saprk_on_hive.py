from pyspark.sql import SparkSession
import random

from pyspark.sql.types import StringType, IntegerType, DoubleType

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("test"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    table_name_df = ["myhive.courier_feedback", "myhive.courier_report", "myhive.merchant_device"]
    for table_name in table_name_df:
        table_df = spark.table(table_name)
        table_schema = table_df.schema
        table_df.printSchema()

        # 生成错误数据
        error_data = []

        # 空值
        for _ in range(random.randint(1000, 2000)):
            # 生成一行随机数据
            row_data = []
            for field in table_schema.fields:
                # 根据表中每列的数据类型生成随机数据
                if isinstance(field.dataType, StringType):
                    row_data.append(None)
                elif isinstance(field.dataType, IntegerType):
                    row_data.append(random.randint(1, 100))
                elif isinstance(field.dataType, DoubleType):
                    row_data.append(random.uniform(0.0,100.0))
                else:
                    row_data.append(random.randint(1, 100))
                # 其他数据类型根据需要继续添加条件判断

            # 添加到错误数据列表中
            error_data.append(row_data)

        # 重复值
        for _ in range(random.randint(1000, 2000)):
            # 生成一行随机数据
            row_data = []
            for field in table_schema.fields:
                # 根据表中每列的数据类型生成随机数据
                if isinstance(field.dataType, StringType):
                    row_data.append("error")
                elif isinstance(field.dataType, IntegerType):
                    row_data.append(-100)
                elif isinstance(field.dataType, DoubleType):
                    row_data.append(-100.0)
                else:
                    row_data.append(-100)
                # 其他数据类型根据需要继续添加条件判断

            # 添加到错误数据列表中
            error_data.append(row_data)

        error_df = spark.createDataFrame(error_data, table_schema)
        error_df.show()
        print(error_df.count())

        error_df.write.mode("append").format("Hive").saveAsTable(table_name)

    spark.stop()
