from pyspark.sql import SparkSession
from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank, col

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("shop_analysis"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # 读取表数据
    courier_feedback_df = spark.table("myhive.courier_feedback_clean")
    courier_report_df = spark.table("myhive.courier_report_clean")

    # 分析商家处理的订单数量
    shop_order_counts = courier_feedback_df.groupBy("shop_id").count() \
        .withColumnRenamed("count", "order_count")
    shop_order_counts.orderBy(desc("order_count")).show()

    # 写入到hive表
    shop_order_counts.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.shop_order_counts")

    # 分析每天最高销量的三个商家
    shop_top_by_day_df = courier_feedback_df.groupBy(["day", "shop_id"]).count() \
        .withColumnRenamed("count", "shop_day_count")

    # 定义窗口规范，按城市进行分区，并按订单数量降序排序
    window_spec = Window.partitionBy("day").orderBy(desc("shop_day_count"))

    # 添加排名列
    ranked_df = shop_top_by_day_df.withColumn("rank", rank().over(window_spec))

    # 过滤出每个城市排名在前三的时间段
    shop_top_by_day = ranked_df.where(col("rank") <= 3).orderBy(desc("shop_day_count"))
    shop_top_by_day = shop_top_by_day.orderBy(col("day").asc(), col("rank").asc()).select("day", "shop_id",
                                                                                          "shop_day_count")
    shop_top_by_day.show()

    # 写入到hive表
    shop_top_by_day.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.shop_top_by_day")

    # 结束spark
    spark.stop()
