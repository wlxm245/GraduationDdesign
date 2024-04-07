from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws, unix_timestamp, lpad, avg, desc, rank, count
from pyspark.sql.types import Row
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("delivery_time"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # 创建city_mapping字典
    city_mapping = {
        1: '上海',
        2: '杭州',
        3: '深圳',
        4: '北京',
        5: '潍坊',
        6: '广州',
        7: '遂宁',
        8: '西宁',
        9: '定安',
        10: '廊坊'
    }

    # 将字典转换为DataFrame
    city_mapping_df = spark.createDataFrame([Row(city_id=k, city_name=v) for k, v in city_mapping.items()])

    # 读取Hive表
    courier_report_df = spark.table("myhive.courier_report_clean")

    # 计算接受订单和完成配送的时间戳
    courier_report_df = courier_report_df.withColumn("acceptance_time",
                                                     concat_ws(':', lpad(col("acceptance_hour"), 2, '0'),
                                                               lpad(col("acceptance_minute"), 2, '0'),
                                                               lpad(col("acceptance_second"), 2, '0')))
    courier_report_df = courier_report_df.withColumn("delivery_time",
                                                     concat_ws(':', lpad(col("delivery_hour"), 2, '0'),
                                                               lpad(col("delivery_minute"), 2, '0'),
                                                                lpad(col("delivery_second"), 2, '0')))
    # 将时间字符串转换为时间戳，并计算时间差（秒）
    courier_report_df = courier_report_df.withColumn("total_delivery_time_seconds",
                                                     (unix_timestamp(col("delivery_time"), "HH:mm:ss") - unix_timestamp(
                                                         col("acceptance_time"), "HH:mm:ss")).cast("bigint"))

    # 显示结果
    courier_report_df.show()

    # 分析配送流程效率，例如计算平均配送时间
    average_delivery_time = courier_report_df.agg({"total_delivery_time_seconds": "avg"}).collect()[0][0]
    print(f"外卖员平均配送时间: {average_delivery_time} 秒（{average_delivery_time / 60} 分钟）")

    # 将平均配送时间写入Hive表
    spark.createDataFrame([(average_delivery_time, average_delivery_time / 60)],
                          ["average_delivery_second", "average_delivery_minute"]) \
        .write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.average_delivery_time")

    # 聚合分析，按小时计算平均配送时间
    average_delivery_time_per_hour = courier_report_df.groupBy("city_id", "acceptance_hour") \
        .agg(avg("total_delivery_time_seconds").alias("avg_delivery_time_seconds"),
             count("total_delivery_time_seconds").alias("count")).where("count > 1000")

    # 定义窗口规范，按城市进行分区，并按订单数量降序排序
    window_spec = Window.partitionBy("city_id").orderBy(desc("avg_delivery_time_seconds"))

    # 添加排名列
    ranked_df = average_delivery_time_per_hour.withColumn("rank", rank().over(window_spec))

    # 过滤出每个城市排名在前三的时间段
    top_busy_hours_per_city_df = ranked_df.where(col("rank") <= 3).orderBy(desc("avg_delivery_time_seconds")) \
        .join(city_mapping_df, on=["city_id"], how="inner") \
        .select("city_name", "acceptance_hour", "avg_delivery_time_seconds", "count")
    top_busy_hours_per_city_df.show()

    # 存到hive表
    top_busy_hours_per_city_df.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.top_busy_hours_per_city")

    # 停止SparkSession
    spark.stop()
