from pyspark.sql.types import Row, DoubleType, StructType, StructField
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, unix_timestamp, lpad, concat_ws, when, expr

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("courier_analysis"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # 将feedback改成中文
    feedback_mapping = {
        0: "稍后尝试",
        1: "确认"
    }
    feedback_mapping_df = spark.createDataFrame([Row(feedback=k, feedback_tran=v) for k, v in feedback_mapping.items()])

    # 读取表数据
    courier_feedback_df = spark.table("myhive.courier_feedback_clean")
    courier_report_df = spark.table("myhive.courier_report_clean")

    # 计算响应时间（以秒为单位）
    response_time_seconds = (col("feedback_hour") * 3600 + col("feedback_minute") * 60 + col("feedback_second")) - \
                            (col("acceptance_hour") * 3600 + col("acceptance_minute") * 60 + col("acceptance_second"))

    # 添加响应时间列到 DataFrame
    courier_feedback_with_response_time_df = courier_feedback_df.withColumn("response_time_seconds",
                                                                            response_time_seconds)
    courier_feedback_with_response_time_df = courier_feedback_with_response_time_df.join(feedback_mapping_df,
                                                                                         on=["feedback"],
                                                                                         how="inner")

    # 计算平均响应时间
    avg_response_time = courier_feedback_with_response_time_df.agg({"response_time_seconds": "avg"}).collect()[0][0]
    print("平均响应时间: {:.2f} 秒".format(avg_response_time))

    # 手动定义模式
    schema = StructType([
        StructField("avg_response_second", DoubleType()),
        StructField("avg_response_minute", DoubleType())
    ])

    # 将平均响应时间写入hive表
    spark.createDataFrame([(avg_response_time, avg_response_time / 60)], schema) \
        .write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable("myhive_detail.avg_response_time")

    # 分析快递员对通知的回应情况
    response_counts = courier_feedback_with_response_time_df.groupBy("courier_id", "feedback_tran").count() \
        .withColumnRenamed("count", "feedback_count")
    response_counts.where("feedback_tran = '稍后尝试'").orderBy(desc("feedback_count")).show()
    response_counts.where("feedback_tran = '确认'").orderBy(desc("feedback_count")).show()

    # 将分析快递员对通知的回应情况写入hive表
    response_counts.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.courier_response_counts")

    # 分析快递员处理的订单数量
    order_counts = courier_feedback_with_response_time_df.groupBy("courier_id").count() \
        .withColumnRenamed("count", "order_count")
    order_counts.orderBy(desc("order_count")).show()

    # 计算每个外卖员的工作时长（以秒为单位）
    courier_report_df = courier_report_df.withColumn("acceptance_time",
                                                     concat_ws(':', lpad(col("acceptance_hour"), 2, '0'),
                                                               lpad(col("acceptance_minute"), 2, '0'),
                                                               lpad(col("acceptance_second"), 2, '0')))
    courier_report_df = courier_report_df.withColumn("delivery_time",
                                                     concat_ws(':', lpad(col("delivery_hour"), 2, '0'),
                                                               lpad(col("delivery_minute"), 2, '0'),
                                                               lpad(col("delivery_second"), 2, '0')))
    # 将时间字符串转换为时间戳，并计算时间差（秒）
    work_duration_seconds = (
            unix_timestamp(col("delivery_time"), "HH:mm:ss") -
            unix_timestamp(col("acceptance_time"), "HH:mm:ss")).cast("bigint")
    # 考虑跨越天数的情况，处理负数时长
    work_duration_seconds = when(work_duration_seconds < 0, 24 * 3600 + work_duration_seconds).otherwise(
        work_duration_seconds)

    # 添加工作时长列到 DataFrame
    courier_report_with_work_duration_df = courier_report_df.withColumn("work_duration_seconds", work_duration_seconds)

    # 计算每个外卖员的平均工作时长（以秒为单位）
    avg_work_duration_seconds = courier_report_with_work_duration_df.groupBy("courier_id") \
        .agg({"work_duration_seconds": "avg"}) \
        .withColumnRenamed("avg(work_duration_seconds)", "avg_work_duration_seconds")
    avg_work_duration_seconds.show()

    # 将快递员数据整合起来
    courier_final = order_counts.join(avg_work_duration_seconds, on=["courier_id"], how="inner")
    courier_final.show()

    # 计算avg_work_duration_seconds和order_count列的99%和1%分位数
    percentiles = courier_final.approxQuantile(["avg_work_duration_seconds", "order_count"], [0.01, 0.99], 0.001)

    # 提取分位数
    p01_work_duration_seconds, p99_work_duration_seconds = percentiles[0]
    p10_order_count, p90_order_count = percentiles[1]

    # 使用when函数将每列的值映射到0到100的范围内
    courier_final = courier_final.withColumn("work_duration_score",
                                             expr("CASE WHEN avg_work_duration_seconds <= {} THEN 100 "
                                                  "WHEN avg_work_duration_seconds >= {} THEN 0 "
                                                  "ELSE 100 * ({} - avg_work_duration_seconds) / ({} - {}) END"
                                                  .format(p01_work_duration_seconds, p99_work_duration_seconds,
                                                          p99_work_duration_seconds, p99_work_duration_seconds,
                                                          p01_work_duration_seconds)))

    courier_final = courier_final.withColumn("order_count_score",
                                             expr("CASE WHEN order_count <= {} THEN 0 "
                                                  "WHEN order_count >= {} THEN 100 "
                                                  "ELSE 100 * (order_count - {}) / ({} - {}) END"
                                                  .format(p10_order_count, p90_order_count,
                                                          p10_order_count, p90_order_count,
                                                          p10_order_count)))

    # 计算得分的平均值
    courier_final = courier_final.withColumn("final_score",
                                             (col("work_duration_score") * 0.4) + (col("order_count_score") * 0.6))
    courier_final = courier_final.drop(*('work_duration_score', 'order_count_score'))
    courier_final.orderBy(desc("final_score")).show()

    # 将快递员数据整合写入hive表
    courier_final.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.courier_final")

    # 停止SparkSession
    spark.stop()
