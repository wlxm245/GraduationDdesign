from pyspark.sql import SparkSession

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("data_clean"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext


    # 删除函数
    def clean(df):
        # 删除包含任何缺失值的行
        df_filled = df.na.drop()
        # 删除重复行
        df_unique = df_filled.dropDuplicates()
        return df_unique


    #对4个表处理
    df_courier_device = clean(spark.table("myhive.courier_device"))
    df_courier_device.write.mode("overwrite").saveAsTable("myhive.courier_device_clean")

    df_merchant_device = clean(spark.table("myhive.merchant_device"))
    df_merchant_device.write.mode("overwrite").saveAsTable("myhive.merchant_device_clean")

    df_test_courier_feedback = clean(spark.table("myhive.courier_feedback"))
    df_courier_feedback = df_test_courier_feedback.where("acceptance_hour >= 0 AND acceptance_minute >= 0 AND "
                                                         "acceptance_second >= 0 AND feedback_hour >= 0 AND "
                                                         "feedback_minute >= 0 AND feedback_second >= 0 AND feedback "
                                                         ">= 0 AND distance_to_merchant >= 0 AND "
                                                         "distance_to_merchant <= 1000")
    df_courier_feedback.write.mode("overwrite").saveAsTable("myhive.courier_feedback_clean")

    df_test_courier_report = clean(spark.table("myhive.courier_report"))
    df_courier_report = df_test_courier_report.where("acceptance_hour >= 0 AND acceptance_minute >= 0 AND "
                                                     "acceptance_second >= 0 AND arrier_hour >= 0 AND arrier_minute "
                                                     ">= 0 AND arrier_second >= 0 AND pickup_hour >= 0 AND "
                                                     "pickup_minute >= 0 AND pickup_second >= 0 AND delivery_hour >= "
                                                     "0 AND delivery_minute >= 0 AND delivery_second >= 0")
    df_courier_report.write.mode("overwrite").saveAsTable("myhive.courier_report_clean")


    #查看删除情况
    spark.table("myhive.courier_device").show()
    spark.table("myhive.courier_device_clean").show()
    spark.table("myhive.merchant_device").show()
    spark.table("myhive.merchant_device_clean").show()
    spark.table("myhive.courier_feedback").show()
    spark.table("myhive.courier_feedback_clean").show()
    spark.table("myhive.courier_report").show()
    spark.table("myhive.courier_report_clean").show()

    spark.stop()
