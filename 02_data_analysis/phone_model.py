from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType, Row

if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("phone_model"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    df_courier = spark.table("myhive.courier_device_clean")
    df_courier = df_courier.withColumn("city_id", df_courier["city_id"].cast(IntegerType()))
    df_courier.createOrReplaceTempView("courier_view")

    df_merchant = spark.table("myhive.merchant_device_clean")
    df_merchant = df_merchant.withColumn("city_id", df_merchant["city_id"].cast(IntegerType()))
    df_merchant.createOrReplaceTempView("shop_view")

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

    # 统计快递员操作系统版本和手机型号的使用频率
    print("courier_phone_mode_counts")
    courier_phone_mode_counts = spark.sql("""
        SELECT phone_mode, COUNT(*) as count
        FROM courier_view
        GROUP BY phone_mode
        ORDER BY count DESC
        LIMIT 20
    """)
    courier_phone_mode_counts.show()
    # 将数据存入hive中
    courier_phone_mode_counts.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.courier_phone_mode_counts")

    print("courier_os_version_counts")
    courier_os_version_counts = spark.sql("""
        SELECT phone_mode, os_version, COUNT(*) as count
        FROM courier_view
        GROUP BY phone_mode, os_version
        ORDER BY count DESC
        LIMIT 20
    """)
    courier_os_version_counts.show()
    # 将数据存入hive中
    courier_os_version_counts.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.courier_os_version_counts")

    print("courier_city_phone_mode_favourite")
    courier_city_phone_mode_favourite = spark.sql("""
        SELECT city_id, phone_mode, count
        FROM (
        SELECT city_id, phone_mode, COUNT(*) as count,
        ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY COUNT(*) DESC) as rn
        FROM courier_view
        GROUP BY city_id, phone_mode
        ) t
        WHERE t.rn = 1
        order by city_id ASC
        LIMIT 20
    """).join(city_mapping_df, on=["city_id"], how="inner").select("city_name", "phone_mode", "count")
    courier_city_phone_mode_favourite.show()
    # 将数据存入hive中
    courier_city_phone_mode_favourite.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.courier_city_phone_mode_favourite")

    # 统计商家制造商和品牌的使用频率
    print("shop_brand_counts")
    shop_brand_counts = spark.sql("""
        SELECT brand, COUNT(*) as count
        FROM shop_view
        GROUP BY brand
        ORDER BY count DESC
        LIMIT 20
    """)
    shop_brand_counts.show()
    # 将数据存入hive中
    shop_brand_counts.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.shop_brand_counts")

    print("shop_model_counts")
    shop_model_counts = spark.sql("""
        SELECT model, COUNT(*) as count
        FROM shop_view
        GROUP BY model
        ORDER BY count DESC
        LIMIT 20
    """)
    shop_model_counts.show()
    # 将数据存入hive中
    shop_model_counts.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.shop_model_counts")

    print("shop_city_model_favourite")
    shop_city_model_favourite = spark.sql("""
        SELECT city_id, model, count
        FROM (
        SELECT city_id, model, COUNT(*) as count,
        ROW_NUMBER() OVER (PARTITION BY city_id ORDER BY COUNT(*) DESC) as rn
        FROM shop_view
        GROUP BY city_id, model
        ) 
        WHERE t.rn = 1
        order by city_id ASC
        LIMIT 20
    """).join(city_mapping_df, on=["city_id"], how="inner").select("city_name", "model", "count")
    shop_city_model_favourite.show()
    # 将数据存入hive中
    shop_city_model_favourite.write.mode("overwrite").option("delimiter", ",").format("orc") \
        .saveAsTable("myhive_detail.shop_city_model_favourite")

    # 对每个城市的外卖员和商家数量统计
    courier_counts = spark.sql("SELECT city_id, COUNT(*) AS courier_count FROM courier_view GROUP BY city_id")
    merchant_counts = spark.sql("SELECT city_id, COUNT(*) AS merchant_count FROM shop_view GROUP BY city_id")
    city_counts = courier_counts.join(merchant_counts, "city_id", "inner") \
        .join(city_mapping_df, on=["city_id"], how="inner").select("city_name", "courier_count", "merchant_count")
    city_counts.show()
    city_counts.write.mode("overwrite").option("delimiter", ",").format("orc").saveAsTable(
        "myhive_detail.city_courier_merchant_counts")

    spark.stop()
