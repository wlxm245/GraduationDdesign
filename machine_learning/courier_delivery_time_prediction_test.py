import time
from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler, OneHotEncoder, MinMaxScaler
from pyspark.ml.regression import GBTRegressor, RandomForestRegressor, LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.sql.functions import when, lit, col
from pyspark.sql import DataFrame


# 评价模型
def evaluate_predictions(predictions, name):
    # 计算MAE、MSE、RMSE和R-Squared等
    evaluator = RegressionEvaluator(labelCol="delivery_time", predictionCol=name)
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})
    mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})

    # 输出指标结果
    print(f"MAE:{mae}       MSE:{mse}       RMSE:{rmse}         R-Squared:{r2}")

    return mae, mse, rmse, r2


# 将时间转换成时间戳
def convert_to_timestamp(df: DataFrame, time_cols: list) -> DataFrame:
    for col_name in time_cols:
        df = df.withColumn(f"{col_name}_time", col(f"{col_name}_hour") * 60 + col(f"{col_name}_minute"))
        df = df.withColumn(f"{col_name}_time", col(f"{col_name}_time").cast("int"))
        if col_name != "acceptance":
            df = df.withColumn(f"{col_name}_time",
                               when((col("acceptance_time") > 1080) & (col(f"{col_name}_time") < 120),
                                    col(f"{col_name}_time") + 1440).otherwise(
                                   col(f"{col_name}_time"))
                               )
    return df


if __name__ == '__main__':
    spark = SparkSession.builder. \
        appName("courier_delivery_time_prediction"). \
        master("yarn"). \
        config("spark.sql.shuffle.partitions", 2). \
        config("spark.sql.warehouse.dir", "hdfs://node1:8020/user/hive/warehouse"). \
        config("hive.metastore.uris", "thrift://node1:9083"). \
        enableHiveSupport(). \
        getOrCreate()
    sc = spark.sparkContext

    # 读取快递报告数据
    courier_report_df = spark.table("myhive.courier_report_clean")

    # 删除非必要数据
    courier_report_df = courier_report_df.drop("no")

    # 使用预测的数据
    data_df = courier_report_df.withColumn("city_id", col("city_id").cast("int"))

    # 调用函数进行转换
    time_columns = ["acceptance", "arrier", "pickup", "delivery"]
    data_df = convert_to_timestamp(data_df, time_columns)

    data_df = data_df.drop(*(
        "acceptance_hour", "acceptance_minute", "acceptance_second", "arrier_hour", "arrier_minute", "arrier_second",
        "pickup_hour", "pickup_minute", "pickup_second",
        "delivery_hour", "delivery_minute", "delivery_second"))

    # 进一步处理特征向量
    # 创建OneHotEncoder将索引编码成独热向量
    encoder = OneHotEncoder(inputCol="city_id", outputCol="city_id_encoded")
    data_df = encoder.fit(data_df).transform(data_df)

    data_df.show()

    # 特征选择
    feature_columns = ['city_id_encoded', "day", 'acceptance_time', 'arrier_time', 'pickup_time']

    # 合并特征列为一个特征向量
    assembler = VectorAssembler(inputCols=feature_columns, outputCol="raw_features")
    data = assembler.transform(data_df)
    data.show()

    # 归一化特征向量
    scaler = MinMaxScaler(inputCol="raw_features", outputCol="features")
    scaler_model = scaler.fit(data)
    data = scaler_model.transform(data)

    # 划分数据集为训练集和测试集
    train_data, test_data = data.randomSplit([0.8, 0.2], seed=20)

    # 初始化线性回归模型
    lr_model = LinearRegression(featuresCol="features", labelCol="delivery_time", predictionCol="prediction_lr")

    # 获取训练开始时间
    start_time = time.time()

    # 训练模型
    lr_trained_model = lr_model.fit(train_data)

    # 获取训练结束时间
    end_time = time.time()

    # 计算训练时间
    training_time = end_time - start_time
    print("线性回归训练时间:", training_time, "seconds")

    # 在测试集上进行预测
    lr_predictions = lr_trained_model.transform(test_data)
    lr_predictions_train = lr_trained_model.transform(train_data)

    # 评估模型性能
    print("线性回归 test_data 评估")
    evaluate_predictions(lr_predictions, "prediction_lr")
    print("线性回归 train_data 评估")
    evaluate_predictions(lr_predictions_train, "prediction_lr")

    # 初始化随机森林回归模型
    rf_model = RandomForestRegressor(featuresCol="features", labelCol="delivery_time",
                                     predictionCol="prediction_rf")

    # 获取训练开始时间
    start_time = time.time()

    # 训练模型
    trained_model = rf_model.fit(train_data)

    # 获取训练结束时间
    end_time = time.time()

    # 计算训练时间
    training_time = end_time - start_time
    print("随机森林训练时间:", training_time, "seconds")

    # 在测试集上进行预测
    rf_predictions = trained_model.transform(test_data)
    rf_predictions_train = trained_model.transform(train_data)

    # 评估模型性能
    print("随机森林test_data评估")
    evaluate_predictions(rf_predictions, "prediction_rf")
    print("随机森林train_data评估")
    evaluate_predictions(rf_predictions_train, "prediction_rf")

    # 初始化 GBTRegressor 模型
    gbt = GBTRegressor(featuresCol="features", labelCol="delivery_time", predictionCol="prediction_gbt")

    # 获取训练开始时间
    start_time = time.time()

    # 训练模型
    gbt_model = gbt.fit(train_data)

    # 获取训练结束时间
    end_time = time.time()

    # 计算训练时间
    training_time = end_time - start_time
    print("GBT训练时间:", training_time, "seconds")

    # 在测试集上进行预测
    gbt_predictions = gbt_model.transform(test_data)
    gbt_predictions_train = gbt_model.transform(train_data)

    # 评估模型
    # 评估模型性能
    print("GBT test_data评估")
    evaluate_predictions(gbt_predictions, "prediction_gbt")
    print("GBT train_data评估")
    evaluate_predictions(gbt_predictions_train, "prediction_gbt")

    # 使用均值融合模型
    fusion_predictions = rf_predictions.join(gbt_predictions,
                                             on=["city_id", "day", "courier_id", "shop_id", "acceptance_time",
                                                 "arrier_time", "pickup_time", "delivery_time", "city_id_encoded",
                                                 "features"],
                                             how="inner")
    fusion_predictions = fusion_predictions.join(lr_predictions,
                                                 on=["city_id", "day", "courier_id", "shop_id", "acceptance_time",
                                                     "arrier_time", "pickup_time", "delivery_time", "city_id_encoded",
                                                     "features"],
                                                 how="inner")\
        .withColumn("fusion_prediction", (col("prediction_rf") + col("prediction_gbt") + col("prediction_lr")) / 3.0)

    print("融合模型test_data评估")
    evaluate_predictions(fusion_predictions, "fusion_prediction")

    fusion_predictions_train = rf_predictions_train.join(gbt_predictions_train,
                                                         on=["city_id", "day", "courier_id", "shop_id",
                                                             "acceptance_time", "arrier_time", "pickup_time",
                                                             "delivery_time", "city_id_encoded", "features"],
                                                         how="inner")
    fusion_predictions_train = fusion_predictions_train.join(lr_predictions_train,
                                                             on=["city_id", "day", "courier_id", "shop_id",
                                                                 "acceptance_time", "arrier_time", "pickup_time",
                                                                 "delivery_time", "city_id_encoded", "features"],
                                                             how="inner")\
        .withColumn("fusion_prediction", (col("prediction_rf") + col("prediction_gbt") + col("prediction_lr")) / 3.0)
    print("融合模型train_data评估")
    evaluate_predictions(fusion_predictions_train, "fusion_prediction")

    # 结束 SparkSession
    spark.stop()
