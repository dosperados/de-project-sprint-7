import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

from datetime import datetime, timedelta
import sys
import logging
import pyspark.sql.functions as F 
from pyspark.sql.window import Window 
from pyspark.sql.types import *
from math import radians, cos, sin, asin, sqrt

#comment: задаем все переменные далее по коду они будут обозначены где они используются
sname = sys.argv[1] #"dosperados" 
hdfs_path = sys.argv[2] #"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = sys.argv[3]  #"/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
date = sys.argv[4] #'2022-05-21'
depth = sys.argv[5] #28


#comment: Функция формирует list со список папок для загрузки  
def input_event_paths(hdfs_path:str, date:str, depth:int, event:str, sname:str):
    
    event_type = f"/event_type={event}"
    url_in = f'/user/{sname}/data/events/date='

    return sorted(list(set(sorted([hdfs_path + url_in + (datetime.strptime(date, '%Y-%m-%d') - timedelta(days=i)).strftime('%Y-%m-%d') + event_type for i in range(int(depth))]))))


#comment: Func calc distance
def get_distance(lon_a, lat_a, lon_b, lat_b):
    # Transform to radians
    lon_a, lat_a, lon_b, lat_b = map(radians, [lon_a,  lat_a, lon_b, lat_b])
    dist_longit = lon_b - lon_a
    dist_latit = lat_b - lat_a

    # Calculate area
    area = sin(dist_latit/2)**2 + cos(lat_a) * cos(lat_b) * sin(dist_longit/2)**2

    # Calculate the central angle
    central_angle = 2 * asin(sqrt(area))
    radius = 6371

    # Calculate Distance
    distance = central_angle * radius

    return abs(round(distance, 2))

#create a user defined function to use it on our Spark DataFrame
udf_get_distance = F.udf(get_distance)


def main():
    try:
        #comment: Create SparkSession
        spark = (
            SparkSession
            .builder
            .master('yarn')
            .appName(f"{sname}_calculating_friend_recomendation_{date}")
            .getOrCreate()
        )
        logging.info(f"Create SparkSession successful!")
    except:
        logging.exception(f"SparkSession was not created!")


    logging.info("Incoming parametr: ")
    logging.info(f"sname: {sname}")
    logging.info(f"hdfs_path: {hdfs_path}")
    logging.info(f"geo_path: {geo_path}")
    logging.info(f"date: {date}")
    logging.info(f"depth: {depth}")
    
    
    #comment: Формируем списки для загрузки данных по разным event_type
    input_event_message_paths = input_event_paths(hdfs_path=hdfs_path, depth=depth, event='message', date=date, sname=sname)
    input_event_reaction_paths = input_event_paths(hdfs_path=hdfs_path, depth=depth, event='reaction', date=date, sname=sname)
    input_event_subscription_paths = input_event_paths(hdfs_path=hdfs_path, depth=depth, event='subscription', date=date, sname=sname)



#Получаем все подписки и удаляем дубликаты
    #F.col("lat").isNotNull() | (F.col("lon").isNotNull()) |
    #(F.col("event.channel_id").isNotNull() - нет таких записей
    df_all_subscriptions = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")
        .parquet(*input_event_subscription_paths)
        .where( (F.col('event.subscription_channel').isNotNull() & F.col('event.user').isNotNull()) )
        .select(
            F.col('event.subscription_channel').alias('channel_id')
            ,F.col('event.user').alias('user_id')
        )
        .distinct()         
    )

    #Перемножаем подписки (делаем иннер джоин по channel_id)
    #переписал запрос избавился от оконки избавился от дистиркта
    #Удаляем зеркальные дубли -  F.array_sort(F.array(*cols)) + .drop_duplicates(["arr"])
    cols = ['user_left', 'user_right']

    df_subscriptions = (
        df_all_subscriptions
        .withColumnRenamed("user_id", "user_left")
        .join(df_all_subscriptions.withColumnRenamed("user_id", "user_right"), on="channel_id", how='inner')
        .drop("channel_id")
        .withColumn("arr", 
                    F.array_sort(F.array(*cols))
                )
        .drop_duplicates(["arr"])
        .drop("channel_id", "arr")
        .filter(F.col("user_left") != F.col("user_right") )
        .withColumn("hash", F.hash(F.concat(F.col('user_left'),F.col('user_right') )  )   )
    )

    #создаем df по людям которые общались (переписывались - имеют пары message_from message_to и наоборот)
    #считываем из источника input_event_message_paths в df_user_messages
    #df_user_message_from_to - левая стора общения
    #объединяем df_user_message_from_to и df_user_message_to_from = df_user_communications
    df_user_messages_from_to = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")
        .parquet(*input_event_message_paths)
        .where( (F.col('event.message_from').isNotNull() & F.col('event.message_to').isNotNull()) )
        .select(
            F.col('event.message_from').alias('user_left')
            ,F.col('event.message_to').alias('user_right')
        )
        .distinct()
    )

    df_user_messages_to_from = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")
        .parquet(*input_event_message_paths)
        .where( (F.col('event.message_from').isNotNull() & F.col('event.message_to').isNotNull()) )
        .select(
            F.col('event.message_to').alias('user_left')
            ,F.col('event.message_from').alias('user_right')
        )
        .distinct()
    )

    #делаю добавление левых к правым и правых к левым потому что не известно какая комбинация встретится в df_subscriptions
    #filter(F.col("user_left") != F.col("user_right") ) - удаляем пользователей где левый равен правому
    df_user_communications = (
        df_user_messages_from_to
        .union(df_user_messages_to_from)
        .withColumn("arr", 
                    F.array_sort(F.array(*cols))
                )
        .drop_duplicates(["arr"])
        .drop("arr")
        .withColumn("hash", F.hash(F.concat(F.col('user_left'),F.col('user_right') )  )   )
        .filter(F.col("user_left") != F.col("user_right") )
    )

    #Вычитаем из df_subscriptions_without_communication = df_cross_subscriptions - df_user_communications по совадающим user_left; user_right
    #joinOn = [df_subscriptions.user_left == df_user_communications.user_left, df_subscriptions.user_right == df_user_communications.user_right]
    #с этим джоином (вычитанием мучился больше всего) - и до сих пор не понимаю почему после этого действия остается так много записей здесь должно было отфильроваться больше записей
    df_subscriptions_without_communication = (
        df_subscriptions
        .join(df_user_communications
            .withColumnRenamed("user_right", "user_right_temp")
            .withColumnRenamed("user_left", "user_left_temp"), on=["hash"], how='left'
        )
        .where(F.col("user_right_temp").isNull() )
        .drop("user_right_temp","user_left_temp", "hash" )
        .where(F.col("user_left") != 0)
        .filter(F.col("user_left") != F.col("user_right") )
    )

    #Получаем все подписки и удаляем дубликаты
    #F.col("lat").isNotNull() | (F.col("lon").isNotNull()) |
    #(F.col("event.channel_id").isNotNull() - нет таких записей
    df_events_messages = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")
        .parquet(*input_event_message_paths)
        .where( F.col("lat").isNotNull() | (F.col("lon").isNotNull()) )
        .select(
            F.col('event.message_from').alias('user_id')
            ,F.round(F.col("lat"), 2).alias('lat')
            ,F.round(F.col("lon"), 2).alias('lon')
            )
        .distinct()
    )


    #Получение подписок и координат с округлением до двух знаков в дробной части
    df_events_subscription = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")
        .parquet(*input_event_subscription_paths)
        .where( F.col("lat").isNotNull() | (F.col("lon").isNotNull()) )
        .select(
            F.col("event.user").alias('user_id')
            ,F.round(F.col("lat"), 2).alias('lat')
            ,F.round(F.col("lon"), 2).alias('lon')
            )
        .distinct()
    )

    #объединение координат сообщений и подписок
    df_events_coordinats = (
        df_events_subscription.union(df_events_messages)
        .distinct()
    )

    # Создаем df_events_subscription_coordinat с подписками и координатами на основе 
    #df_events_subscription_coordinat = df_subscriptions_without_communication as a join df_events_coordinats as b on a.user_id == b.user_left
    #df_events_subscription_coordinat = df_subscriptions_without_communication as a join df_events_coordinats as b on a.user_id == b.user_right
    #Прошу вашего профессионального совета - правильно ли я делаю этот этап?
    #Логически я хотел создать матрицу интересов пользователей, но представляю что на реальных данных будет слишком большое кол-во данных
    #Возможно нужно брать одно место например самое частое встречающееся (так как я делал округление координат пользователя)
    #либо брать кординаты только последнего события пользователя (тоже считаю хорошим вариантом - так как будет быстрее реагировать на изменения интересов человека)
    df_events_subscription_coordinat = (
        df_subscriptions_without_communication
        .join(df_events_coordinats
            .withColumnRenamed("user_id", "user_left")
            .withColumnRenamed("lon", "lon_left")
            .withColumnRenamed("lat", "lat_left")
            , on=["user_left"] , how="inner")
        .join(df_events_coordinats
            .withColumnRenamed("user_id", "user_right")
            .withColumnRenamed("lon", "lon_right")
            .withColumnRenamed("lat", "lat_right")
            , on=["user_right"] , how="inner")
    )

    #Считаем дистаницию df_distance - фильтруем и оставляем только те у которых расстояние <= 1км 
    df_distance = (
        df_events_subscription_coordinat
        .withColumn("distance", udf_get_distance(
            F.col("lon_left"), F.col("lat_left"), F.col("lon_right"), F.col("lat_right")
            ).cast(DoubleType())
        )
        .where(F.col("distance") <= 1.0 )
        .withColumnRenamed("lat_left", "lat")
        .withColumnRenamed("lon_left", "lon")
        .drop("lat_right", "lon_right", "distance")
    )


    #hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
    #citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"

    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)

    # Changa data type and column name
    df_citygeodata = df_csv.select(
        F.col("id").cast(LongType()).alias("city_id")
        ,(F.col("city")).alias("city_name")
        ,(F.col("lat")).cast(DoubleType()).alias("city_lat")
        ,(F.col("lon")).cast(DoubleType()).alias("city_lon")
    )

    #Перемножаем на координаты городов df_user_city (так как растояние 1 км между пользователями значит они находятся в одном городе и множно брать координаты одного человека для вычисления zone_id)
    #Считаем расстояние до города df_distance_city для вычисления zone_id фильтруем чтобы получить только один город для связки user_left; user_right
    # здесь делаю чекпоинт
    sc = SparkContext.getOrCreate(SparkConf())
    sc.setCheckpointDir(dirName=f"{hdfs_path}/user/{sname}/content")

    df_user_city = (
        df_distance.crossJoin(
            
            df_citygeodata.hint("broadcast")
        ).withColumn("distance", udf_get_distance(
                F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
                ).cast(DoubleType())
            )
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("user_left", "user_right").orderBy(F.col("distance").asc())
            )
        )
        .filter(F.col("row") == 1)
        .drop("row","lon", "lat", "city_lon", "city_lat", "distance", "channel_id")
        .withColumnRenamed("city_id", "zone_id")
        .distinct()
        .checkpoint()
    )

    #Формируем витрину
    df_friend_recomendation_analitics_mart = (
        df_user_city
        .withColumn("processed_dttm" , F.current_date()
                )
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col('city_name')) )
        .withColumn("local_time", F.from_utc_timestamp(F.col("processed_dttm"),F.col('timezone')))
        .drop("timezone", "city_name")
    )




    #comment: Сохранение витрины для аналитиков на hdfs 

    (
        df_friend_recomendation_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{sname}/marts/friend_recomendation")
    )

    #по вашей рекомендации сделал createOrReplaceTempView - но есть недопонимение как это потом будут использовать аналитики
    #в каком это формате сохраняется и где будет хранится)
    (
        df_friend_recomendation_analitics_mart.createOrReplaceTempView("df_friend_recomendation_analitics_mart")
    )


if __name__ == '__main__':
    main()