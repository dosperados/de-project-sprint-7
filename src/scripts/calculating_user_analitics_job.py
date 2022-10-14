import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession

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
            .appName(f"{sname}_calculating_user_by_date_{date}_with_depth_{depth}")
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
    
    input_event_paths = input_event_paths(hdfs_path=hdfs_path, date=date, depth=depth, event='message', sname=sname)
    
    #comment : Если в поле event.datetime == Null тогда брать поле event.message_ts
    #Загрузка всех сообщений
    df_message = (
    spark.read
    .option("basePath", f"{hdfs_path}/user/{sname}/data/events")\
    .parquet(*input_event_paths)
    .select(F.col('event.message_from').alias('user_id')
            ,F.date_trunc("day",F.coalesce(F.col('event.datetime'),F.col('event.message_ts'))).alias("date")
            ,'lat', 'lon')
    .distinct()
    )

    #citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)

    ##comment: Change data type and column name for static city table
    df_citygeodata = df_csv.select(
        F.col("id").cast(LongType()).alias("city_id")
        ,(F.col("city")).alias("city_name")
        ,(F.col("lat")).cast(DoubleType()).alias("city_lat")
        ,(F.col("lon")).cast(DoubleType()).alias("city_lon")
    )

    #Broancast crossJoin df_citygeodata
    #comment: Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_message_and_citygeodata = df_message.crossJoin(
        df_citygeodata.hint("broadcast")
    )

    #comment: Получение DF с дистранцией (distance) до города
    #дистанция считается функцией get_distance() формула описана в документации
    df_message_and_distance = df_message_and_citygeodata.select(
        "user_id"
        ,"date"
        ,"city_name"
        ,udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
        ).cast(DoubleType()).alias("distance")
    )

    #comment: расчет показателя act_city по пользователям
    df_act_city = (
        df_message_and_distance
        .withColumn("row",F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("date").desc())
            ))
        .filter(F.col("row") == 1)
        .drop("row", "distance")
        .withColumnRenamed("user_id", "act_city_user_id")
        .withColumnRenamed("city_name", "act_city")
    )

    #comment: расчет домашнего города по условию что сотрудник находился в этом городе 27 дней или более
    df_home_city = (
        df_message_and_distance
        .withColumn("row_date",F.row_number().over(
            Window.partitionBy("user_id", "city_name").orderBy(F.col("date").desc())
            ))
        .filter(F.col("row_date") >= 27)
        .withColumn("row_by_row_date", F.row_number().over(
            Window.partitionBy("user_id").orderBy(F.col("row_date").desc())
            ))
        .filter(F.col("row_by_row_date") == 1)
        .withColumnRenamed("user_id", "home_city_user_id")
        .withColumnRenamed("city_name", "home_city")
        .drop("row_date", "row_by_row_date", "distance", "date")
    )

    #comment: считаем количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
    df_travel_count = (
        df_message_and_distance
        .withColumn("travel_count",F.rank().over(
            Window.partitionBy("user_id", "city_name").orderBy(F.col("date").desc())
            ))
        .filter(F.col("travel_count") > 1)
        .drop("distance", "date")
        .groupBy("user_id").agg(F.max("travel_count").alias("travel_count"))
        .withColumnRenamed("user_id", "travel_count_user_id")
    )

    #comment: формируем список городов в порядке посещения.
    df_travel_array = (
        df_message_and_distance
        .distinct()
        .groupBy("user_id")
        .agg(F.collect_list('city_name').alias('travel_array'))
        .drop("date")
        .withColumnRenamed("user_id", "travel_array_user_id")
    )

    #comment: расчет метики local_time - формула расчета описана в документации
    df_local_time = (
        df_act_city
        .withColumn("timezone", F.concat(F.lit("Australia/"), F.col('act_city')) )
        .withColumn("local_time", F.from_utc_timestamp(F.col("date"),F.col('timezone')))
        .drop("timezone", "date", "act_city")
        .withColumnRenamed("act_city_user_id", "local_time_user_id")
    )

    #comment: объединения всех метрик в одину ветрину
    df_user_analitics_mart = (
        df_act_city
        .select("act_city_user_id", "act_city")
        .join(df_home_city.select("home_city_user_id", "home_city"), df_act_city.act_city_user_id == df_home_city.home_city_user_id, how='left')
        .join(df_travel_count,  df_act_city.act_city_user_id == df_travel_count.travel_count_user_id, how='left') #куратор прошу здесь подсказки/совета я сталкнулся с трудностью что если в каждом df ключевое поле имеет одинаковое имя я не смог удалить лишние стлобцы командой .drop(df2.id) в связи с этим прошлось поле user_id переименовывать в каждом df чтобв они не повторялись
        .join(df_travel_array,  df_act_city.act_city_user_id == df_travel_array.travel_array_user_id, how='left')
        .join(df_local_time,    df_act_city.act_city_user_id == df_local_time.local_time_user_id, how='left')
        .drop("home_city_user_id",  "travel_count_user_id", "travel_array_user_id", "local_time_user_id")
        .withColumnRenamed("act_city_user_id", "user_id")
    )

    #comment: Сохранение витрины для аналитиков на hdfs 
    #тут также прошо помощи и разъеснений как правильно делать и где обычно хранят такие резальтаты
    #я бы сохранал бы конечно в БД, но в предоставленной инфраструктуре нет реляционной базы для сохранения результата
    (
        df_user_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{sname}/marts/users")
    )



if __name__ == '__main__':
    main()