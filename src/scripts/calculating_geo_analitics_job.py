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
            .appName(f"{sname}_calculating_GEO_by_date_{date}_with_depth_{depth}")
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
    


    #comment: Сохранение витрины для аналитиков на hdfs 
    #тут также прошо помощи и разъеснений как правильно делать и где обычно хранят такие резальтаты
    #я бы сохранал бы конечно в БД, но в предоставленной инфраструктуре нет реляционной базы для сохранения результата
    (
        df_user_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{sname}/marts/geo")
    )

    #comment: Сохраняем вы события message с заполненными координатами
    df_all_message = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")\
        .parquet(*input_event_message_paths)
        .where(F.col("lat").isNotNull() | (F.col("lon").isNotNull())) #события с пустыми полями координат не пригодны для анализа (вопрос качество данных) - было принято решение удалить эти записи так как от нет пользы
        .select(
            F.col('event.message_from').alias('user_id')
            ,F.col('event.message_id').alias('message_id')
            #,F.coalesce(F.col('event.datetime'),F.col('event.message_ts')).alias("event_date")
            ,'lat', 'lon',"date"
            )
        #.withColumn("event_type", F.lit("message"))
    )

    #comment: Сохраняем вы события reaction с заполненными координатами
    df_all_reaction = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")\
        .parquet(*input_event_reaction_paths)
        .where(F.col("lat").isNotNull() | (F.col("lon").isNotNull())) #события с пустыми полями координат не пригодны для анализа (вопрос качество данных) - было принято решение удалить эти записи так как от нет пользы
        .select(
            F.col('event.message_id').alias('message_id')
            ,F.coalesce(F.col('event.datetime'),F.col('event.message_ts')).alias("event_date")
            ,'lat', 'lon',"date"
            )
        #.withColumn("event_type", F.lit("reaction"))
    )

    #comment: Сохраняем вы события subscription с заполненными координатами
    df_all_subscription = (
        spark.read
        .option("basePath", f"{hdfs_path}/user/{sname}/data/events")\
        .parquet(*input_event_subscription_paths)
        .where(F.col("lat").isNotNull() | (F.col("lon").isNotNull())) #события с пустыми полями координат не пригодны для анализа (вопрос качество данных) - было принято решение удалить эти записи так как от нет пользы
        .select(
            F.col('event.user').alias('user')
            ,'lat', 'lon',"date"
            )
        #.withColumn("event_type", F.lit("subscription"))
    )

    #hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
    #citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
    df_csv = spark.read.csv(citygeodata_csv, sep = ';', header = True)

    #Changa data type and column name
    df_citygeodata = df_csv.select(
        F.col("id").cast(LongType()).alias("city_id")
        ,(F.col("city")).alias("city_name")
        ,(F.col("lat")).cast(DoubleType()).alias("city_lat")
        ,(F.col("lon")).cast(DoubleType()).alias("city_lon")
    )

    # Broancast crossJoin df_citygeodata
    #comment Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_message_and_citygeodata = df_all_message.crossJoin(
        df_citygeodata.hint("broadcast")
    )

    # Broancast crossJoin df_citygeodata
    #comment Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_reaction_and_citygeodata = df_all_reaction.crossJoin(
        df_citygeodata.hint("broadcast")
    )

    # Broancast crossJoin df_citygeodata
    #comment Получение DF события перемноженные на список городов - для дальнейшего вычисления растояния до города
    df_all_subscription_and_citygeodata = df_all_subscription.crossJoin(
        df_citygeodata.hint("broadcast")
    )

    #comment: Получение DF с дистранцией (distance) до города
    df_distance_message = (
        df_all_message_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())
        )
    )

    #comment: Получение DF с дистранцией (distance) до города
    df_distance_reaction = (
        df_all_reaction_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())
        )
    )

    #comment: Получение DF с дистранцией (distance) до города
    df_distance_subscription = (
        df_all_subscription_and_citygeodata
        .withColumn("distance", udf_get_distance(
            F.col("lon"), F.col("lat"), F.col("city_lon"), F.col("city_lat")
            ).cast(DoubleType())
        )
    )

    # получение ближайшего города
    df_city_message = (
        df_distance_message
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("user_id", "message_id").orderBy(F.col("distance").asc())
            )
        )
        .filter(F.col("row") == 1)
        .select(
            "user_id", "message_id", "date", "city_id", "city_name"
        )
        .withColumnRenamed("city_id", "zone_id")
    )

    # получение ближайшего города
    df_city_reaction = (
        df_distance_reaction
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("message_id", "date", "lat", "lon").orderBy(F.col("distance").asc())
            )
        )
        .filter(F.col("row") == 1)
        .select(
            "message_id", "date", "city_id", "city_name"
        )
        .withColumnRenamed("city_id", "zone_id")
    )

    # получение ближайшего города
    df_city_subscription = (
        df_distance_subscription
        .withColumn("row" ,F.row_number().over(
                Window.partitionBy("lat", "lon", "date").orderBy(F.col("distance").asc())
            )
        )
        .filter(F.col("row") == 1)
        .select(
            "user", "date", "city_id", "city_name"
        )
        .withColumnRenamed("city_id", "zone_id")
    )

    #zone_id — идентификатор зоны (города);
    #week — неделя расчёта;
    #month — месяц расчёта;
    #week_message — количество сообщений за неделю;
    #month_message — количество сообщений за месяц;
    df_count_message = (
        df_city_message
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_message",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "week"))
                        )
                    )
        .withColumn("month_message",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "month"))
                        )
                    )
        .groupBy("zone_id", "week", "month", "week_message", "month_message").agg(F.max("month_message").alias("max"))
        .drop("max")
        .withColumn("hash", F.hash(F.concat(F.col('zone_id'),F.col('week'),F.col('month') )  )   )
        )

    #zone_id — идентификатор зоны (города);
    #week — неделя расчёта;
    #month — месяц расчёта;
    #week_user — количество регистраций за неделю;
    #month_user — количество регистраций за месяц.
    df_count_reg = (
        df_city_message
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        #.groupBy("zone_id", "week", "month").agg(F.count("message_id").alias("week_message"))
        .withColumn("row",
                    (
                        F.row_number().over(
                            Window.partitionBy("user_id").orderBy(F.col("date").asc())
                        )
                    )
                )
        .filter(F.col("row") == 1)
        .withColumn("week_user",
                    (
                        F.count("row").over(
                            Window.partitionBy("zone_id", "week"))
                        )
                    )
        .withColumn("month_user",
                    (
                        F.count("row").over(
                            Window.partitionBy("zone_id", "month"))
                        )
                    )
        .groupBy("zone_id", "week", "month", "week_user", "month_user").agg(F.max("week_user").alias("max"))
        .drop("max")
        .withColumn("hash", F.hash(F.concat(F.col('zone_id'),F.col('week'),F.col('month') )  )   )
        )
    
    #zone_id — идентификатор зоны (города);
    #week — неделя расчёта;
    #month — месяц расчёта;
    #week_reaction — количество реакций за неделю;
    #month_reaction — количество реакций за месяц;
    df_count_reaction = (
        df_city_reaction
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_reaction",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "week"))
                        )
                    )
        .withColumn("month_reaction",
                    (
                        F.count("message_id").over(
                            Window.partitionBy("zone_id", "month"))
                        )
                    )
        .groupBy("zone_id", "week", "month", "week_reaction", "month_reaction").agg(F.max("month_reaction").alias("max"))
        .drop("max")
        .withColumn("hash", F.hash(F.concat(F.col('zone_id'),F.col('week'),F.col('month') )  )   )
        )
    
    #zone_id — идентификатор зоны (города);
    #week — неделя расчёта;
    #month — месяц расчёта;
    #week_subscription — количество подписок за неделю;
    #month_subscription — количество подписок за месяц;

    df_count_subscription = (
        df_city_subscription
        .withColumn("week", F.date_trunc("week",F.col('date')))
        .withColumn("month", F.date_trunc("month",F.col('date')))
        .withColumn("week_subscription",
                    (
                        F.count("user").over(
                            Window.partitionBy("zone_id", "week"))
                        )
                    )
        .withColumn("month_subscription",
                    (
                        F.count("user").over(
                            Window.partitionBy("zone_id", "month"))
                        )
                    )
        .groupBy("zone_id", "week", "month", "week_subscription", "month_subscription").agg(F.max("month_subscription").alias("max"))
        .drop("max")
        .withColumn("hash", F.hash(F.concat(F.col('zone_id'),F.col('week'),F.col('month') )  )   )
        )

    #Моя задумка была сделать отдельные df по нужным метрикам потом объеденить все в одну ветрину связав по трем полям "zone_id", "week", "month"
    #но в спарк left join со связкой по трем полям не получилось у меня сделать, если все же такое возможно прошу сразу написать на моем примере
    #в итоге я сделал обходным путем создав сурогатный ключ через hash("zone_id", "week", "month")

    # объединения всех метрик в одину ветрину
    #df_count_message
    #df_count_reg
    #df_count_reaction
    #df_count_subscription
    df_geo_analitics_mart = (
        df_count_message
        .join(df_count_reg.select("week_user", "month_user", "hash"), on=["hash"], how='left')
        .join(df_count_reaction.select("week_reaction", "month_reaction", "hash"), on=["hash"], how='left')
        .join(df_count_subscription.select("week_subscription", "month_subscription", "hash"), on=["hash"], how='left')
        .drop("hash")

    )
    #кстати конструкцию on=["hash"] - считаю очень красивой выглядит локанично и понятно! (долго искал как упростить написание джойна и нашел - google что бы мы без тебя делали))))


    #comment: Сохранение витрины для аналитиков на hdfs 
    #тут также прошо помощи и разъеснений как правильно делать и где обычно хранят такие резальтаты
    #я бы сохранал бы конечно в БД, но в предоставленной инфраструктуре нет реляционной базы для сохранения результата
    (
        df_geo_analitics_mart
        .write
            .mode("overwrite") 
            .parquet(f"{hdfs_path}/user/{sname}/marts/geo")
    )


if __name__ == '__main__':
    main()