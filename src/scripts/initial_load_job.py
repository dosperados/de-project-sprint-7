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


# пример джобы
# /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster initial_load_job.py dosperados hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/


def main():
    try: 

        # задаем все переменные далее по коду они будут обозначены где они используются
        sname = sys.argv[1] #"dosperados" 
        hdfs_path = sys.argv[2] #"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
        geo_path = sys.argv[3]  #"/user/master/data/geo/events/"
        citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
        logging.info("Incoming parametr: ")
        logging.info(f"sname: {sname}")
        logging.info(f"hdfs_path: {hdfs_path}")
        logging.info(f"geo_path: {geo_path}")
        
        try:
            #Create SparkSession
            spark = (
                SparkSession
                .builder
                .master('yarn')
                .appName(f"{sname}_initial_load")
                .getOrCreate()
            )
            logging.info(f"Create SparkSession successful!")
        except:
            logging.exception(f"SparkSession was not created!")

        #Read from source
        #hdfs_path = "hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
        #geo_path = "/user/master/data/geo/events/"
        #sname = "dosperados"

        events = spark.read\
                    .option("basePath", f"{hdfs_path}{geo_path}")\
                    .parquet(f"{hdfs_path}{geo_path}")\

        #Save in parquet and partition by "date", "event_type" for easy read work with df
        events.write \
                .partitionBy("date", "event_type") \
                .mode("overwrite") \
                .parquet(f"{hdfs_path}/user/{sname}/data/events")


    except:
        logging.exception("An exception was thrown!")



if __name__ == '__main__':
    main()


#однократное копирование статичного справочника городов сделано вручную и не требуется перезаливка
#Copy static file with city geolocation data
#! hdfs dfs -put geo.csv hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/dosperados/data/citygeodata