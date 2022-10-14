#импорт библиотек для инициализации спарка
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
import os
import findspark
findspark.init()
findspark.find()
from pyspark.sql import SparkSession

#переменные окружения спарка
os.environ['HADOOP_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['YARN_CONF_DIR'] = '/etc/hadoop/conf'
os.environ['JAVA_HOME']='/usr'
os.environ['SPARK_HOME'] ='/usr/lib/spark'
os.environ['PYTHONPATH'] ='/usr/local/lib/python3.8'

#импорт библиотек для DAG
from airflow import DAG
from datetime import datetime, timedelta
import sys
import logging
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#создаем логирование
TASK_LOGGER = logging.getLogger('project_7')
TASK_LOGGER.info('Main calc metrics and marts')

#задаем все переменные далее по коду (они будут обозначены коментариями в коде где они используются)
sname = Variable.get("project_7_sname") #"dosperados"
hdfs_path = Variable.get("project_7_hdfs_path") #"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = Variable.get("project_7_geo_path")#"/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"
date = '2022-05-21'
depth = Variable.get("project_7_depth") #28


## /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster /scripts/update_stg_by_date_job.py dosperados hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/ 2022-05-21 28
## spark-submit --master yarn --deploy-mode cluster /scripts/update_stg_by_date_job.py sname hdfs_path geo_path date depth

args = {
    "owner": "dosperados",
    'email': ['dosperados@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'Dayly_update_stg',
        default_args=args,
        description='Launch pyspark job for update stg layer and call the calculating DAG',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2022, 10, 14),
        tags=['pyspark', 'hadoop', 'hdfs', 'project'],
        is_paused_upon_creation=True,
) as dag:

    start_task = DummyOperator(task_id='start')

    update_stg_by_date_task = SparkSubmitOperator(
        task_id='update_stg_by_date_job',
        application ='/scripts/update_stg_by_date_job.py' ,
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path, date, depth],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> update_stg_by_date_task  >> end_task