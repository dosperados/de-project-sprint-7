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
from datetime import date, datetime, timedelta
import sys
import logging
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

#создаем логирование
TASK_LOGGER = logging.getLogger('project_7')
TASK_LOGGER.info('Full (first) initianal load from source')

#задаем все переменные далее по коду (они будут обозначены коментариями в коде где они используются)
sname = Variable.get("project_7_sname") #"dosperados"
hdfs_path = Variable.get("project_7_hdfs_path") #"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020"
geo_path = Variable.get("project_7_geo_path")#"/user/master/data/geo/events/"
citygeodata_csv = f"{hdfs_path}/user/{sname}/data/citygeodata/geo.csv"

## /usr/lib/spark/bin/spark-submit --master yarn --deploy-mode cluster initial_load_job.py dosperados hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020 /user/master/data/geo/events/
## spark-submit --master yarn --deploy-mode cluster initial_load_job.py sname hdfs_path geo_path

args = {
    "owner": "dosperados",
    'email': ['dosperados@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0
}

with DAG(
        'Initial_full_migration_by_date_and_evend_type',
        default_args=args,
        description='Launch pyspark job for migration data',
        catchup=False,
        schedule_interval=None,
        start_date=datetime(2022, 10, 14),
        tags=['pyspark', 'hadoop', 'hdfs', 'project'],
        is_paused_upon_creation=True,
) as dag:

    start_task = DummyOperator(task_id='start')

    initial_load_task = SparkSubmitOperator(
        task_id='initial_load_job',
        application ='/scripts/initial_load_job.py' ,
        conn_id= 'yarn_spark',
        application_args = [sname, hdfs_path, geo_path],
        conf={
        "spark.driver.maxResultSize": "20g"
        },
        executor_cores = 2,
        executor_memory = '4g'
    )

    end_task = DummyOperator(task_id='end')

    start_task >> initial_load_task >> end_task