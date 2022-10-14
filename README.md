# Спринт 8 Проект 7 - Data Lake для социальной сети

### Обшее описание задачи
Коллеги из другого проекта по просьбе вашей команды начали вычислять координаты событий (сообщений, подписок, реакций, регистраций), которые совершили пользователи соцсети. Значения координат будут появляться в таблице событий. Пока определяется геопозиция только исходящих сообщений, но уже сейчас можно начать разрабатывать новый функционал.

В продукт планируют внедрить систему рекомендации друзей. Приложение будет предлагать пользователю написать человеку, если пользователь и адресат:
- состоят в одном канале,
- раньше никогда не переписывались,
- --находятся не дальше 1 км друг от друга.--

При этом команда хочет лучше изучить аудиторию соцсети, чтобы в будущем запустить монетизацию. Для этого было решено провести геоаналитику:
- Выяснить, где находится большинство пользователей по количеству сообщений, лайков и подписок из одной точки.
- Посмотреть, в какой точке Австралии регистрируется больше всего новых пользователей.
- Определить, как часто пользователи путешествуют и какие города выбирают.

Благодаря такой аналитике в соцсеть можно будет вставить рекламу: приложение сможет учитывать местонахождение пользователя и предлагать тому подходящие услуги компаний-партнёров.

### Основная цель:
- [x] Обновить структуру Data Lake
- [x] Создать витрину в разрезе пользователей
- [x] Создать витрину в разрезе зон
- [ ] Построить витрину для рекомендации друзей
- [x] Автоматизировать обновление витрин

#### Состав витрины - `df_user_analitics_mart` (в разрезе пользователей) :
* `user_id` — идентификатор пользователя.
* `act_city` — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
* `home_city` — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
* `travel_count` — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
* `travel_array` — список городов в порядке посещения.
* `local_time`  —  местное время 
	> Расчет местного времени функцией: **F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone'))** где:
		 `TIME_UTC` — время в таблице событий. Указано в UTC+0.
		 `timezone` — актуальный адрес. Атрибуты содержатся в виде Australia/Sydney.
```
+-------+--------+---------+------------+--------------------+-------------------+
|user_id|act_city|home_city|travel_count|        travel_array|         local_time|
+-------+--------+---------+------------+--------------------+-------------------+
|     26|  Sydney|     null|        null|[Launceston, Town...|2022-05-13 10:00:00|
|   1806|  Sydney|     null|           6|[Gold Coast, Bris...|2021-05-20 10:00:00|
|   3091|  Sydney|     null|        null|[Darwin, Cranbour...|2022-05-01 10:00:00|
|   7747|  Sydney|     null|        null|[Maitland, Melbou...|2021-04-28 10:00:00|
|   8440|  Sydney|     null|           2|[Mackay, Maitland...|2021-05-02 10:00:00|
|  10959|  Sydney|     null|           4|[Maitland, Canber...|2022-05-07 10:00:00|
|  11945|  Sydney|     null|        null|[Gold Coast, Canb...|2021-04-25 10:00:00|
|  12044|  Sydney|     null|        null|[Wollongong, Ball...|2022-05-19 10:00:00|
|  13248|  Sydney|     null|        null|[Rockhampton, Ben...|2021-04-27 10:00:00|
|  13518|  Sydney|     null|        null|[Cranbourne, Toow...|2021-05-08 10:00:00|
+-------+--------+---------+------------+--------------------+-------------------+
only showing top 10 rows

root
 |-- user_id: long (nullable = true)
 |-- act_city: string (nullable = true)
 |-- home_city: string (nullable = true)
 |-- travel_count: integer (nullable = true)
 |-- travel_array: array (nullable = true)
 |    |-- element: string (containsNull = false)
 |-- local_time: timestamp (nullable = true)
```

#### Состав витрины - `df_geo_analitics_mart` (разрезе зон):
* `month` — месяц расчёта;
* `week` — неделя расчёта;
* `zone_id` — идентификатор зоны (города);
* `week_message` — количество сообщений за неделю;
* `week_reaction` — количество реакций за неделю;
* `week_subscription` — количество подписок за неделю;
* `week_user` — количество регистраций за неделю;
* `month_message` — количество сообщений за месяц;
* `month_reaction` — количество реакций за месяц;
* `month_subscription` — количество подписок за месяц;
* `month_user` — количество регистраций за месяц.

```
+-------+-------------------+-------------------+------------+-------------+---------+----------+-------------+--------------+-----------------+------------------+
|zone_id|               week|              month|week_message|month_message|week_user|month_user|week_reaction|month_reaction|week_subscription|month_subscription|
+-------+-------------------+-------------------+------------+-------------+---------+----------+-------------+--------------+-----------------+------------------+
|     12|2022-05-09 00:00:00|2022-05-01 00:00:00|         233|          506|      143|       279|         6662|         18363|            12833|             35261|
|      3|2022-05-09 00:00:00|2022-05-01 00:00:00|         712|         1610|      418|       865|        23357|         63761|            44336|            122094|
|     14|2022-05-09 00:00:00|2022-05-01 00:00:00|          57|          125|       34|        64|         1923|          5159|             3619|              9958|
|      7|2022-05-09 00:00:00|2022-05-01 00:00:00|         292|          591|      137|       269|         7528|         20469|            14184|             38747|
|     17|2022-05-09 00:00:00|2022-05-01 00:00:00|         291|          624|      150|       315|         8764|         24133|            16900|             46148|
|      5|2022-05-09 00:00:00|2022-05-01 00:00:00|         260|          616|      161|       324|         8600|         24040|            16685|             45817|
|      4|2022-05-09 00:00:00|2022-05-01 00:00:00|         421|          923|      207|       421|        11821|         32580|            22464|             62105|
|     18|2022-05-09 00:00:00|2022-05-01 00:00:00|          84|          180|       47|       100|         2543|          7004|             4871|             13489|
|     22|2022-05-16 00:00:00|2022-05-01 00:00:00|         394|          756|      161|       324|        15338|         24118|            29285|             46196|
|     18|2022-05-16 00:00:00|2022-05-01 00:00:00|          96|          180|       53|       100|         4461|          7004|             8618|             13489|
+-------+-------------------+-------------------+------------+-------------+---------+----------+-------------+--------------+-----------------+------------------+
only showing top 10 rows
root
 |-- zone_id: long (nullable = true)
 |-- week: timestamp (nullable = true)
 |-- month: timestamp (nullable = true)
 |-- week_message: long (nullable = false)
 |-- month_message: long (nullable = false)
 |-- week_user: long (nullable = true)
 |-- month_user: long (nullable = true)
 |-- week_reaction: long (nullable = true)
 |-- month_reaction: long (nullable = true)
 |-- week_subscription: long (nullable = true)
 |-- month_subscription: long (nullable = true)
```

#### Слои хранилища
* `RAW` - /user/master/data/geo/events/ (**hdfs**)
* `STG` - /user/dosperados/data/events/ (**hdfs**)
* `DDS - geo` - /user/dosperados/data/citygeodata/geo.csv (**hdfs**)
* `DDS - events` - /user/dosperados/data/events/date=yyyy-mm-dd (**hdfs**)
* `Mart` - /user/dosperados/data/marts (**hdfs**)
	* geo  - разрезе зон
	* users - в разрезе пользователей


### Описание схемы RAW-слоя
```
root
 |-- event: struct (nullable = true)
 |    |-- admins: array (nullable = true)
 |    |    |-- element: long (containsNull = true)
 |    |-- channel_id: long (nullable = true)
 |    |-- datetime: string (nullable = true)
 |    |-- media: struct (nullable = true)
 |    |    |-- media_type: string (nullable = true)
 |    |    |-- src: string (nullable = true)
 |    |-- message: string (nullable = true)
 |    |-- message_channel_to: long (nullable = true)
 |    |-- message_from: long (nullable = true)
 |    |-- message_group: long (nullable = true)
 |    |-- message_id: long (nullable = true)
 |    |-- message_to: long (nullable = true)
 |    |-- message_ts: string (nullable = true)
 |    |-- reaction_from: string (nullable = true)
 |    |-- reaction_type: string (nullable = true)
 |    |-- subscription_channel: long (nullable = true)
 |    |-- subscription_user: string (nullable = true)
 |    |-- tags: array (nullable = true)
 |    |    |-- element: string (containsNull = true)
 |    |-- user: string (nullable = true)
 |-- event_type: string (nullable = true)
 |-- lat: double (nullable = true)
 |-- lon: double (nullable = true)
 
 +--------------------+------------+-------------------+------------------+
|               event|  event_type|                lat|               lon|
+--------------------+------------+-------------------+------------------+
|[,, 2022-05-21 02...|subscription|-33.543102424518636|151.48624064210895|
|[[103235], 859291...|     message| -33.94742527623598|151.32387878072961|
|[,, 2022-05-21 12...|    reaction| -37.12220179510437|144.28231815733022|
|[,, 2022-05-21 17...|subscription| -20.81900702450072|149.59015699102054|
|[[107808], 865707...|     message|-11.539551427762717|131.17148068495302|
+--------------------+------------+-------------------+------------------+
```

## Схема взаимодействия
Airflow startr DAG -> PySpark read -> Hadoop -> PySpark calculation metric -> Store result Hadoop



## Запуск проекта
1. Загрузить **variables** из файла `/src/variables.json` в Airflow - Variable
2. Инициальная загрузка данных **DAG_initial_load.py** выполняется в ручную (один раз) далее используется скрипт инкрементальной загрузки и расчетов
3. Обновление STG слоя - **DAG_update_stg.py** автозапуск каждый день (ХОТЕЛ НАСТРОИТЬ ПОСЛЕДОВАТЕЛЬНЫЙ ВЫЗОВ СЛЕДУЩЕГО **DAG_main_calc_marts.py** - не знаю как, если поможете буду благодарен)
4. Для расчета витрины запустить **DAG_main_calc_marts.py**
5. Результаты расчетов сохранены в формате `parquet` в соответвующих папках
	- hdfs:/user/dosperados/marts/users
	- hdfs:/user/dosperados/marts/geo

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- `DAG_initial_load.py` — Инициальная партиционорование данных  выполняется в ручную (однакратно)  
	- `DAG_main_calc_marts.py` — DAG производит запуск джобов расчета метрик
	- `DAG_update_stg.py` — DAG обновляет слой STG из источника и автоматически запускает **DAG_main_calc_marts.py** для расчета метирик ветрины.
	
- `/src/sqripts` - py файлы c job'ами
	- `initial_load_job.py` — Job инициальной загрузки.
	- `calculating_user_analitics_job.py` — Job расчета пользовательских метрик и сохранения витрины.
	- `calculating_geo_analitics_job.py` — Job расчета geo метрик и сохранения витрины.
	- `update_stg_by_date_job.py`  — Job обновления STG.
