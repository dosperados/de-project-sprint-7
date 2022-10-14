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
~~Здест структура источника~~

### Описание схемы STG-слоя
~~Здест структура партиционированного слоя~~

## Схема взаимодействия
Airflow startr DAG -> PySpark read -> Hadoop -> PySpark calculation metric -> Store result Hadoop



## Запуск проекта
1. Загрузить **variables** из файла `/src/variables.json` в Airflow - Variable
2. Инициальная загрузка данных выполняется в ручную (один раз) далее используется скрипт инкрементальной загрузки и расчетов
3. ::::::::::::::::::::::::::::::::::::::::::::::::::::

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- `DAG_initial_load.py` — Инициальная партиционорование данных  выполняется в ручную (однакратно)  
	- `DAG_main_calc_marts.py` — DAG производит запуск джобов расчета метрик
	- `DAG_update_stg.py` — DAG обновляет слой STG из источника и автоматически запускает **DAG_main_calc_marts.py** для расчета метирик ветрины.
	
- `/src/sqripts` - py файлы c job'ами
	- `initial_load_job.py` — 
	- `calculating_user_analitics_job.py` — 
	- `calculating_geo_analitics_job.py` — 
