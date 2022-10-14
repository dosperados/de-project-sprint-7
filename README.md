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

### Основная задача:
- [x] Обновить структуру Data Lake
- [x] Создать витрину в разрезе пользователей
- [x] Создать витрину в разрезе зон
- [ ] Построить витрину для рекомендации друзей
- [x] Автоматизировать обновление витрин

### Структура витрины - в разрезе пользователей
#### Состав витрины - `df_user_analitics_mart`:
* `user_id` — идентификатор пользователя.
* `act_city` — актуальный адрес. Это город, из которого было отправлено последнее сообщение.
* `home_city` — домашний адрес. Это последний город, в котором пользователь был дольше 27 дней.
* `travel_count` — количество посещённых городов. Если пользователь побывал в каком-то городе повторно, то это считается за отдельное посещение.
* `travel_array` — список городов в порядке посещения.
* `local_time`  —  местное время 
	> Расчет местного времени функцией: **F.from_utc_timestamp(F.col("TIME_UTC"),F.col('timezone'))** где:
		 `TIME_UTC` — время в таблице событий. Указано в UTC+0.
		 `timezone` — актуальный адрес. Атрибуты содержатся в виде Australia/Sydney.

### Структура витрины - в разрезе зон
#### Состав витрины - `df_geo_analitics_mart`:
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

### Слои хранилища
* `RAW` - /user/master/data/geo/events/ (**hdfs**)
* `STG` - /user/dosperados/data/events/ (**hdfs**)
* `DDS - geo` - /user/dosperados/data/citygeodata/geo.csv (**hdfs**)
* `DDS - events` - /user/dosperados/data/events/date=yyyy-mm-dd (**hdfs**)
* `Mart` - /user/dosperados/data/marts


### Описание схемы RAW-слоя
~~Здест структура источника~~

### Описание схемы STG-слоя
~~Здест структура партиционированного слоя~~

## Схема взаимодействия
Airflow startr DAG -> PySpark read -> Hadoop -> PySpark calculation metric



## Запуск проекта
1. Загрузить **variables** из файла `/src/variables.json` в Airflow - Variable
2. Инициальная загрузка данных выполняется в ручную (один раз) далее используется скрипт инкрементальной загрузки и расчетов
3. ::::::::::::::::::::::::::::::::::::::::::::::::::::

### Структура репозитория

Внутри `src` расположены две папки:
- `/src/dags` - DAG - файлы
	- `DAG_initial_load.py` — Инициальная партиционорование данных  выполняется в ручную (однакратно)  
	- `DAG_main_calc_marts.py` — DAG производит запуск джобов расчета метрик
	
- `/src/sqripts` - py файлы c job'ами
	- ccc
