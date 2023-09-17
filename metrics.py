from loguru import logger
import pandas as pd
import datetime as dt
import impala.dbapi
import numpy as np
import warnings
import calendar

import os
from pyspark.sql import SparkSession

warnings.filterwarnings('ignore')
from impala.dbapi import connect
from datetime import timedelta, date
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable, XCom, TaskInstance


# Изменяемые под себя настройки для корректной работы дага в среде
setup_table = 'test.dq_setup_table' # настроечная таблица
table_for_recording = 'test.dq_result_table' # таблица для записи
deviations_table = ... # таблица отклонений

spark = SparkSession \
    .builder \
    .appName("spark") \
    .config("spark.jars", "hadoop-aws-3.3.2.jar,aws-java-sdk-bundle-1.11.1026.jar") \
    .config("spark.sql.catalogImplementation", "hive") \
    .config("spark.hadoop.hive.metastore.uris", "thrift://hive-metastore.hive-metastore:9083") \
    .config("spark.sql.warehouse.dir", "s3a://metastore/") \
    .config("spark.hadoop.fs.s3a.access.key", "4AOV3JSFNIGKEL2IMP6U") \
    .config("spark.hadoop.fs.s3a.secret.key", "fOaSMCzMyHFDpWXA7in9PIx56BFB3xehZ4RNJKJs") \
    .config("spark.hadoop.fs.s3a.endpoint", os.environ.get("FEAST_S3_ENDPOINT_URL")) \
    .config("spark.sql.debug.maxToStringFields", "100") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false") \
    .enableHiveSupport() \
    .getOrCreate()


import datetime as dt
import numpy as np
import pandas as pd
from loguru import logger


ROWS_AMOUNT_IN_ITERATION = 50


def get_statistics_for_date(spark, calculation_date: dt.date, table_name: str, date_field: str):
    logger.info(f"Расчёт статистики для таблицы {table_name} за {calculation_date}")

    # Извлечение структуры из таблицы
    df_table_struct = spark.sql(f"DESCRIBE {table_name};").toPandas()

    # Замена типов данных формата "varchar(*)" на "varchar"
    df_table_struct['data_type'] = df_table_struct['data_type'].str.split('(').str[0]

    df_table_struct = df_table_struct[~df_table_struct['col_name'].isin([date_field])]

    # Отделение числовых полей
    df_table_struct_num = df_table_struct[~df_table_struct['data_type'].isin(['string', 'timestamp', 'varchar'])] \
        .reset_index(drop=True)

    # Отделение строковых полей и дат
    df_table_struct_str = df_table_struct[df_table_struct['data_type'].isin(['string', 'timestamp', 'varchar'])] \
        .reset_index(drop=True)

    columns = [
        'table_name', 'column_name', 'calculation_date',
        'rows_amount_cnt', 'amount_completed_cnt', 'nulls_amount_cnt',
        'distinct_amount_cnt', 'minimum_value_info', 'maximum_value_info',
        'median_value_info', 'average_value_info', 'sum_attributes_nval',
    ]

    df_result_num = pd.DataFrame()
    num_columns_cnt = df_table_struct_num.shape[0]
    indices = [ROWS_AMOUNT_IN_ITERATION * (i + 1) for i in range(int(num_columns_cnt / ROWS_AMOUNT_IN_ITERATION))]
    for array in np.split(df_table_struct_num.index, indices):
        if len(array) > 0:
            query = ""
            for index, row in df_table_struct_num[df_table_struct_num.index.isin(array)].iterrows():
                query += f"""
                    SELECT
                        '{table_name}' table_name,
                        '{row['col_name']}' column_name,
                        to_date({date_field}) calculation_date,
                        COUNT(*) rows_amount_cnt,
                        COUNT({row['col_name']}) amount_completed_cnt,
                        (COUNT(*) - COUNT({row['col_name']})) nulls_amount_cnt,
                        COUNT(DISTINCT CAST({row['col_name']} as Decimal(38,6))) distinct_amount_cnt,
                        MIN(CAST({row['col_name']} as Decimal(38,6))) minimum_value_info,
                        MAX(CAST({row['col_name']} as Decimal(38,6))) maximum_value_info,
                        PERCENTILE(CAST({row['col_name']} as BIGINT), 0.5) median_value_info,
                        AVG(CAST({row['col_name']} as Decimal(38,6))) average_value_info,
                        SUM(CAST({row['col_name']} as Decimal(38,6))) sum_attributes_nval
                    FROM {table_name}
                    WHERE to_date({date_field}) = '{calculation_date}'
                    GROUP BY column_name, calculation_date UNION ALL"""
            query = f"{query[:-9]} ORDER BY column_name, calculation_date;"
            df_result_num = pd.concat([df_result_num, spark.sql(query).toPandas()])

    df_result_str = pd.DataFrame()
    str_columns_cnt = df_table_struct_str.shape[0]
    indices = [ROWS_AMOUNT_IN_ITERATION * (i + 1) for i in range(int(str_columns_cnt / ROWS_AMOUNT_IN_ITERATION))]
    for array in np.split(df_table_struct_str.index, indices):
        if len(array) > 0:
            query = ""
            for index, row in df_table_struct_str[df_table_struct_str.index.isin(array)].iterrows():
                query += f"""
                    SELECT
                        '{table_name}' table_name,
                        '{row['col_name']}' column_name,
                        to_date({date_field}) calculation_date,
                        COUNT(*) rows_amount_cnt,
                        COUNT({row['col_name']}) amount_completed_cnt,
                        (COUNT(*) - COUNT({row['col_name']})) nulls_amount_cnt,
                        COUNT(DISTINCT {row['col_name']}) distinct_amount_cnt,
                        NULL minimum_value_info,
                        NULL maximum_value_info,
                        NULL median_value_info,
                        NULL average_value_info,
                        NULL sum_attributes_nval
                    FROM {table_name}
                    WHERE to_date({date_field}) = '{calculation_date}'
                    GROUP BY column_name, calculation_date UNION ALL"""
            query = f"{query[:-9]} ORDER BY column_name, calculation_date"
            df_result_str = pd.concat([df_result_str, spark.sql(query).toPandas()])

    # Объединение результатов двух расчётов
    df_result = pd.concat([df_result_num, df_result_str])

    # Для float указывается тип object, исправим
    float_fields = [
        'minimum_value_info',
        'maximum_value_info',
        'average_value_info',
        'median_value_info',
        'sum_attributes_nval',
    ]
    for float_field in float_fields:
        df_result[float_field] = df_result[float_field].astype('float64')

    # Добавим индекс столбцов, чтобы сортировать результаты в порядке, совпадающем с исходной таблицей
    res_columns = columns[:2] + ['index', 'data_type', 'comment'] + columns[2:]
    df_result = pd.merge(df_result,
                         df_table_struct[['col_name', 'data_type', 'comment']].reset_index(),
                         left_index=True, right_index=True)
    df_result['index'] = df_result['index'] + 1
    df_result = df_result[res_columns].sort_values(['index', 'calculation_date'])
    df_result['dublicate_cnt'] = df_result.duplicated().sum()
    df_result.rename({"index": "column_id"}, axis=1, inplace=True)
    logger.info(f'Выполнен расчёт статистики за {calculation_date}')
    df_result['report_dttm'] = pd.Timestamp('now')
    return df_result


def metriks_func():

    table_temp = spark.sql(sqlQuery=f"""
                SELECT
                    TABLE_NM as TABLE_NM,
                    TABLE_REPORT_DT_NM as TABLE_REPORT_DT_NM,
                    NEXT_CHECK_DT as NEXT_CHECK_DT,
                    INTERVAL_TERM as INTERVAL_TERM, 
                    UPDATE_TABLE_DT as UPDATE_TABLE_DT
                FROM {setup_table}; 
                """).toPandas()

    # получаем текущие даты обновления для записанных таблиц
    update_table_now_dt = []
    flg = []
    for index, row in table_temp.iterrows():
        df_check = spark.sql(f"DESCRIBE {row['TABLE_NM']};").toPandas()
        type_field = df_check.loc[df_check['column_name'] == row['TABLE_REPORT_DT_NM'], 'data_type'].iloc[0]
        if type_field == 'timestamp':
            spark.sql(f"""DROP TABLE IF EXISTS {row['TABLE_NM']}_v;""")
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS {row['TABLE_NM']}_v AS
                SELECT
                    CAST(trunc({row['TABLE_REPORT_DT_NM']},'dd') as string) as report_date, *
                FROM {row['TABLE_NM']};
            """)
            flg.append('1')
            row['TABLE_NM'] = row['TABLE_NM'] + '_v'
            row['TABLE_REPORT_DT_NM'] = 'report_date'
        else:
            flg.append('0')
        update_data = spark.sql(f"""SELECT MAX({row['TABLE_REPORT_DT_NM']}) FROM {row['TABLE_NM']};""").toPandas()
        update_table_now_dt.append(str(update_data.iloc[0]['report_date']))

    table_temp['update_table_now_dt'] = update_table_now_dt
    logging.info(flg)
    table_temp['flg'] = flg
    logging.info(f"список дат обновлений таблиц - {update_table_now_dt}")

    # фильтр - текущая дата обновления должна быть больше записанной даты обновления в настроечной таблице
    table_temp_2 = table_temp.loc[table_temp['UPDATE_TABLE_DT'] <= table_temp['update_table_now_dt']]

    # считаем проверки
    for index, row in table_temp_2.iterrows():
        if row['flg'] == '1':
            table_name = row['TABLE_NM'] + '_v'
            name_field = 'report_date'

            spark.sql(sqlQuery=f"""
                UPDATE {table_for_recording}
                SET table_name = '{table_name}'
                WHERE table_name = '{row['TABLE_NM']}';
            """)

            spark.sql(sqlQuery=f"""
                UPDATE {deviations_table}
                SET table_name = '{table_name}'
                WHERE table_name = '{row['TABLE_NM']}';
            """)
        else:
            table_name = row['TABLE_NM']
            name_field = row['TABLE_REPORT_DT_NM']

        logging.info(f'Смотри сюда! - {table_name},{name_field}')
        next_check_old = dt.datetime.strptime(row['NEXT_CHECK_DT'], '%Y-%m-%d %H:%M:%S')
        update_table_old = dt.datetime.strptime(row['UPDATE_TABLE_DT'], '%Y-%m-%d %H:%M:%S')
        if len(row['update_table_now_dt']) > 10:
            update_table_now_dt = dt.datetime.strptime(row['update_table_now_dt'], '%Y-%m-%d %H:%M:%S')
            flag = 1
        else:
            update_table_now_dt = dt.datetime.strptime(row['update_table_now_dt'], '%Y-%m-%d')
            flag = 2

        while update_table_old <= update_table_now_dt:
            if flag == 1:
                update_table_old_str = dt.datetime.strftime(update_table_old, "%Y-%m-%d %H:%M:%S")
            else:
                update_table_old_str = dt.datetime.strftime(update_table_old,"%Y-%m-%d")

            update_data = spark.sql(f"""
                SELECT *
                FROM {table_name}
                WHERE {name_field} = '{update_table_old_str}'
                LIMIT 3;
            """).toPandas()

            logging.info(f'Длина таблицы  = {update_data.shape[0]}')
            if update_data.shape[0] > 0:
                metriks_func_one(next_check_old, table_name, name_field)

                # меняем и перезаписываем даты
                # меняется LAST_CHECK_DT, NEXT_CHECK_DT и UPDATE_TABLE_DT
                interval = row['INTERVAL_TERM']
                if interval == 1:
                    next_check_new = next_check_old + dt.timedelta(days=1)
                    update_table_new = update_table_old + dt.timedelta(days=1)
                if interval == 2:
                    next_check_new = next_check_old + dt.timedelta(days=7)
                    update_table_new = update_table_old + dt.timedelta(days=7)
                if interval == 3:
                    days_in_month = calendar.monthrange(next_check_old.year, next_check_old.month)[1]
                    next_check_new = next_check_old + dt.timedelta(days=days_in_month)
                    logging.info(next_check_new)
                    days_in_month_2 = calendar.monthrange(update_table_old.year, update_table_old.month)[1]
                    update_table_new = update_table_old + dt.timedelta(days=days_in_month_2)
                    logging.info(update_table_new)

                if row['flg'] == '1':
                    table_name = table_name[:-2]
                    logging.info(table_name)

                hook.run(sql=f"""UPDATE {setup_table}
                            SET LAST_CHECK_DT = '{next_check_old}', 
                            NEXT_CHECK_DT = '{next_check_new}', UPDATE_TABLE_DT = '{update_table_new}'
                            WHERE TABLE_NM = '{table_name}' and NEXT_CHECK_DT = '{next_check_old}'
                            and INTERVAL_TERM = {interval} and UPDATE_TABLE_DT = '{update_table_old}'
                        """)

                if row['flg'] == '1':
                    table_name = table_name + '_v'
                    logging.info(table_name)

                next_check_old = next_check_new
                update_table_old = update_table_new

            else:
                interval = row['INTERVAL_TERM']
                if interval == 1:
                    next_check_new = next_check_old + dt.timedelta(days=1)
                    update_table_new = update_table_old + dt.timedelta(days=1)
                if interval == 2:
                    next_check_new = next_check_old + dt.timedelta(days=7)
                    update_table_new = update_table_old + dt.timedelta(days=7)
                if interval == 3:
                    days_in_month = calendar.monthrange(next_check_old.year, next_check_old.month)[1]
                    next_check_new = next_check_old + dt.timedelta(days=days_in_month)
                    logging.info(next_check_new)
                    days_in_month_2 = calendar.monthrange(update_table_old.year, update_table_old.month)[1]
                    update_table_new = update_table_old + dt.timedelta(days=days_in_month_2)
                    logging.info(update_table_new)

                if row['flg'] == '1':
                    table_name = table_name[:-2]
                    logging.info(table_name)

                hook.run(sql=f"""UPDATE {setup_table}
                            SET  
                            NEXT_CHECK_DT = '{next_check_new}', UPDATE_TABLE_DT = '{update_table_new}'
                            WHERE TABLE_NM = '{table_name}' and NEXT_CHECK_DT = '{next_check_old}'
                            and INTERVAL_TERM = {interval} and UPDATE_TABLE_DT = '{update_table_old}'
                        """)

                if row['flg'] == '1':
                    table_name = table_name + '_v'
                    logging.info(table_name)

                next_check_old = next_check_new
                update_table_old = update_table_new

        logging.info(F'то, что будет удаляться - {table_name}')
        if row['flg'] == '1':
            conn = impala_conn()
            cursor = conn.cursor()
            cursor.execute(f"drop table {table_name}")
            conn.close()
            hook = sql_conn.get_hook()
            hook.run(sql=f"""UPDATE {table_for_recording}
                    SET table_name = '{row['TABLE_NM']}'
                    where table_name = '{table_name}'
                """)
            hook.run(sql=f"""UPDATE {deviations_table}
                    SET table_name = '{row['TABLE_NM']}'
                    where table_name = '{table_name}'
                """)

    count = table_temp_2.shape[0]
    return count



def metriks_func_one(current_date, table_name, date_field):
    pd.set_option('use_inf_as_na', True)
    # По сколько записей за раз запрос будет извлекать из таблицы (500 или менее)
    ROWS_AMOUNT_IN_ITERATION = 50
    # Минимальный процент по умолчанию, при котором выводится флаг
    proc = 10
    # Получение статистических метрик по одному срезу

    # Получение статистических метрик за прошлый месяц

    # Получение вручную заведённых пороговых значений по полям
    def setup_metrics(full_table_name):
        df_setup = spark.sql(f"""
            SELECT
                column_name as column_name,
                rows_amount_cnt as ROWS_AMOUNT_IN_ITERATION_limit,
                amount_completed_cnt as amount_completed_limit,
                nulls_amount_cnt as nulls_amount_limit,
                distinct_amount_cnt as distinct_amount_limit,
                minimum_value_info as minimum_value_limit,
                maximum_value_info as maximum_value_limit,
                median_value_info as median_value_limit,
                average_value_info as average_value_limit,
                sum_attributes_nval as sum_attributes_limit
            FROM {deviations_table}
            WHERE 1=1
                table_name = '{full_table_name}'
                and period_to_dt = '2100-01-01';
        """).toPandas()
        logging.info(f"3. Выполнено извлечение порогов из настроечной таблицы: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        return df_setup

    # Расчёт отклонений
    def get_deviation(current_date, full_table_name, date_field):
        # Преобразование даты под формат даты среза
        # report_dttm = transformation(current_date, date_field, full_table_name)
        # Получение статистических метрик по одному срезу
        df_curr_metrics = curr_metrics(current_date, full_table_name, date_field)
        # Получение статистических метрик за прошлый месяц
        # current_date,
        df_prev_metrics = prev_metrics(current_date, full_table_name, df_curr_metrics)

        # Объединение срезов по 'table_name', 'column_name', 'index', 'data_type', 'comment_info'
        df_merged = pd.merge(df_curr_metrics,
                             df_prev_metrics,
                             how='left',
                             on=['table_name', 'column_name', 'column_id', 'data_type', 'comment_info'],
                             suffixes=('_curr', '_prev'))

        old_date = date.today().replace(year=2000, month=1, day=1)
        df_merged.report_date_prev.fillna(old_date, inplace=True)


        # Расчёт отклонений по поля
        ROWS_AMOUNT_IN_ITERATION_list, amount_completed_list, nulls_amount_list, nulls_amount_prc_list, distinct_amount_list, minimum_value_list, maximum_value_list, median_value_list, average_value_list, sum_attributes_list = [], [], [], [], [], [], [], [], [], []
        for i in range(df_merged.shape[0]):
            # Расчет дельты общего количества строк
            if min(df_merged.at[i, 'rows_amount_cnt_curr'], df_merged.at[i, 'rows_amount_cnt_prev']) != 0:
                q = round((abs((df_merged.at[i, 'rows_amount_cnt_curr'] - df_merged.at[i, 'rows_amount_cnt_prev']) /
                               min(df_merged.at[i, 'rows_amount_cnt_curr'],
                                   df_merged.at[i, 'rows_amount_cnt_prev']) * 100)), 2)
                ROWS_AMOUNT_IN_ITERATION_list.append(q)
            elif df_merged.at[i, 'rows_amount_cnt_curr'] == 0 and df_merged.at[i, 'rows_amount_cnt_prev'] == 0:
                ROWS_AMOUNT_IN_ITERATION_list.append(0)
            else:
                ROWS_AMOUNT_IN_ITERATION_list.append(100)
            # Расчет дельты количества заполненных строк
            if min(df_merged.at[i, 'amount_completed_cnt_curr'],
                   df_merged.at[i, 'amount_completed_cnt_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'amount_completed_cnt_curr'] - df_merged.at[
                        i, 'amount_completed_cnt_prev']) /
                         min(df_merged.at[i, 'amount_completed_cnt_curr'],
                             df_merged.at[i, 'amount_completed_cnt_prev']) * 100)), 2)
                amount_completed_list.append(q)
            elif df_merged.at[i, 'amount_completed_cnt_curr'] == 0 and df_merged.at[
                i, 'amount_completed_cnt_prev'] == 0:
                amount_completed_list.append(0)
            else:
                amount_completed_list.append(100)
                # Расчет дельты количества пустых значений
            if min(df_merged.at[i, 'nulls_amount_cnt_curr'], df_merged.at[i, 'nulls_amount_cnt_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'nulls_amount_cnt_curr'] - df_merged.at[i, 'nulls_amount_cnt_prev']) /
                         min(df_merged.at[i, 'nulls_amount_cnt_curr'],
                             df_merged.at[i, 'nulls_amount_cnt_prev']) * 100)), 2)
                nulls_amount_list.append(q)
            elif df_merged.at[i, 'nulls_amount_cnt_curr'] == 0 and df_merged.at[i, 'nulls_amount_cnt_prev'] == 0:
                nulls_amount_list.append(0)
            else:
                nulls_amount_list.append(100)
            # Расчет дельты уникальных значений
            if min(df_merged.at[i, 'distinct_amount_cnt_curr'],
                   df_merged.at[i, 'distinct_amount_cnt_prev']) != 0:
                q = round((abs((df_merged.at[i, 'distinct_amount_cnt_curr'] - df_merged.at[
                    i, 'distinct_amount_cnt_prev']) /
                               min(df_merged.at[i, 'distinct_amount_cnt_curr'],
                                   df_merged.at[i, 'distinct_amount_cnt_prev']) * 100)), 2)
                distinct_amount_list.append(q)
            elif df_merged.at[i, 'distinct_amount_cnt_curr'] == 0 and \
                    df_merged.at[i, 'distinct_amount_cnt_prev'] == 0:
                distinct_amount_list.append(0)
            else:
                distinct_amount_list.append(100)
            # Расчет дельты минимальных значений
            if min(df_merged.at[i, 'minimum_value_info_curr'], df_merged.at[i, 'minimum_value_info_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'minimum_value_info_curr'] - df_merged.at[i, 'minimum_value_info_prev']) /
                         min(df_merged.at[i, 'minimum_value_info_curr'],
                             df_merged.at[i, 'minimum_value_info_prev']) * 100)), 2)
                minimum_value_list.append(q)
            elif df_merged.at[i, 'minimum_value_info_curr'] == 0 and df_merged.at[
                 i, 'minimum_value_info_prev'] == 0:
                minimum_value_list.append(0)
            else:
                minimum_value_list.append(100)
            # Расчет дельты максимальных значений
            if min(df_merged.at[i, 'maximum_value_info_curr'], df_merged.at[i, 'maximum_value_info_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'maximum_value_info_curr'] - df_merged.at[i, 'maximum_value_info_prev']) /
                         min(df_merged.at[i, 'maximum_value_info_curr'],
                             df_merged.at[i, 'maximum_value_info_prev']) * 100)), 2)
                maximum_value_list.append(q)
            elif df_merged.at[i, 'maximum_value_info_curr'] == 0 and df_merged.at[
                i, 'maximum_value_info_prev'] == 0:
                maximum_value_list.append(0)
            else:
                maximum_value_list.append(100)
            # Расчет дельты медианы
            if min(df_merged.at[i, 'median_value_info_curr'], df_merged.at[i, 'median_value_info_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'median_value_info_curr'] - df_merged.at[i, 'median_value_info_prev']) /
                         min(df_merged.at[i, 'median_value_info_curr'],
                             df_merged.at[i, 'median_value_info_prev']) * 100)), 2)
                median_value_list.append(q)
            elif df_merged.at[i, 'median_value_info_curr'] == 0 and df_merged.at[
                i, 'median_value_info_prev'] == 0:
                median_value_list.append(0)
            else:
                median_value_list.append(100)
            # Расчет дельты средних значений
            if min(df_merged.at[i, 'average_value_info_curr'], df_merged.at[i, 'average_value_info_prev']) != 0:
                q = round(
                    (abs((df_merged.at[i, 'average_value_info_curr'] - df_merged.at[i, 'average_value_info_prev']) /
                         min(df_merged.at[i, 'average_value_info_curr'],
                             df_merged.at[i, 'average_value_info_prev']) * 100)), 2)
                average_value_list.append(q)
            elif df_merged.at[i, 'average_value_info_curr'] == 0 and df_merged.at[
                i, 'average_value_info_prev'] == 0:
                average_value_list.append(0)
            else:
                average_value_list.append(100)
            # Расчет суммы атрибутов поля
            if min(df_merged.at[i, 'sum_attributes_nval_curr'],
                   df_merged.at[i, 'sum_attributes_nval_prev']) != 0:
                q = round((abs((df_merged.at[i, 'sum_attributes_nval_curr'] - df_merged.at[
                    i, 'sum_attributes_nval_prev']) /
                               min(df_merged.at[i, 'sum_attributes_nval_curr'],
                                   df_merged.at[i, 'sum_attributes_nval_prev']) * 100)), 2)
                sum_attributes_list.append(q)
            elif df_merged.at[i, 'sum_attributes_nval_curr'] == 0 and df_merged.at[
                 i, 'sum_attributes_nval_prev'] == 0:
                sum_attributes_list.append(0)
            else:
                sum_attributes_list.append(100)

        df_merged['ROWS_AMOUNT_IN_ITERATION_delta'] = ROWS_AMOUNT_IN_ITERATION_list
        df_merged['amount_completed_delta'] = amount_completed_list
        df_merged['nulls_amount_delta'] = nulls_amount_list
        df_merged['distinct_amount_delta'] = distinct_amount_list
        df_merged['minimum_value_delta'] = minimum_value_list
        df_merged['maximum_value_delta'] = maximum_value_list
        df_merged['median_value_delta'] = median_value_list
        df_merged['average_value_delta'] = average_value_list
        df_merged['sum_attributes_delta'] = sum_attributes_list

        # Получение вручную заведённых пороговых значений по полям
        df_setup = setup_metrics(full_table_name)
        # Объединение со значениями порогов по 'column_name'
        df_limit = pd.merge(df_merged['column_name'], df_setup, how='left', on='column_name').fillna(proc)
        df_merged = pd.merge(df_merged, df_limit, on='column_name')

        # Расчёт флагов отклонений по каждому из полей
        df_merged['ROWS_AMOUNT_IN_ITERATION_flg'] = np.select([df_merged['ROWS_AMOUNT_IN_ITERATION_delta'] >
                                                  df_merged['ROWS_AMOUNT_IN_ITERATION_limit']], [1], 0)
        df_merged['amount_completed_flg'] = np.select([df_merged['amount_completed_delta'] >
                                                       df_merged['amount_completed_limit']], [1], 0)
        df_merged['nulls_amount_flg'] = np.select([df_merged['nulls_amount_delta'] >
                                                   df_merged['nulls_amount_limit']], [1], 0)
        df_merged['distinct_amount_flg'] = np.select([df_merged['distinct_amount_delta'] >
                                                      df_merged['distinct_amount_limit']], [1], 0)
        df_merged['minimum_value_flg'] = np.select([df_merged['minimum_value_delta'] >
                                                    df_merged['minimum_value_limit']], [1], 0)
        df_merged['maximum_value_flg'] = np.select([df_merged['maximum_value_delta'] >
                                                    df_merged['maximum_value_limit']], [1], 0)
        df_merged['median_value_flg'] = np.select([df_merged['median_value_delta'] >
                                                   df_merged['median_value_limit']], [1], 0)
        df_merged['average_value_flg'] = np.select([df_merged['average_value_delta'] >
                                                    df_merged['average_value_limit']], [1], 0)
        df_merged['sum_attributes_flg'] = np.select([df_merged['sum_attributes_delta'] >
                                                     df_merged['sum_attributes_limit']], [1], 0)
        df_merged['dublicate_cnt_flg'] = np.select([df_merged['dublicate_cnt_curr'] > 0], [1], 0)
        # df_merged.replace(np.inf, None, inplace = True)
        df_merged['deviation_flg'] = 0
        df_merged['deviation_desc'] = ''

        # Расчёт общего флага и описания полученных отклонений
        for index, row in df_merged.iterrows():
            if row['ROWS_AMOUNT_IN_ITERATION_flg'] == 1 or row['amount_completed_flg'] == 1 or row['nulls_amount_flg'] == 1 \
            or row['dublicate_cnt_flg'] == 1 or row['distinct_amount_flg'] or row['minimum_value_flg'] == 1 \
            or row['maximum_value_flg'] == 1 or row['median_value_flg'] == 1 or row['average_value_flg'] \
            or row['sum_attributes_flg'] == 1:
                df_merged.at[index, 'deviation_flg'] = 1

            deviation_desc = ""
            if row['ROWS_AMOUNT_IN_ITERATION_flg'] == 1:
                if (row['rows_amount_cnt_curr'] == 0 or row['rows_amount_cnt_prev'] == 0) and row[
                    'rows_amount_cnt_curr'] != \
                        row['rows_amount_cnt_prev']:
                    deviation_desc += "Дельта по показателю ROWS_AMOUNT_IN_ITERATION не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу rows_amount_cnt " + str(
                        round(row['ROWS_AMOUNT_IN_ITERATION_delta'], 2)) + "%, "
            if row['amount_completed_flg'] == 1:
                if (row['amount_completed_cnt_curr'] == 0 or ['amount_completed_cnt_prev'] == 0) and \
                row['amount_completed_cnt_curr'] != row['amount_completed_cnt_prev']:
                    deviation_desc += "Дельта по показателю amount_completed не считаема, " \
                                      "так как значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу amount_completed_cnt " + str(
                        round(row['amount_completed_delta'], 2)) + "%, "
            if row['nulls_amount_flg'] == 1:
                if (row['nulls_amount_cnt_curr'] == 0 or row['nulls_amount_cnt_prev'] == 0) and \
                row['nulls_amount_cnt_curr'] != row['nulls_amount_cnt_curr']:
                    deviation_desc += "Дельта по показателю nulls_amount не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу nulls_amount_cnt " + str(
                        round(row['nulls_amount_delta'], 2)) + "%, "
            if row['distinct_amount_flg'] == 1:
                if (row['distinct_amount_cnt_curr'] == 0 or row['distinct_amount_cnt_prev'] == 0) and \
                row['distinct_amount_cnt_curr'] != row['distinct_amount_cnt_prev']:
                    deviation_desc += "Дельта по показателю distinct_amount не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу distinct_amount_cnt " + str(
                        round(row['distinct_amount_delta'], 2)) + "%, "
            if row['minimum_value_flg'] == 1:
                if (row['minimum_value_info_curr'] == 0 or row['minimum_value_info_prev'] == 0) and \
                row['minimum_value_info_curr'] != row['minimum_value_info_prev']:
                    deviation_desc += "Дельта по показателю minimum_value не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу minimum_value_info " + str(
                        round(row['minimum_value_delta'], 2)) + "%, "
            if row['maximum_value_flg'] == 1:
                if (row['maximum_value_info_curr'] == 0 or row['maximum_value_info_prev'] == 0) and \
                row['maximum_value_info_curr'] != row['maximum_value_info_prev']:
                    deviation_desc += "Дельта по показателю maximum_value не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу maximum_value_info " + str(
                        round(row['maximum_value_delta'], 2)) + "%, "
            if row['median_value_flg'] == 1:
                if (row['median_value_info_curr'] == 0 or row['median_value_info_prev'] == 0) and \
                row['median_value_info_curr'] != row['median_value_info_prev']:
                    deviation_desc += "Дельта по показателю median_value не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу median_value_info " + str(
                        round(row['median_value_delta'], 2)) + "%, "
            if row['average_value_flg'] == 1:
                if (row['average_value_info_curr'] == 0 or row['average_value_info_prev'] == 0) and \
                row['average_value_info_curr'] != row['average_value_info_prev']:
                    deviation_desc += "Дельта по показателю average_value не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу average_value_info " + str(
                        round(row['average_value_delta'], 2)) + "%, "
            if row['sum_attributes_flg'] == 1:
                if (row['sum_attributes_nval_curr'] == 0 or row['sum_attributes_nval_prev'] == 0) and \
                row['sum_attributes_nval_curr'] != row['sum_attributes_nval_prev']:
                    deviation_desc += "Дельта по показателю sum_attributes не считаема, так как " \
                                      "значение в одном из срезов = 0, "
                else:
                    deviation_desc += "Отклонение по столбцу sum_attributes_nval " + str(
                        round(row['sum_attributes_delta'], 2)) + "%, "
            if row['dublicate_cnt_flg'] == 1:
                deviation_desc += "Имеются полные дубликаты строк, "

            df_merged.at[index, 'deviation_desc'] = deviation_desc[:-2]

        # Требуемые поля для таблицы
        columns_merged = [
            'table_name', 'column_name', 'comment_info',
            'data_type', 'column_id', 'report_date_prev', 'report_date_curr',
            'rows_amount_cnt_curr', 'ROWS_AMOUNT_IN_ITERATION_delta', 'amount_completed_cnt_curr',
            'amount_completed_delta', 'nulls_amount_cnt_curr', 'nulls_amount_delta',
            'distinct_amount_cnt_curr', 'distinct_amount_delta',
            'minimum_value_info_curr', 'minimum_value_delta',
            'maximum_value_info_curr', 'maximum_value_delta',
            'median_value_info_curr', 'median_value_delta',
            'average_value_info_curr', 'average_value_delta',
            'sum_attributes_nval_curr', 'sum_attributes_delta',
            'dublicate_cnt_curr', 'deviation_flg', 'deviation_desc',
            'period_from_dt', 'period_to_dt'
        ]
        logging.info("досчиталось до сюда")
        # Заменяем дату period_to_dt в старых срезах
        open_date = dt.datetime.strptime('2100-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
        now_date = dt.datetime.now()

        spark.sql(f"""
            UPDATE {table_for_recording}
            SET period_to_dt = '{now_date.strftime("%Y-%m-%d %H:%M:%S")}'
            where 1=1
                AND report_date = '{current_date}'
                AND table_name = '{full_table_name}'
                AND period_to_dt = '{open_date}';
        """)

        # Открытие новых строк
        df_merged['period_from_dt'] = (now_date + dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
        df_merged['period_to_dt'] = dt.datetime.strptime('2100-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
        df_merged = df_merged[columns_merged]

        logging.info(f"4. Выполнен расчёт отклонений за {df_merged['report_date_curr'].unique()[0]}: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        df_merged = df_merged[['table_name', 'column_name', 'comment_info',
                               'data_type', 'column_id', 'report_date_prev', 'report_date_curr',
                               'rows_amount_cnt_curr', 'ROWS_AMOUNT_IN_ITERATION_delta', 'amount_completed_cnt_curr',
                               'amount_completed_delta', 'nulls_amount_cnt_curr', 'nulls_amount_delta',
                               'distinct_amount_cnt_curr', 'distinct_amount_delta',
                               'minimum_value_info_curr', 'minimum_value_delta',
                               'maximum_value_info_curr', 'maximum_value_delta',
                               'median_value_info_curr', 'median_value_delta',
                               'average_value_info_curr', 'average_value_delta',
                               'sum_attributes_nval_curr', 'sum_attributes_delta',
                               'dublicate_cnt_curr', 'deviation_flg', 'deviation_desc',
                               'period_from_dt', 'period_to_dt']]
        logging.info("досчиталось до сюда нет")
        return df_merged

    # Запись данных в итоговую таблицу
    def write_to_sql(current_date, full_table_name, date_field):
        df_final = get_deviation(current_date, full_table_name, date_field)

        for index, row in df_final.iterrows():
            hook.run("""insert into upd.airflow.metrics (TABLE_NAME, column_name, comment_info,
                        data_type, column_id, comparison_dt, report_date,
                        rows_amount_cnt, delta_rows_amount_cnt, amount_completed_cnt,
                        delta_amount_completed_cnt, nulls_amount_cnt, delta_nulls_amount_cnt,
                        distinct_amount_cnt, delta_distinct_amount_cnt,
                        minimum_value_info, delta_minimum_value_info,
                        maximum_value_info, delta_maximum_value_info,
                        median_value_info, delta_median_value_info,
                        average_value_info, delta_average_value_info,
                        sum_attributes_nval, delta_sum_attributes_nval,
                        dublicate_cnt, deviation_flg, deviation_desc, period_from_dt,
                        period_to_dt) VALUES ('{0}','{1}','{2}','{3}',
                        CAST(case when '{4}' = 'nan' then null else '{4}' end as float),
                        CAST(case when '{5}' = 'nan' then null else '{5}' end as datetime), 
                        CAST(case when '{6}' = 'nan' then null else '{6}' end as datetime),
                        CAST(case when '{7}' = 'nan' then null else '{7}' end as float),
                        CAST(case when '{8}' = 'nan' then null else '{8}' end as float),
                        CAST(case when '{9}' = 'nan' then null else '{9}' end as float),
                        CAST(case when '{10}' = 'nan' then null else '{10}' end as float),
                        CAST(case when '{11}' = 'nan' then null else '{11}' end as float),
                        CAST(case when '{12}' = 'nan' then null else '{12}' end as float),
                        CAST(case when '{13}' = 'nan' then null else '{13}' end as float),
                        CAST(case when '{14}' = 'nan' then null else '{14}' end as float),
                        CAST(case when '{15}' = 'nan' then null else '{15}' end as float),
                        CAST(case when '{16}' = 'nan' then null else '{16}' end as float),
                        CAST(case when '{17}' = 'nan' then null else '{17}' end as float),
                        CAST(case when '{18}' = 'nan' then null else '{18}' end as float),
                        CAST(case when '{19}' = 'nan' then null else '{19}' end as float),
                        CAST(case when '{20}' = 'nan' then null else '{20}' end as float),
                        CAST(case when '{21}' = 'nan' then null else '{21}' end as float),
                        CAST(case when '{22}' = 'nan' then null else '{22}' end as float),
                        CAST(case when '{23}' = 'nan' then null else '{23}' end as float),
                        CAST(case when '{24}' = 'nan' then null else '{24}' end as float),
                        CAST(case when '{25}' = 'nan' then null else '{25}' end as float),
                        CAST(case when '{26}' = 'nan' then null else '{26}' end as float),
                        '{27}','{28}','{29}')
                        """.format(row['table_name'], row['column_name'], row['comment_info'], row['data_type'],
                                   row['column_id'], row['report_date_prev'], row['report_date_curr'],
                                   row['rows_amount_cnt_curr'],
                                   row['ROWS_AMOUNT_IN_ITERATION_delta'], row['amount_completed_cnt_curr'],
                                   row['amount_completed_delta'], row['nulls_amount_cnt_curr'],
                                   row['nulls_amount_delta'], row['distinct_amount_cnt_curr'],
                                   row['distinct_amount_delta'], row['minimum_value_info_curr'],
                                   row['minimum_value_delta'], row['maximum_value_info_curr'],
                                   row['maximum_value_delta'], row['median_value_info_curr'],
                                   row['median_value_delta'], row['average_value_info_curr'],
                                   row['average_value_delta'], row['sum_attributes_nval_curr'],
                                   row['sum_attributes_delta'], row['dublicate_cnt_curr'], row['deviation_flg'],
                                   row['deviation_desc'], row['period_from_dt'], row['period_to_dt']))


        # hook.run(f"truncate table {table_for_recording}")
        # hook.run(f"insert into {table_for_recording} select * from {table_for_recording}_copy")
        # hook.run(f"drop table {table_for_recording}_copy")

        finish = dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logging.info(
            "5. Данные итоговой таблицы обновлены: {0}".format(dt.datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
        logging.info('Расчет завершен:' + finish + '\n')

    write_to_sql(current_date, table_name, date_field)