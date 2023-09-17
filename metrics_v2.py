import datetime as dt
import numpy as np
import pandas as pd
import re
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType


ROWS_AMOUNT_IN_ITERATION = 50

SETUP_TABLE_NAME = 'test.dq_setup_table'

STATISTICS_TABLE_NAME = 'test.dq_table_statistics'
FLOAT_FIELDS_IN_STATISTICS_TABLE = [
    'minimum_value_info',
    'maximum_value_info',
    'average_value_info',
    'median_value_info',
    'sum_attributes_nval',
]


def get_inferred_data_type(column_data_type: str):
    if column_data_type == 'string':
        return StringType()
    elif 'int' in column_data_type:
        return IntegerType()
    elif 'decimal' in column_data_type:
        precision, scale = re.search(r'decimal\((\d+),(\d+)\)', column_data_type).groups()
        return DecimalType(int(precision), int(scale))
    elif column_data_type == 'timestamp':
        return TimestampType()
    elif column_data_type == 'date':
        return DateType()
    else:
        return StringType()


def get_schema_for_hive_table(spark, table_name: str):
    df_table_struct = spark.sql(f'DESCRIBE {table_name}').toPandas()
    schema = StructType([StructField(row_data['col_name'], get_inferred_data_type(row_data['data_type']))
                         for _, row_data in df_table_struct.iterrows()])
    return schema


def float_to_decimal(precision: int):
    if precision != 0:
        return lambda x: Decimal(str(round(x, precision)))
    else:
        return lambda x: Decimal(int(x))


def get_table_statistics(spark,
                         table_name: str,
                         date_field: str,
                         interval: str,
                         end_of_calculation_period: dt.date):
    start_of_calculation_period = None
    if interval == 'day':
        start_of_calculation_period = end_of_calculation_period
        logger.info(f"Расчёт статистики для таблицы {table_name} за {end_of_calculation_period}")
    elif interval == 'week':
        start_of_calculation_period = end_of_calculation_period - dt.timedelta(days=6)
        logger.info(f"Расчёт статистики для таблицы {table_name} за неделю {start_of_calculation_period} - {end_of_calculation_period}")
    elif interval == 'month':
        start_of_calculation_period = end_of_calculation_period.replace(day=1)
        end_of_calculation_period = (end_of_calculation_period + relativedelta(months=1)).replace(day=1) - dt.timedelta(days=1)
        logger.info(f"Расчёт статистики для таблицы {table_name} за неделю {start_of_calculation_period} - {end_of_calculation_period}")

    # Извлечение структуры из таблицы
    df_table_struct = spark.sql(f"DESCRIBE {table_name};").toPandas()

    df_table_struct = df_table_struct[~df_table_struct['col_name'].isin([date_field])]

    # Отделение числовых полей
    df_table_struct_num = df_table_struct[~df_table_struct['data_type'].isin(['string', 'timestamp', 'varchar'])] \
        .reset_index(drop=True)

    # Отделение строковых полей и дат
    df_table_struct_str = df_table_struct[df_table_struct['data_type'].isin(['string', 'timestamp', 'varchar'])] \
        .reset_index(drop=True)

    res_columns = [
        'table_name', 'column_name',
        'column_id', 'data_type', 'comment',
        'start_of_calculation_period', 'end_of_calculation_period', 'interval',
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
                        to_date('{start_of_calculation_period}') start_of_calculation_period,
                        to_date('{end_of_calculation_period}') end_of_calculation_period,
                        '{interval}' interval,
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
                    WHERE to_date({date_field}) BETWEEN  '{start_of_calculation_period}' AND '{end_of_calculation_period}'
                    GROUP BY column_name UNION ALL"""
            query = f"{query[:-9]} ORDER BY column_name, end_of_calculation_period;"
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
                        to_date('{start_of_calculation_period}') start_of_calculation_period,
                        to_date('{end_of_calculation_period}') end_of_calculation_period,
                        '{interval}' interval,
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
                    WHERE to_date({date_field}) BETWEEN '{start_of_calculation_period}' AND '{end_of_calculation_period}'
                    GROUP BY column_name UNION ALL"""
            query = f"{query[:-9]} ORDER BY column_name, end_of_calculation_period"
            df_result_str = pd.concat([df_result_str, spark.sql(query).toPandas()])

    # Объединение результатов двух расчётов
    df_result = pd.concat([df_result_num, df_result_str])

    # Для float указывается тип object, исправим
    for float_field in FLOAT_FIELDS_IN_STATISTICS_TABLE:
        df_result[float_field] = df_result[float_field].astype('float64')

    df_result = pd.merge(df_result, df_table_struct, left_on='column_name', right_on='col_name')
    df_result.reset_index(inplace=True)
    df_result['column_id'] = df_result.index
    df_result = df_result[res_columns].sort_values(['column_id', 'end_of_calculation_period', 'interval'])
    df_result['dublicate_cnt'] = df_result.duplicated().sum()

    if interval == 'day':
        logger.info(f'Выполнен расчёт статистики за {end_of_calculation_period}')
    else:
        logger.info(f'Выполнен расчёт статистики за {start_of_calculation_period} - {end_of_calculation_period}')

    df_result['report_dttm'] = pd.Timestamp('now')
    return df_result


def insert_statistics(spark, df_statistics):
    table_schema = get_schema_for_hive_table(spark, STATISTICS_TABLE_NAME)
    df_statistics_copy = df_statistics.copy()
    for float_field in FLOAT_FIELDS_IN_STATISTICS_TABLE:
        df_statistics_copy[float_field] = df_statistics_copy[float_field].apply(float_to_decimal(2))
    spark_df = spark.createDataFrame(df_statistics_copy, schema=table_schema)
    spark_df.write.format('hive').mode('append').saveAsTable(STATISTICS_TABLE_NAME)
    logger.info(f'Выполнена вставка в таблицу вычисленной статистики')


def get_previous_existing_metircs(spark,
                                  end_of_calculation_period: dt.date,
                                  interval: str,
                                  table_name: str):
    previous_metrics = spark.sql(f'''
        WITH v AS (
            SELECT
                *,
                rank() OVER (ORDER BY end_of_calculation_period DESC, report_dttm DESC) AS rnk
            FROM {STATISTICS_TABLE_NAME}
            WHERE 1=1
                AND table_name = '{table_name}'
                AND end_of_calculation_period < '{end_of_calculation_period}'
                AND interval = '{interval}'
        )
        SELECT * FROM v WHERE rnk = 1;
    ''').toPandas()
    return previous_metrics


DELTA = {
    'day': dt.timedelta(days=1),
    'week': dt.timedelta(days=7),
    'month': relativedelta(months=1),
}


def compute_statistics(spark, current_date: dt.date):
    setup_table_df = spark.sql(f'''
        WITH v AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY TABLE_NAME, INTERVAL_TYPE ORDER BY NEXT_CHECK_DT DESC) rn
            FROM {SETUP_TABLE_NAME}
            WHERE NEXT_CHECK_DT <= '{current_date}'
        )
        SELECT *
        FROM v
        WHERE rn = 1;
    ''').toPandas()
    for index, row in setup_table_df.iterrows():
        table_name = row['TABLE_NAME']
        date_field = row['TABLE_DATE_FIELD']
        next_check_dt = row['NEXT_CHECK_DT']
        interval = row['INTERVAL_TYPE']
        delta = DELTA[interval]
        while next_check_dt <= current_date:
            end_of_calculation_period = next_check_dt - dt.timedelta(days=1)
            table_statistics_df = get_table_statistics(spark, table_name, date_field, interval, end_of_calculation_period)
            insert_statistics(spark, table_statistics_df)
            next_check_dt = next_check_dt + delta
        spark.sql(f'''
            INSERT INTO {SETUP_TABLE_NAME} VALUES 
            ('{table_name}', '{date_field}', cast('{next_check_dt}' as date), '{interval}');
            ''')
