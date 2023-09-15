import datetime as dt
import numpy as np
import pandas as pd
import re
from decimal import Decimal
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType


ROWS_AMOUNT_IN_ITERATION = 50

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
    for float_field in FLOAT_FIELDS_IN_STATISTICS_TABLE:
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


def insert_statistics(spark, df_statistics):
    table_schema = get_schema_for_hive_table(spark, STATISTICS_TABLE_NAME)
    df_statistics_copy = df_statistics.copy()
    for float_field in FLOAT_FIELDS_IN_STATISTICS_TABLE:
        df_statistics_copy[float_field] = df_statistics_copy[float_field].apply(float_to_decimal(2))
    spark_df = spark.createDataFrame(df_statistics_copy, schema=table_schema)
    spark_df.write.format('hive').mode('append').saveAsTable(STATISTICS_TABLE_NAME)
    logger.info(f'Выполнена вставка в таблицу вычисленной статистики')