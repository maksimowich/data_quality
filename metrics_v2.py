import datetime as dt
import numpy as np
import pandas as pd
import re
from dateutil.relativedelta import relativedelta
from decimal import Decimal
from loguru import logger
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, TimestampType, DecimalType

ROWS_AMOUNT_IN_ITERATION = 50
PROC = 10

STATISTICS_TABLE_NAME = 'test.dq_table_statistics'
FLOAT_FIELDS_IN_STATISTICS_TABLE = [
    'minimum_value',
    'maximum_value',
    'average_value',
    'median_value',
    'sum_of_values',
]

SETUP_TABLE_NAME = 'test.dq_setup_table'

DEVIATIONS_SETUP_TABLE_NAME = 'test.dq_deviations_setup'

METRICS_TABLE_NAME = 'test.dq_metrics'
FLOAT_FIELDS_IN_METRICS_TABLE = [
    'delta_rows_cnt',
    'delta_completed_rows_cnt',
    'delta_nulls_cnt',
    'delta_distinct_values_cnt',
    'minimum_value',
    'delta_minimum_value',
    'maximum_value',
    'delta_maximum_value',
    'median_value',
    'delta_median_value',
    'average_value',
    'delta_average_value',
    'sum_of_values',
    'delta_sum_of_values',
]


def get_spark_data_type(column_data_type: str):
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
    schema = StructType([StructField(row_data['col_name'], get_spark_data_type(row_data['data_type']))
                         for _, row_data in df_table_struct.iterrows()])
    return schema


def float_to_decimal(precision: int):
    if precision != 0:
        return lambda x: Decimal(str(round(x, precision)))
    else:
        return lambda x: Decimal(int(x))


def insert_pandas_df_into_hive(spark, df: pd.DataFrame, table_name: str, float_fields=None):
    if float_fields is None:
        float_fields = []
    table_schema = get_schema_for_hive_table(spark, table_name)
    df_copy = df.copy()
    for float_field in float_fields:
        df_copy[float_field] = df_copy[float_field].apply(float_to_decimal(2))
    spark_df = spark.createDataFrame(df_copy, schema=table_schema)
    spark_df.write.format('hive').mode('append').saveAsTable(table_name)
    logger.info(f'Выполнена вставка в таблицу {table_name}')


def get_table_statistics(spark,
                         table_name: str,
                         date_field: str,
                         interval_type: str,
                         end_of_calculation_period: dt.date):
    start_of_calculation_period = None
    if interval_type == 'day':
        start_of_calculation_period = end_of_calculation_period
        logger.info(f"Расчёт статистики для таблицы {table_name} за {end_of_calculation_period}")
    elif interval_type == 'week':
        start_of_calculation_period = end_of_calculation_period - dt.timedelta(days=6)
        logger.info(f"Расчёт статистики для таблицы {table_name} за неделю {start_of_calculation_period} - {end_of_calculation_period}")
    elif interval_type == 'month':
        start_of_calculation_period = end_of_calculation_period.replace(day=1)
        end_of_calculation_period = (end_of_calculation_period + relativedelta(months=1)).replace(day=1) - dt.timedelta(days=1)
        logger.info(f"Расчёт статистики для таблицы {table_name} за месяц {start_of_calculation_period} - {end_of_calculation_period}")

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
        'table_name', 'column_name', 'column_id', 'data_type', 'comment',
        'start_of_calculation_period', 'end_of_calculation_period', 'interval_type',
        'rows_cnt', 'completed_rows_cnt', 'nulls_cnt', 'distinct_values_cnt',
        'minimum_value', 'maximum_value', 'median_value', 'average_value', 'sum_of_values',
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
                        '{interval_type}' interval_type,
                        COUNT(*) rows_cnt,
                        COUNT({row['col_name']}) completed_rows_cnt,
                        (COUNT(*) - COUNT({row['col_name']})) nulls_cnt,
                        COUNT(DISTINCT CAST({row['col_name']} as Decimal(38,6))) distinct_values_cnt,
                        MIN(CAST({row['col_name']} as Decimal(38,6))) minimum_value,
                        MAX(CAST({row['col_name']} as Decimal(38,6))) maximum_value,
                        PERCENTILE(CAST({row['col_name']} as BIGINT), 0.5) median_value,
                        AVG(CAST({row['col_name']} as Decimal(38,6))) average_value,
                        SUM(CAST({row['col_name']} as Decimal(38,6))) sum_of_values
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
                        '{interval_type}' interval_type,
                        COUNT(*) rows_cnt,
                        COUNT({row['col_name']}) completed_rows_cnt,
                        (COUNT(*) - COUNT({row['col_name']})) nulls_cnt,
                        COUNT(DISTINCT {row['col_name']}) distinct_values_cnt,
                        NULL minimum_value,
                        NULL maximum_value,
                        NULL median_value,
                        NULL average_value,
                        NULL sum_of_values
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
    df_result = df_result[res_columns].sort_values(['column_id', 'end_of_calculation_period', 'interval_type'])
    df_result['dublicates_cnt'] = df_result.duplicated().sum()

    if interval_type == 'day':
        logger.info(f'Выполнен расчёт статистики за {end_of_calculation_period}')
    else:
        logger.info(f'Выполнен расчёт статистики за {start_of_calculation_period} - {end_of_calculation_period}')

    df_result['report_dttm'] = pd.Timestamp('now')
    return df_result


def get_previous_existing_metrics(spark,
                                  end_of_calculation_period: dt.date,
                                  interval_type: str,
                                  table_name: str):
    previous_metrics_df = spark.sql(f'''
        WITH v AS (
            SELECT
                *,
                rank() OVER (ORDER BY end_of_calculation_period DESC, report_dttm DESC) AS rnk
            FROM {STATISTICS_TABLE_NAME}
            WHERE 1=1
                AND table_name = '{table_name}'
                AND end_of_calculation_period < '{end_of_calculation_period}'
                AND interval_type = '{interval_type}'
        )
        SELECT * FROM v WHERE rnk = 1;
    ''').toPandas()

    def get_float(decimal_value):
        return float(decimal_value) if decimal_value is not None else None

    for float_field in FLOAT_FIELDS_IN_STATISTICS_TABLE:
        previous_metrics_df[float_field] = previous_metrics_df[float_field].apply(get_float)
    return previous_metrics_df


DELTA = {
    'day': dt.timedelta(days=1),
    'week': dt.timedelta(days=7),
    'month': relativedelta(months=1),
}


def get_deviations_setup_df(spark, table_name):
    deviations_setup_df = spark.sql(f"""
        WITH v AS (
            SELECT
                *,
                ROW_NUMBER() OVER (PARTITION BY table_name, column_name ORDER BY period_from_dttm DESC) AS rn
            FROM {DEVIATIONS_SETUP_TABLE_NAME}
            WHERE table_name = '{table_name}'
        )
        SELECT 
            column_name,
            rows_cnt_limit,
            completed_rows_cnt_limit,
            nulls_cnt_limit,
            distinct_values_cnt_limit,
            minimum_value_limit,
            maximum_value_limit,
            median_value_limit,
            average_value_limit,
            sum_of_values_limit
        FROM v
        WHERE rn = 1;
        """).toPandas()
    logger.info(f"Выполнено извлечение порогов отклонений")
    return deviations_setup_df


def get_deviations_df(deviations_setup_df, current_table_statistics_df, previous_table_statistics_df):
    merged_statistics_df = pd.merge(current_table_statistics_df,
                                    previous_table_statistics_df,
                                    how='left',
                                    on=['table_name', 'column_name', 'column_id', 'data_type', 'comment'],
                                    suffixes=('_curr', '_prev'))

    # old_date = dt.date.today().replace(year=2000, month=1, day=1)
    # merged_statistics_df['report_dttm_prev'].fillna(old_date, inplace=True)

    metrics = {
        'rows_cnt': [],
        'completed_rows_cnt': [],
        'nulls_cnt': [],
        # nulls_amount_prc_list = []
        'distinct_values_cnt': [],
        'minimum_value': [],
        'maximum_value': [],
        'median_value': [],
        'average_value': [],
        'sum_of_values': [],
    }

    def compute_delta(df: pd.DataFrame, metric_name: str, row_index: int, list_to_save: list):
        previous_value = df.at[row_index, f'{metric_name}_prev']
        current_value = df.at[row_index, f'{metric_name}_curr']
        if np.isnan(previous_value) or np.isnan(current_value):
            q = np.nan
        else:
            m = min(previous_value, current_value)
            if m != 0:
                q = round(abs((df.at[i, f'{metric_name}_curr'] - previous_value) / m * 100), 2)
            elif current_value == 0 and previous_value == 0:
                q = 0
            else:
                q = 100
        list_to_save.append(q)

    for i in range(merged_statistics_df.shape[0]):
        for metric_name, metric_list in metrics.items():
            compute_delta(merged_statistics_df, metric_name, i, metric_list)

    for metric_name, metric_list in metrics.items():
        merged_statistics_df[f'delta_{metric_name}'] = metric_list

    limit_df = pd.merge(merged_statistics_df['column_name'], deviations_setup_df, how='left', on='column_name').fillna(PROC)
    merged_statistics_df = pd.merge(merged_statistics_df, limit_df, on='column_name')

    def compute_deviation_flg(df, metric_name: str):
        df[f'{metric_name}_flg'] = np.select([df[f'delta_{metric_name}'] > df[f'{metric_name}_limit']], [1], 0)

    for metric_name in metrics.keys():
        compute_deviation_flg(merged_statistics_df, metric_name)
    merged_statistics_df['dublicates_cnt_flg'] = np.select([merged_statistics_df['dublicates_cnt_curr'] > 0], [1], 0)
    merged_statistics_df['deviation_flg'] = 0
    merged_statistics_df['deviation_desc'] = ''

    def get_description(metric_name: str, row):
        if row[f'{metric_name}_flg'] == 1:
            if (row[f'{metric_name}_curr'] == 0 or row[f'{metric_name}_prev'] == 0) and row[f'{metric_name}_curr'] != row[f'{metric_name}_prev']:
                return f"Дельта по показателю {metric_name} не считаема, так как значение в одном из срезов = 0, "
            else:
                return f"Отклонение по столбцу {metric_name} {str(round(row[f'delta_{metric_name}'], 2))} %, "
        else:
            return ''

    # Расчёт общего флага и описания полученных отклонений
    for index, row in merged_statistics_df.iterrows():
        if (row['rows_cnt_flg'] == 1
                or row['completed_rows_cnt_flg'] == 1
                or row['nulls_cnt_flg'] == 1
                or row['dublicates_cnt_flg'] == 1
                or row['distinct_values_cnt_flg']
                or row['minimum_value_flg'] == 1
                or row['maximum_value_flg'] == 1
                or row['median_value_flg'] == 1
                or row['average_value_flg'] == 1
                or row['sum_of_values_flg'] == 1):
            merged_statistics_df.at[index, 'deviation_flg'] = 1

        deviation_desc = ""
        for metric_name in metrics.keys():
            deviation_desc += get_description(metric_name, row)

        if row['dublicates_cnt_flg'] == 1:
            deviation_desc += "Имеются полные дубликаты строк, "

        merged_statistics_df.at[index, 'deviation_desc'] = deviation_desc[:-2]
    logger.info("Рассчитан датафрейм отклонений")

    columns_to_rename1 = {f'{metric_name}_curr': metric_name for metric_name in metrics.keys()}
    columns_to_rename2 = {
        'end_of_calculation_period_curr': 'end_of_calculation_period',
        'interval_type_curr': 'interval_type',
        'dublicates_cnt_curr': 'dublicates_cnt',
    }

    merged_statistics_df.rename(columns=columns_to_rename1 | columns_to_rename2, inplace=True)
    result_columns = [
        'table_name', 'column_name', 'comment', 'data_type', 'column_id',
        'end_of_calculation_period', 'interval_type',
        'rows_cnt', 'delta_rows_cnt',
        'completed_rows_cnt', 'delta_completed_rows_cnt',
        'nulls_cnt', 'delta_nulls_cnt',
        'distinct_values_cnt', 'delta_distinct_values_cnt',
        'minimum_value', 'delta_minimum_value',
        'maximum_value', 'delta_maximum_value',
        'median_value', 'delta_median_value',
        'average_value', 'delta_average_value',
        'sum_of_values', 'delta_sum_of_values',
        'dublicates_cnt',
        'deviation_flg', 'deviation_desc',
    ]

    merged_statistics_df = merged_statistics_df[result_columns]
    merged_statistics_df['report_dttm'] = pd.Timestamp('now')
    return merged_statistics_df


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
        interval_type = row['INTERVAL_TYPE']
        delta = DELTA[interval_type]
        while next_check_dt <= current_date:
            end_of_calculation_period = next_check_dt - dt.timedelta(days=1)
            current_table_statistics_df = get_table_statistics(spark, table_name, date_field, interval_type, end_of_calculation_period)
            previous_table_statistics_df = get_previous_existing_metrics(spark, end_of_calculation_period, interval_type, table_name)

            deviations_setup_df = get_deviations_setup_df(spark, table_name)
            deviations_df = get_deviations_df(deviations_setup_df, current_table_statistics_df, previous_table_statistics_df)

            insert_pandas_df_into_hive(spark, current_table_statistics_df, STATISTICS_TABLE_NAME, FLOAT_FIELDS_IN_STATISTICS_TABLE)
            insert_pandas_df_into_hive(spark, deviations_df, METRICS_TABLE_NAME, FLOAT_FIELDS_IN_METRICS_TABLE)
            next_check_dt = next_check_dt + delta

        spark.sql(f'''
            INSERT INTO {SETUP_TABLE_NAME} VALUES 
            ('{table_name}', '{date_field}', cast('{next_check_dt}' as date), '{interval_type}');
            ''')
