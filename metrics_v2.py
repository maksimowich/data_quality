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

SETUP_TABLE_NAME = 'test.dq_setup_table'

DEVIATIONS_SETUP_TABLE_NAME = 'test.dq_deviations_setup'

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
                         interval_type: str,
                         end_of_calculation_period: dt.date):
    start_of_calculation_period = None
    if interval_type == 'day':
        start_of_calculation_period = end_of_calculation_period
        logger.info(f"Расчёт статистики для таблицы {table_name} за {end_of_calculation_period}")
    elif interval_type == 'week':
        start_of_calculation_period = end_of_calculation_period - dt.timedelta(days=6)
        logger.info(
            f"Расчёт статистики для таблицы {table_name} за неделю {start_of_calculation_period} - {end_of_calculation_period}")
    elif interval_type == 'month':
        start_of_calculation_period = end_of_calculation_period.replace(day=1)
        end_of_calculation_period = (end_of_calculation_period + relativedelta(months=1)).replace(day=1) - dt.timedelta(
            days=1)
        logger.info(
            f"Расчёт статистики для таблицы {table_name} за неделю {start_of_calculation_period} - {end_of_calculation_period}")

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
        'start_of_calculation_period', 'end_of_calculation_period', 'interval_type',
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
                        '{interval_type}' interval_type,
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
                        '{interval_type}' interval_type,
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
    df_result = df_result[res_columns].sort_values(['column_id', 'end_of_calculation_period', 'interval_type'])
    df_result['dublicate_cnt'] = df_result.duplicated().sum()

    if interval_type == 'day':
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
        SELECT
            column_name as column_name,
            rows_amount_cnt as rows_amount_cnt_limit,
            amount_completed_cnt as amount_completed_limit,
            nulls_amount_cnt as nulls_amount_limit,
            distinct_amount_cnt as distinct_amount_limit,
            minimum_value_info as minimum_value_limit,
            maximum_value_info as maximum_value_limit,
            median_value_info as median_value_limit,
            average_value_info as average_value_limit,
            sum_attributes_nval as sum_attributes_limit
        FROM {DEVIATIONS_SETUP_TABLE_NAME}
        WHERE 1=1
            AND table_name = '{table_name}'
            AND period_to_dt = '2100-01-01';
        """).toPandas()
    logger.info(f"Выполнено извлечение порогов отклонений")
    return deviations_setup_df


def get_deviation(deviation_setup_df, current_table_statistics_df, previous_table_statistics_df):
    merged_statistics_df = pd.merge(current_table_statistics_df,
                         previous_table_statistics_df,
                         how='left', on=['table_name', 'column_name', 'column_id', 'data_type', 'comment'],
                         suffixes=('_curr', '_prev'))

    old_date = dt.date.today().replace(year=2000, month=1, day=1)
    merged_statistics_df['report_dttm_prev'].fillna(old_date, inplace=True)

    # Расчёт отклонений по полям
    rows_amount_cnt_list = []
    amount_completed_list = []
    nulls_amount_list = []
    nulls_amount_prc_list = []
    distinct_amount_list = []
    minimum_value_list = []
    maximum_value_list = []
    median_value_list = []
    average_value_list = []
    sum_attributes_list = []

    for i in range(merged_statistics_df.shape[0]):
        # Расчет дельты общего количества строк
        if min(merged_statistics_df.at[i, 'rows_amount_cnt_curr'], merged_statistics_df.at[i, 'rows_amount_cnt_prev']) != 0:
            q = round((abs((merged_statistics_df.at[i, 'rows_amount_cnt_curr'] - merged_statistics_df.at[i, 'rows_amount_cnt_prev']) /
                           min(merged_statistics_df.at[i, 'rows_amount_cnt_curr'],
                               merged_statistics_df.at[i, 'rows_amount_cnt_prev']) * 100)), 2)
            rows_amount_cnt_list.append(q)
        elif merged_statistics_df.at[i, 'rows_amount_cnt_curr'] == 0 and merged_statistics_df.at[i, 'rows_amount_cnt_prev'] == 0:
            rows_amount_cnt_list.append(0)
        else:
            rows_amount_cnt_list.append(100)

        # Расчет дельты количества заполненных строк
        if min(merged_statistics_df.at[i, 'amount_completed_cnt_curr'], merged_statistics_df.at[i, 'amount_completed_cnt_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'amount_completed_cnt_curr'] - merged_statistics_df.at[
                    i, 'amount_completed_cnt_prev']) /
                     min(merged_statistics_df.at[i, 'amount_completed_cnt_curr'],
                         merged_statistics_df.at[i, 'amount_completed_cnt_prev']) * 100)), 2)
            amount_completed_list.append(q)
        elif merged_statistics_df.at[i, 'amount_completed_cnt_curr'] == 0 and merged_statistics_df.at[i, 'amount_completed_cnt_prev'] == 0:
            amount_completed_list.append(0)
        else:
            amount_completed_list.append(100)

        # Расчет дельты количества пустых значений
        if min(merged_statistics_df.at[i, 'nulls_amount_cnt_curr'], merged_statistics_df.at[i, 'nulls_amount_cnt_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'nulls_amount_cnt_curr'] - merged_statistics_df.at[i, 'nulls_amount_cnt_prev']) /
                     min(merged_statistics_df.at[i, 'nulls_amount_cnt_curr'],
                         merged_statistics_df.at[i, 'nulls_amount_cnt_prev']) * 100)), 2)
            nulls_amount_list.append(q)
        elif merged_statistics_df.at[i, 'nulls_amount_cnt_curr'] == 0 and merged_statistics_df.at[i, 'nulls_amount_cnt_prev'] == 0:
            nulls_amount_list.append(0)
        else:
            nulls_amount_list.append(100)

        # Расчет дельты уникальных значений
        if min(merged_statistics_df.at[i, 'distinct_amount_cnt_curr'], merged_statistics_df.at[i, 'distinct_amount_cnt_prev']) != 0:
            q = round((abs((merged_statistics_df.at[i, 'distinct_amount_cnt_curr'] - merged_statistics_df.at[
                i, 'distinct_amount_cnt_prev']) /
                           min(merged_statistics_df.at[i, 'distinct_amount_cnt_curr'],
                               merged_statistics_df.at[i, 'distinct_amount_cnt_prev']) * 100)), 2)
            distinct_amount_list.append(q)
        elif merged_statistics_df.at[i, 'distinct_amount_cnt_curr'] == 0 and merged_statistics_df.at[i, 'distinct_amount_cnt_prev'] == 0:
            distinct_amount_list.append(0)
        else:
            distinct_amount_list.append(100)

        # Расчет дельты минимальных значений
        if min(merged_statistics_df.at[i, 'minimum_value_info_curr'], merged_statistics_df.at[i, 'minimum_value_info_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'minimum_value_info_curr'] - merged_statistics_df.at[i, 'minimum_value_info_prev']) /
                     min(merged_statistics_df.at[i, 'minimum_value_info_curr'],
                         merged_statistics_df.at[i, 'minimum_value_info_prev']) * 100)), 2)
            minimum_value_list.append(q)
        elif merged_statistics_df.at[i, 'minimum_value_info_curr'] == 0 and merged_statistics_df.at[i, 'minimum_value_info_prev'] == 0:
            minimum_value_list.append(0)
        else:
            minimum_value_list.append(100)

        # Расчет дельты максимальных значений
        if min(merged_statistics_df.at[i, 'maximum_value_info_curr'], merged_statistics_df.at[i, 'maximum_value_info_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'maximum_value_info_curr'] - merged_statistics_df.at[i, 'maximum_value_info_prev']) /
                     min(merged_statistics_df.at[i, 'maximum_value_info_curr'],
                         merged_statistics_df.at[i, 'maximum_value_info_prev']) * 100)), 2)
            maximum_value_list.append(q)
        elif merged_statistics_df.at[i, 'maximum_value_info_curr'] == 0 and merged_statistics_df.at[i, 'maximum_value_info_prev'] == 0:
            maximum_value_list.append(0)
        else:
            maximum_value_list.append(100)

        # Расчет дельты медианы
        if min(merged_statistics_df.at[i, 'median_value_info_curr'], merged_statistics_df.at[i, 'median_value_info_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'median_value_info_curr'] - merged_statistics_df.at[i, 'median_value_info_prev']) /
                     min(merged_statistics_df.at[i, 'median_value_info_curr'],
                         merged_statistics_df.at[i, 'median_value_info_prev']) * 100)), 2)
            median_value_list.append(q)
        elif merged_statistics_df.at[i, 'median_value_info_curr'] == 0 and merged_statistics_df.at[i, 'median_value_info_prev'] == 0:
            median_value_list.append(0)
        else:
            median_value_list.append(100)

        # Расчет дельты средних значений
        if min(merged_statistics_df.at[i, 'average_value_info_curr'], merged_statistics_df.at[i, 'average_value_info_prev']) != 0:
            q = round(
                (abs((merged_statistics_df.at[i, 'average_value_info_curr'] - merged_statistics_df.at[i, 'average_value_info_prev']) /
                     min(merged_statistics_df.at[i, 'average_value_info_curr'],
                         merged_statistics_df.at[i, 'average_value_info_prev']) * 100)), 2)
            average_value_list.append(q)
        elif merged_statistics_df.at[i, 'average_value_info_curr'] == 0 and merged_statistics_df.at[i, 'average_value_info_prev'] == 0:
            average_value_list.append(0)
        else:
            average_value_list.append(100)

        # Расчет суммы атрибутов поля
        if min(merged_statistics_df.at[i, 'sum_attributes_nval_curr'],
               merged_statistics_df.at[i, 'sum_attributes_nval_prev']) != 0:
            q = round((abs((merged_statistics_df.at[i, 'sum_attributes_nval_curr'] - merged_statistics_df.at[
                i, 'sum_attributes_nval_prev']) /
                           min(merged_statistics_df.at[i, 'sum_attributes_nval_curr'],
                               merged_statistics_df.at[i, 'sum_attributes_nval_prev']) * 100)), 2)
            sum_attributes_list.append(q)
        elif merged_statistics_df.at[i, 'sum_attributes_nval_curr'] == 0 and merged_statistics_df.at[i, 'sum_attributes_nval_prev'] == 0:
            sum_attributes_list.append(0)
        else:
            sum_attributes_list.append(100)

    merged_statistics_df['rows_amount_cnt_delta'] = rows_amount_cnt_list
    merged_statistics_df['amount_completed_delta'] = amount_completed_list
    merged_statistics_df['nulls_amount_delta'] = nulls_amount_list
    merged_statistics_df['distinct_amount_delta'] = distinct_amount_list
    merged_statistics_df['minimum_value_delta'] = minimum_value_list
    merged_statistics_df['maximum_value_delta'] = maximum_value_list
    merged_statistics_df['median_value_delta'] = median_value_list
    merged_statistics_df['average_value_delta'] = average_value_list
    merged_statistics_df['sum_attributes_delta'] = sum_attributes_list

    df_limit = pd.merge(merged_statistics_df['column_name'], deviation_setup_df, how='left', on='column_name').fillna(PROC)
    merged_statistics_df = pd.merge(merged_statistics_df, df_limit, on='column_name')

    # Расчёт флагов отклонений по каждому из полей
    merged_statistics_df['rows_amount_cnt_flg'] = np.select(
        [merged_statistics_df['rows_amount_cnt_delta'] > merged_statistics_df['rows_amount_cnt_limit']], [1], 0)
    merged_statistics_df['amount_completed_flg'] = np.select(
        [merged_statistics_df['amount_completed_delta'] > merged_statistics_df['amount_completed_limit']], [1], 0)
    merged_statistics_df['nulls_amount_flg'] = np.select([merged_statistics_df['nulls_amount_delta'] > merged_statistics_df['nulls_amount_limit']], [1],
                                              0)
    merged_statistics_df['distinct_amount_flg'] = np.select(
        [merged_statistics_df['distinct_amount_delta'] > merged_statistics_df['distinct_amount_limit']], [1], 0)
    merged_statistics_df['minimum_value_flg'] = np.select([merged_statistics_df['minimum_value_delta'] > merged_statistics_df['minimum_value_limit']],
                                               [1], 0)
    merged_statistics_df['maximum_value_flg'] = np.select([merged_statistics_df['maximum_value_delta'] > merged_statistics_df['maximum_value_limit']],
                                               [1], 0)
    merged_statistics_df['median_value_flg'] = np.select([merged_statistics_df['median_value_delta'] > merged_statistics_df['median_value_limit']], [1],
                                              0)
    merged_statistics_df['average_value_flg'] = np.select([merged_statistics_df['average_value_delta'] > merged_statistics_df['average_value_limit']],
                                               [1], 0)
    merged_statistics_df['sum_attributes_flg'] = np.select([merged_statistics_df['sum_attributes_delta'] > merged_statistics_df['sum_attributes_limit']],
                                                [1], 0)
    merged_statistics_df['dublicate_cnt_flg'] = np.select([merged_statistics_df['dublicate_cnt_curr'] > 0], [1], 0)
    # merged_statistics_df.replace(np.inf, None, inplace = True)
    merged_statistics_df['deviation_flg'] = 0
    merged_statistics_df['deviation_desc'] = ''

    # Расчёт общего флага и описания полученных отклонений
    for index, row in merged_statistics_df.iterrows():
        if row['rows_amount_cnt_flg'] == 1 or row['amount_completed_flg'] == 1 or row['nulls_amount_flg'] == 1 \
                or row['dublicate_cnt_flg'] == 1 or row['distinct_amount_flg'] or row['minimum_value_flg'] == 1 \
                or row['maximum_value_flg'] == 1 or row['median_value_flg'] == 1 or row['average_value_flg'] \
                or row['sum_attributes_flg'] == 1:
            merged_statistics_df.at[index, 'deviation_flg'] = 1

        deviation_desc = ""
        if row['rows_amount_cnt_flg'] == 1:
            if (row['rows_amount_cnt_curr'] == 0 or row['rows_amount_cnt_prev'] == 0) and row[
                'rows_amount_cnt_curr'] != \
                    row['rows_amount_cnt_prev']:
                deviation_desc += "Дельта по показателю ROWS_AMOUNT_IN_ITERATION не считаема, так как " \
                                  "значение в одном из срезов = 0, "
            else:
                deviation_desc += "Отклонение по столбцу rows_amount_cnt " + str(
                    round(row['rows_amount_cnt_delta'], 2)) + "%, "
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

        merged_statistics_df.at[index, 'deviation_desc'] = deviation_desc[:-2]
    logger.info("Рассчитан датафрейм отклонений")

    # Требуемые поля для таблицы
    columns_merged = [
        'table_name', 'column_name', 'comment',
        'data_type', 'column_id',
        'report_dttm_prev', 'report_dttm_curr',
        'rows_amount_cnt_curr', 'rows_amount_cnt_delta',
        'amount_completed_cnt_curr', 'amount_completed_delta',
        'nulls_amount_cnt_curr', 'nulls_amount_delta',
        'distinct_amount_cnt_curr', 'distinct_amount_delta',
        'minimum_value_info_curr', 'minimum_value_delta',
        'maximum_value_info_curr', 'maximum_value_delta',
        'median_value_info_curr', 'median_value_delta',
        'average_value_info_curr', 'average_value_delta',
        'sum_attributes_nval_curr', 'sum_attributes_delta',
        'dublicate_cnt_curr', 'deviation_flg', 'deviation_desc',
        'period_from_dt', 'period_to_dt'
    ]
    # Заменяем дату period_to_dt в старых срезах
    open_date = dt.datetime.strptime('2100-01-01 00:00:00', "%Y-%m-%d %H:%M:%S")
    now_date = dt.datetime.now()

    # spark.sql(f"""
    #     UPDATE {table_for_recording}
    #     SET period_to_dt = '{now_date.strftime("%Y-%m-%d %H:%M:%S")}'
    #     where 1=1
    #         AND report_dttm = '{current_date}'
    #         AND table_name = '{full_table_name}'
    #         AND period_to_dt = '{open_date}';
    # """)

    merged_statistics_df['period_from_dt'] = (now_date + dt.timedelta(seconds=1)).strftime("%Y-%m-%d %H:%M:%S")
    merged_statistics_df['period_to_dt'] = dt.datetime.strptime('2100-01-01 00:00:00', '%Y-%m-%d %H:%M:%S')
    merged_statistics_df = merged_statistics_df[columns_merged]

    # logging.info(f"Выполнен расчёт отклонений за {merged_statistics_df['report_dttm_curr'].unique()[0]}: {dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
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
            table_statistics_df = get_table_statistics(spark, table_name, date_field, interval_type,
                                                       end_of_calculation_period)
            insert_statistics(spark, table_statistics_df)
            previous_table_statistics_df = get_previous_existing_metrics(spark, end_of_calculation_period,
                                                                         interval_type, table_name)
            # if previous_table_statistics_df is None
            next_check_dt = next_check_dt + delta
        spark.sql(f'''
            INSERT INTO {SETUP_TABLE_NAME} VALUES 
            ('{table_name}', '{date_field}', cast('{next_check_dt}' as date), '{interval_type}');
            ''')
