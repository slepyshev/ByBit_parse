import pandas as pd
import requests
import datetime
import pytz
import numpy as np
from clickhouse_driver import Client
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator


# создадим соединение с БД clickhouse
client = Client('some-clickhouse-server',
                user='default',
                port=9000,
                verify=False,
                database='default',
                settings={"numpy_columns": False, 'use_numpy': True},
                compression=False)

# получаем промежутки времени в нужном формате для api
tz_moscow = pytz.timezone('Europe/Moscow')
now = datetime.datetime.now(tz_moscow)
now = now.strftime('%Y-%m-%d %H')
now = datetime.datetime.strptime(now, '%Y-%m-%d %H')
now = int(now.timestamp())*1000
start_date = now - 114000000
end_date = now

# задаем валюту и интервал для api
list_currency = ['BTC', 'ETH']
interval = '60'


def get_data(currency, interval, start_date, end_date):
    # Формируем и отправляем запрос к api
    url = f'https://api-testnet.bybit.com/v5/market/kline?category=inverse&symbol={
        currency}USD&interval={interval}&start={start_date}&end={end_date}'
    res = requests.get(url)

    # Обрабатываем ответ от api и заносим данные в датафрейм
    res = res.json()
    res = res['result']['list']
    df = pd.DataFrame(data=res, columns=[
                      'open_time', 'open_price', 'high_price', 'low_price', 'close_price', 'volume', 'turnover'])
    df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
    df['open_time'] = df['open_time'] + pd.Timedelta(hours=3)
    df[['open_price', 'high_price', 'low_price', 'close_price']] = df[[
        'open_price', 'high_price', 'low_price', 'close_price']].astype(np.float64)

    # Добавляем дату загрузки данных
    dt_load = datetime.datetime.now()
    dt_load = dt_load.strftime('%Y-%m-%d %H:%M:%S')
    df['dt_load'] = dt_load
    df['currency'] = currency
    df.drop(['turnover'], axis=1, inplace=True)
    return df

# проверим существует ли таблица в БД если нет, создадим


def check_table(client):
    result = client.execute("EXISTS TABLE default.CryptoCurrencyPriceUSD")
    if result[0] == 0:
        client.execute("""
            CREATE TABLE default.CryptoCurrencyPriceUSD
            (
                open_time     Datetime,
            	open_price    Float64,
                high_price    Float64,
                low_price     Float64,
                close_price   Float64,
                volume  	  Float64,
                dt_load 	  Datetime,
                currency  	  String
            )
            ENGINE = ReplacingMergeTree(dt_load)
            ORDER BY (currency, open_time)
            SETTINGS index_granularity = 8192
            ;""")

# заносим данные в БД


def insert_to_database(client, df):
    client.insert_dataframe(
        f'INSERT INTO default.CryptoCurrencyPriceUSD VALUES', df)


def main():
    for currency in list_currency:
        df = get_data(currency, interval, start_date, end_date)
        check_table(client)
        insert_to_database(client, df)
    print('Данные успешно загружены с источника в БД')


# заворачиваем все в dag
with DAG(
    dag_id="loading_price_from_ByBit",
    start_date=datetime.datetime(2024, 11, 17),
    schedule_interval="@hourly",
    catchup=False,
) as dag:
    start = EmptyOperator(task_id='start')
    end = EmptyOperator(task_id='end')

    run_load_data = PythonOperator(
        task_id="load_data",
        python_callable=main)

    start >> run_load_data >> end
