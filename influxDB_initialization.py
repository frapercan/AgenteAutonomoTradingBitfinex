import json
import requests
import pendulum
import ntplib  # Network Time protocol
from influxdb_client import InfluxDBClient, Point
import time
import pandas as pd
import numpy as np
import logging

from influxdb_client.client.write_api import SYNCHRONOUS

logging.basicConfig(filename='log', level=logging.DEBUG)
params = {
    "url": "http://localhost:9999",
    "org": "US",
    "token": "zDxSwoznrqSipuTs2VJfrOJl-e2twnCKsraGd-K_4YKUY0c_EB9fb341_kCi0lQqz9dUx_yWYGKUqiZaI2cGOA==",
    "http_api_url": 'https://api-pub.bitfinex.com/v2/',
    "pair": 'tBTCUSD',
    "timeframe": '1m'}

ERROR_CODE_UNKNOWN_EVENT = 10000
ERROR_CODE_UNKNOWN_PAIR = 10001
ERROR_CODE_SUBSCRIPTION_FAILED = 10300
ERROR_CODE_ALREADY_SUBSCRIBED = 10301
ERROR_CODE_UNKNOWN_CHANNEL = 10302
ERROR_CODE_CHANNEL_LIMIT = 10305
ERROR_CODE_RATE_LIMIT = 11010

INFO_CODE_RECONNECT = 20051
INFO_CODE_START_MAINTENANCE = 20060
INFO_CODE_END_MAINTENANCE = 20061
ERROR_CODE_START_MAINTENANCE = 20006

NTPClient = ntplib.NTPClient()

client = InfluxDBClient(
    url=params['url'],
    token=params['token'],
    org=params['org']
)

def populate_db(pair, sample_distance):
    now = NTPClient.request('time.google.com')
    pivot_dt = '1007681680000'
    while 1:
        print(pivot_dt)
        # Build url
        url = params['http_api_url'] + 'candles/trade:{sample_distance}:{pair}/' \
                                       'hist?limit={limit}&start={pivot_dt}&sort=1' \
            .format(sample_distance=sample_distance, pair=pair, limit=10000, pivot_dt=pivot_dt)
        print(url)
        # Request API
        json_response = requests.get(url)
        response = json.loads(json_response.text)
        time.sleep(3)
        if 'error' in response:

            # Check rate limit
            if response[1] == ERROR_CODE_RATE_LIMIT:
                print('Error: reached the limit number of requests. Wait 120 seconds...')
                time.sleep(120)
                continue
            # Check platform status
            elif response[1] == ERROR_CODE_START_MAINTENANCE:
                print('Error: platform is in maintenance. Forced to stop all requests.')
                break
        else:
            # Get last timestamp of request (in second, so divided by 1000)
            print(response)

            last_dt = int(response[::-1][0][0]) // 1000
            print('2')
            last_dt = pendulum.from_timestamp(last_dt)

            # Put it as new start datetime (in millisecond, so multiplied by 1000)
            if pivot_dt == last_dt.int_timestamp * 1000:
                break
            pivot_dt = last_dt.int_timestamp * 1000
            client.write_api().write(record=serialize_points(response), bucket=pair)


def keep_updated(last_data_timestamp,pair, sample_distance):
    pivot_dt = last_data_timestamp
    while 1:
        print(pivot_dt,'keep_updated')
        # Build url
        url = url_generator(pair, sample_distance, pivot_dt)
        print(url)
        # Request API
        json_response = requests.get(url)
        response = json.loads(json_response.text)
        time.sleep(3)
        if 'error' in response:

            # Check rate limit
            if response[1] == ERROR_CODE_RATE_LIMIT:
                print('Error: reached the limit number of requests. Wait 120 seconds...')
                time.sleep(120)
                continue
            # Check platform status
            elif response[1] == ERROR_CODE_START_MAINTENANCE:
                print('Error: platform is in maintenance. Forced to stop all requests.')
                break
        else:
            # Get last timestamp of request (in second, so divided by 1000)
            print(response)

            last_dt = int(response[::-1][0][0]) // 1000
            print('2')
            last_dt = pendulum.from_timestamp(last_dt)

            # Put it as new start datetime (in millisecond, so multiplied by 1000)
            if pivot_dt == last_dt.int_timestamp * 1000:
                break
            pivot_dt = last_dt.int_timestamp * 1000
            client.write_api().write(record=serialize_points(response), bucket=pair)


def data_sync_service(pair, sample_distance):
    """
    Main function, keeps data updated with a short lag and without any gap.
    """
    query = 'from(bucket: "{}") |> range(start: -9999d) |> last()'.format(pair)
    df = client.query_api().query_data_frame(query)
    if df.empty:
        populate_db(pair, sample_distance)
    check_data_integrity_between_fields(df)
    last_data_timestamp = df._time[0].timestamp() * 1000
    while(1):
        keep_updated(last_data_timestamp, pair, sample_distance)
        time.sleep(60)


def check_data_integrity_between_fields(df):
    if df.values.shape != (5, 8):
        logging.warning(msg='some field is missing in the dataframe{}'.format(df)+'-'+pendulum.now().to_datetime_string())  # OHLVC


def serialize_points(response):
    points = []
    for tick in list(response):
        timestamp = pendulum.from_timestamp(int(tick[0]) // 1000)
        timestamp = timestamp.in_tz('UTC')
        timestamp = timestamp.to_atom_string()
        open_price = float(tick[1])
        low_price = float(tick[2])
        high_price = float(tick[3])
        close_price = float(tick[4])
        volume = float(tick[5])
        point = Point(params['pair']).field('close', close_price).field('open', open_price).field('high', high_price) \
            .field('low', low_price).field('volume', volume).time(timestamp)
        points.append(point)
    # Return True is operation is successful
    return points


def url_generator(pair, sample_distance, dt):
    return params['http_api_url'] + 'candles/trade:{sample_distance}:{pair}/' \
                                    'hist?limit={limit}&start={pivot_dt}&sort=1' \
        .format(sample_distance=sample_distance, pair=pair, limit=10000, pivot_dt=dt)


if __name__ == '__main__':
    try:
        data_sync_service(params['pair'], '1m')

    except Exception as e:
        print(e)
