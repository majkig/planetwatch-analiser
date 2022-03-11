import json
import os
import base64
import requests
import pandas as pd
from datetime import date, datetime
from datetime import timedelta

from constants import ADDRESS_LIST

PLANET_ASSET_ID = "27165954"
START_DATE = "2022-01-01"
UNIT = 1000000


def create_directory():
    if not os.path.exists("data"):
        os.makedirs("data")


def get_transactions(address, day):
    after = day - timedelta(days=1)
    url = "https://algoindexer.algoexplorerapi.io/v2/accounts/" + address + "/transactions?asset-id=" + \
          PLANET_ASSET_ID + "&before-time=" + day.strftime("%Y-%m-%d") + "&after-time=" \
          + after.strftime("%Y-%m-%d") + "&currency-greater-than=0"
    req = requests.get(url)
    return req.json()["transactions"]


def process_transactions(data, address, day):
    df = pd.DataFrame([], columns=["Date", "Device", "Amount"])
    for t in data:
        if t["asset-transfer-transaction"]["receiver"] == address:
            df_temp = pd.DataFrame([(day, get_device_name(t["note"]),
                                     t["asset-transfer-transaction"]["amount"] / UNIT)],
                                   columns=["Date", "Device", "Amount"])
            df = pd.concat([df, df_temp], ignore_index=True)
    return df


def get_device_name(note):
    return json.loads(base64.b64decode(note))["deviceId"]


def check_month(dates):
    file_path = "data/" + dates[0].strftime("%B%Y") + ".csv"
    if os.path.exists(file_path):
        df = pd.read_csv(file_path)
        return len(dates) == len(df)
    return False


def get_end_of_month(day):
    next_month = day.replace(day=28) + timedelta(days=4)
    return next_month - timedelta(days=next_month.day)


def process_month(dates):
    df = pd.DataFrame([], columns=["Date", "Device", "Amount"])
    print("Processing month: " + dates[0].strftime("%B %Y"))
    for day in dates:
        for address in ADDRESS_LIST:
            data = get_transactions(address, day)
            tt = process_transactions(data, address, day.strftime("%Y-%m-%d"))
            df = pd.concat([df, tt], ignore_index=True)
    df = df.set_index(["Date", "Device"])["Amount"].unstack()
    df.to_csv("data/" + dates[0].strftime("%B%Y") + ".csv")


def process_rewards():
    create_directory()
    months = pd.date_range(start=START_DATE, end=date.today(), freq='MS')
    for day in months:
        if day.month == date.today().month:
            dates = pd.date_range(day, date.today())
        else:
            dates = pd.date_range(day, get_end_of_month(day))
        if check_month(dates):
            print(dates[0].strftime("%B %Y") + " already processed")
        else:
            process_month(dates)


process_rewards()
