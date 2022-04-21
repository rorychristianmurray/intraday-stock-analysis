from bs4 import BeautifulSoup
import config
import requests
import pandas as pd
import psycopg2
import time
from pgcopy import CopyManager


def create_equity_majors_table():
    query_create_equity_majors_table = """CREATE TABLE equity_majors (
        stock_datetime timestamp(0) NOT NULL,
        symbol VARCHAR NULL,
        price_open float8 NULL,
        price_close float8 NULL,
        price_low float8 NULL,
        price_high float8 NULL,
        trading_volume int4 NULL);"""

    query_create_equity_majors_hypertable = (
        "SELECT create_hypertable('equity_majors', 'stock_datetime');"
    )

    conn = psycopg2.connect(
        database=config.DB_NAME,
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASS,
        port=config.DB_PORT,
    )

    cursor = conn.cursor()

    cursor.execute(query_create_equity_majors_table)

    cursor.execute(query_create_equity_majors_hypertable)

    conn.commit()

    cursor.close()


# scrapes ticker symbols of top 100 US companies by mkt cap
def get_symbols():

    url = (
        "https://companiesmarketcap.com/usa/largest-companies-in-the-usa-by-market-cap/"
    )

    html = requests.get(url).text

    soup = BeautifulSoup(html, "html.parser")

    symbols = [e.text for e in soup.select("div.company-code")]

    return symbols


def get_slice(month: int):
    slice = "year1month1"
    if month <= 12:
        return "year1month" + str(month)
    else:
        return "year2month" + str(month - 12)


def fetch_data(symbol, month: int):
    interval = 1
    slice = get_slice(month)
    apikey = config.APIKEY

    CSV_URL = """https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol={symbol}&interval={interval}min&slice={slice}&apikey={apikey}""".format(
        symbol=symbol, interval=interval, slice=slice, apikey=apikey
    )

    # print(CSV_URL)

    df = pd.read_csv(CSV_URL)
    df["symbol"] = symbol

    df = df.rename(
        columns={
            "time": "stock_datetime",
            "open": "price_open",
            "close": "price_close",
            "high": "price_high",
            "low": "price_low",
            "volume": "trading_volume",
        }
    )

    df["stock_datetime"] = pd.to_datetime(
        df["stock_datetime"], format="%Y-%m-%d %H:%M:%S"
    )

    records = [row for row in df.itertuples(index=False, name=None)]

    return records


def insert_to_db(records):

    conn = psycopg2.connect(
        database=config.DB_NAME,
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASS,
        port=config.DB_PORT,
    )

    cols = (
        "stock_datetime",
        "price_open",
        "price_close",
        "price_low",
        "price_high",
        "trading_volume",
        "symbol",
    )

    mgr = CopyManager(conn, "equity_majors", cols)

    mgr.copy(records)

    conn.commit()


def main():
    # create_equity_majors_table()

    # have done [50:] so far, need to do [:50 tomorrow]
    # 50 * 6 = 300
    symbols = get_symbols()[:50]
    print(symbols)

    for symbol in symbols:
        print("fetching data for ", symbol)
        for month in range(1, 7):
            stock_data = fetch_data(symbol, month)
            print(f"inserting data for {symbol} month {month}")
            insert_to_db(stock_data)
            time.sleep(15)
            continue


if __name__ == "__main__":
    main()


# https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY_EXTENDED&symbol=FB&interval=15min&slice=year2month2&apikey=RVXS95SBCA926VRN
