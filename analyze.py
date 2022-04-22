import plotly.express as px
import pandas as pd
import psycopg2
import config

# test change
def make_conn():
    conn = psycopg2.connect(
        database=config.DB_NAME,
        host=config.DB_HOST,
        user=config.DB_USER,
        password=config.DB_PASS,
        port=config.DB_PORT,
    )

    return conn


def get_renderer():
    renderers = px

    print(renderers)


# how has stock price changed over time?
def stock_price_over_time():

    conn = make_conn()

    query = """
    SELECT symbol, time_bucket('7 days', stock_datetime) AS time_frame, last(price_close, stock_datetime) AS last_closing_price from equity_majors
    WHERE symbol = 'CAT'
    GROUP BY time_frame, symbol
    ORDER BY time_frame
    """

    df = pd.read_sql(query, conn)

    # create line chart
    figure = px.line(df, x="time_frame", y="last_closing_price")

    figure.show()


# trading volume over time
def trading_volume_symbol():
    conn = make_conn()

    query = """
    SELECT symbol, time_bucket('7 days', stock_datetime) AS time_frame, sum(trading_volume) AS volume from equity_majors
    WHERE symbol = 'CAT'
    GROUP BY time_frame, symbol
    ORDER BY time_frame
    """

    df = pd.read_sql(query, conn)

    # create line chart
    figure = px.line(df, x="time_frame", y="volume")

    figure.show()


# symbols by volume
def trading_volume_compare():
    conn = make_conn()

    query = """
    SELECT symbol, sum(trading_volume) AS volume 
    FROM equity_majors
    WHERE (now() - date(stock_datetime)) < INTERVAL '14 DAY'
    GROUP BY symbol
    ORDER BY volume DESC
    """

    df = pd.read_sql(query, conn)

    # print(df.head(3))

    # create bar chart
    figure = px.bar(df, x="symbol", y="volume")

    # figure.show(renderer="png")
    figure.show()


# Compare price performance for FAANG stocks
def compare_faang():
    conn = make_conn()

    query = """
    SELECT symbol, time_bucket('7 days', stock_datetime) AS time_frame, last(price_close, stock_datetime) AS last_closing_price 
    FROM equity_majors
    WHERE symbol IN ('FB', 'AAPL', 'AMZN', 'GOOG', 'NFLX')
    GROUP BY time_frame, symbol
    ORDER BY time_frame
    """

    df = pd.read_sql(query, conn)

    # create line chart
    figure = px.line(df, x="time_frame", y="last_closing_price", color="symbol")

    # figure.show(renderer="png")
    figure.show()


# which symbols had most daily/weekly/monthly gains/losses
def compare_gain_loss():
    conn = make_conn()

    query = """
    SELECT *, (closing_price - opening_price)/opening_price * 100 AS percent_change
    FROM (
        SELECT symbol, time_bucket('30 days', stock_datetime) AS time_frame, 
        last(price_close, stock_datetime) as closing_price, 
        first(price_close, stock_datetime) as opening_price
        FROM equity_majors
        GROUP BY time_frame, symbol
    ) AS change
    ORDER BY percent_change DESC
    """

    df = pd.read_sql(query, conn)

    # create line chart
    figure = px.bar(df, x="symbol", y="percent_change", color="symbol")

    # figure.show(renderer="png")
    figure.show()


if __name__ == "__main__":
    # query1()
    compare_faang()
