import textwrap
from datetime import datetime, timedelta
import requests
import os
import pandas as pd
import psycopg2
import datetime
import functools
from dotenv import load_dotenv

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

load_dotenv()

# API Class
class Finnhub:
  BASE_URL="https://finnhub.io/api/v1"

  def __init__(self, token):
    self.token = token
  
  def quote(self, symbol):
    params = { "symbol": symbol, "token": self.token }
    res = requests.get(f'{self.BASE_URL}/quote', params=params)
    res.raise_for_status()
    return res.json()


with DAG(
    "stock-etl",
    default_args={
        "depends_on_past": False,
        "email": ["santiago.nahmod@widergy.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 5,
        "retry_delay": timedelta(minutes=5),
    },
    description="An ETL that retreives stock prices and loads them to a DW. Runs daily.",
    schedule=timedelta(days=1),
    start_date=datetime.datetime(2024, 5, 15),
    catchup=False,
    tags=["etl"],
) as dag:
  
  @functools.cache
  def conn():
    try:
      conn = psycopg2.connect(
          host=os.environ['REDSHIFT_HOST'],
          dbname='data-engineer-database',
          user=os.environ['REDSHIFT_USER'],
          password=os.environ['REDSHIFT_PASSWORD'],
          port='5439'
      )
      print('Conectado a Redshift con éxito!')
      return conn
    except Exception as e:
        print('No es posible conectar a Redshift')
        print(e)
        return None

  
  def create_table():
    with conn().cursor() as cur:
      cur.execute("""
          create table if not exists stock_history(
              symbol varchar(100) not null, 
              company varchar(100),
              current_price	float,
              high_price float,
              low_price	float,
              open_price float,
              last_close_price float,
              price_timestamp timestamp not null ,
              created_at timestamp,
              primary key (symbol, price_timestamp)
              );
      """)
      conn().commit()
  
  @functools.cache
  def api():
    return Finnhub(token=os.environ['FINNHUB_API_TOKEN'])

  def stocks():
    return [ {'name': 'Apple', 'symbol': 'AAPL'},
             {'name': 'Google', 'symbol': 'GOOGL'},
             {'name': 'Coca Cola', 'symbol': 'KO'},
             {'name': 'Microsoft', 'symbol': 'MSFT'},
             {'name': 'Amazon', 'symbol': 'AMZN'},
             {'name': 'JP Morgan', 'symbol': 'JPM'},
             {'name': 'Spotify', 'symbol': 'SPOT'},
             {'name': 'Disney', 'symbol': 'DIS'},
             {'name': 'Tesla', 'symbol': 'TSLA'},
             {'name': 'Intel', 'symbol': 'INTC'}
            ]
  
  def retreive_price(stock):
    data = api().quote(stock['symbol'])
    result = {k: data[k] for k in ('c', 'h', 'l', 'o', 'pc', 't')}
    result = {**stock, **result}
    print(f"{stock['name']}: ${data['c']:,.2f}")
    return result

  def extract():
    return map(retreive_price, stocks())
  
  def transform(prices):
    df = pd.DataFrame(prices)
    df.rename(columns={'name': 'company', 'c': 'current_price', 'h': 'high_price', 'l': 'low_price', 'o': 'open_price', 'pc': 'last_close_price', 't': 'price_timestamp_unix'}, inplace=True)
    df.company = df.company.astype(str)
    df.symbol = df.symbol.astype(str)
    df['price_timestamp'] = pd.to_datetime(df.price_timestamp_unix, unit='s')
    df.drop_duplicates(subset=['symbol', 'price_timestamp'], keep='first', inplace=True)
    return df
  
  def load(df):
    create_table()
    with conn().cursor() as cur:

      for index, row in df.iterrows():
          # Verificar si ya existe el registro
          cur.execute(
              "SELECT 1 FROM stock_history WHERE symbol = %s AND price_timestamp = %s",
              (row["symbol"], row["price_timestamp"]),
          )
          already_exists = cur.fetchone()
          
          if already_exists is None:
              # Ejecutar insert
              cur.execute(
                  """INSERT INTO stock_history(
                                  symbol,
                                  company,
                                  current_price,
                                  high_price,
                                  low_price,
                                  open_price,
                                  last_close_price,
                                  price_timestamp,
                                  created_at) VALUES %s
      """,
                  [
                      (
                          row["symbol"],
                          row["company"],
                          row["current_price"],
                          row["high_price"],
                          row["low_price"],
                          row["open_price"],
                          row["last_close_price"],
                          row["price_timestamp"].isoformat(timespec="seconds"),
                          datetime.datetime.now(datetime.UTC).isoformat(timespec="seconds"),
                      )
                  ],
              )
      conn().commit()
      conn().close()


  def run_etl():
    prices = extract()
    df = transform(prices)
    load(df)
    print("Done")


  t1 = PythonOperator(task_id="run_etl", python_callable=run_etl)
