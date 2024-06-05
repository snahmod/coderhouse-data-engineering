import textwrap
from datetime import datetime, timedelta
import os
import pandas as pd
import psycopg2
import datetime
import functools
import json
import smtplib

from finnhub import Finnhub
from dotenv import load_dotenv

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator

load_dotenv()

with DAG(
    "stock-etl",
    default_args={
        "depends_on_past": False,
        "email": ["santiago.nahmod@widergy.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
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
  
  def config():
    with open('dags/config.json', 'r') as json_config:
        return json.load(json_config)

  def stocks():
    return config()['stocks']
  
  def thresholds():
    return config()['thresholds']
  
  def info():
    return config()['info']
  
  def read_data_from(context, task_ids):
    csv_filename = context['ti'].xcom_pull(task_ids=task_ids)
    return pd.read_csv(csv_filename)
  
  def retreive_price(stock):
    data = api().quote(stock['symbol'])
    result = {k: data[k] for k in ('c', 'h', 'l', 'o', 'pc', 't')}
    result = {**stock, **result}
    print(f"{stock['name']}: ${data['c']:,.2f}")
    return result

  def extract(**context):
    df = pd.DataFrame(map(retreive_price, stocks()))
    csv_filename = f"{context['ds']}_stocks_extract.csv"
    df.to_csv(csv_filename, index=False)
    return csv_filename
  
  def transform(**context):
    df = read_data_from(context, 'extract')

    df.rename(columns={'name': 'company', 'c': 'current_price', 'h': 'high_price', 'l': 'low_price', 'o': 'open_price', 'pc': 'last_close_price', 't': 'price_timestamp_unix'}, inplace=True)
    df.company = df.company.astype(str)
    df.symbol = df.symbol.astype(str)
    df['price_timestamp'] = pd.to_datetime(df.price_timestamp_unix, unit='s')
    df.drop_duplicates(subset=['symbol', 'price_timestamp'], keep='first', inplace=True)
    
    csv_filename = f"{context['ds']}_stocks_transform.csv"
    df.to_csv(csv_filename, index=False)
    return csv_filename
  
  def load(**context):
    create_table()

    df = read_data_from(context, 'transform')
    df['price_timestamp'] = pd.to_datetime(df.price_timestamp_unix, unit='s')

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
  
  def send_email(to, body_text):
    smtp = smtplib.SMTP('smtp.gmail.com', 587)
    smtp.starttls()
    smtp.login(os.environ['EMAIL_SENDER'],os.environ['EMAIL_APP_PASSWORD'])
    subject='Stock ETL | Alert Report'
    message='Subject: {}\n\n{}'.format(subject,body_text)
    smtp.sendmail('info@stocketl.com', to, message)
    print('Exito')

  def report_alerts(**context):
    s_info = info()

    df = read_data_from(context, 'transform')
    df['price_timestamp'] = pd.to_datetime(df.price_timestamp_unix, unit='s')

    alerts = []
    for threshold in thresholds():
      symbol = threshold['symbol']
      last_close_price = df[df.symbol == symbol].iloc[0]['last_close_price']
      if (threshold['min'] <= last_close_price <= threshold['max']):
        continue
      print(f"{s_info[symbol]['name']} ({symbol}: {last_close_price}) is out of threshold ({threshold['min']} / {threshold['max']})")
      a_type, limit = ('ABOVE', threshold['max']) if last_close_price > threshold['max'] else ('BELOW', threshold['min'])
      alerts.append({ 'symbol':  symbol, 'name': s_info[symbol]['name'],"type": a_type, 'limit': limit, 'cp': last_close_price})

    if not alerts:
      print('Nothing to report')
      return
    
    body_text = 'The following stocks were found to be out of their configured thresholds:\n\n'

    for alert in alerts:
      body_text += f"- {alert['name']} ({alert['symbol']}) current price ${'{:5,.2f}'.format(alert['cp'])} is {alert['type']} of limit ${'{:5,.2f}'.format(alert['limit'])}\n"
    send_email(config()['email'], body_text)



  t_extract = PythonOperator(task_id="extract", python_callable=extract, dag=dag, provide_context=True)
  t_transform = PythonOperator(task_id="transform", python_callable=transform, dag=dag, provide_context=True)
  t_load = PythonOperator(task_id="load", python_callable=load, dag=dag, provide_context=True)
  t_report_alerts = PythonOperator(task_id="report_alerts", python_callable=report_alerts, dag=dag, provide_context=True)

  t_extract >> t_transform >> [t_load, t_report_alerts]
