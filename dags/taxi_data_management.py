import json
from datetime import datetime, timedelta
import logging
import pandas as pd
from pandas_gbq import to_gbq
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from h3 import h3
from google.oauth2 import service_account

bigquery_service_account = Variable.get("bigquery_service_account", deserialize_json=True)
credentials = service_account.Credentials.from_service_account_info(bigquery_service_account)

# SQLAlchemy Engine only accepts a file path for BigQuery credentials
with open('bigquery_service_account.json', 'w') as f:
    f.write(json.dumps(bigquery_service_account))
engine = create_engine('bigquery://peppy-ward-411412/dbo', credentials_path='bigquery_service_account.json')


def calculate_daypart(row):
    pickup_datetime = pd.to_datetime(row["pickup_datetime"])
    hour = pickup_datetime.hour
    if 6 <= hour <= 11:
        return "Morning"
    elif 12 <= hour <= 17:
        return "Noon"
    elif 18 <= hour <= 23:
        return "Evening"
    elif 0 <= hour <= 5:
        return "Night"
    else:
        return "Unknown"


def calculate_hexagon(row):
    pickup_hexagon = h3.geo_to_h3(row["pickup_latitude"], row["pickup_longitude"], 9)
    dropoff_hexagon = h3.geo_to_h3(row["dropoff_latitude"], row["dropoff_longitude"], 9)
    return pickup_hexagon, dropoff_hexagon


# the most popular pickup hexagons
def get_popular_pickup_hexagons(df):
    hexagon_counts = (df.groupby(['daypart', 'pickup_hexagon'])
                      .size()
                      .reset_index(name='count')
                      .sort_values(['daypart', 'count'], ascending=[True, False])
                      .groupby('daypart')
                      .head(5)).reset_index(drop=True)
    hexagon_counts["type"] = "pickup"
    hexagon_counts = hexagon_counts[["type", "daypart", "pickup_hexagon", "count"]]
    return hexagon_counts


# the most popular dropoff hexagons
def get_popular_dropoff_hexagons(df):
    hexagon_counts = (df.groupby(['daypart', 'dropoff_hexagon'])
                      .size()
                      .reset_index(name='count')
                      .sort_values(['daypart', 'count'], ascending=[True, False])
                      .groupby('daypart')
                      .head(5)).reset_index(drop=True)
    hexagon_counts["type"] = "dropoff"
    hexagon_counts = hexagon_counts[["type", "daypart", "dropoff_hexagon", "count"]]
    return hexagon_counts


# the most popular routes
def get_popular_routes(df):
    hexagon_counts = (df.groupby(['daypart', "pickup_hexagon", 'dropoff_hexagon'])
                      .size()
                      .reset_index(name='count')
                      .sort_values(['daypart', 'count'], ascending=[True, False])
                      .groupby('daypart')
                      .head(5)).reset_index(drop=True)
    hexagon_counts["type"] = "route"
    hexagon_counts = hexagon_counts[["type", "daypart", "pickup_hexagon", "dropoff_hexagon", "count"]]
    return hexagon_counts


def extract_data():
    query = """
    WITH Centroids AS (
      SELECT
        zone_id,
        ST_CENTROID(zone_geom) AS centroid
      FROM
        `peppy-ward-411412.dbo.taxi_zone_geom`
    )

    SELECT
      a.*,
      ST_X(p.centroid) AS pickup_latitude,
      ST_Y(p.centroid) AS pickup_longitude,
      ST_X(d.centroid) AS dropoff_latitude,
      ST_Y(d.centroid) AS dropoff_longitude
    FROM
      `peppy-ward-411412.dbo.tlc_green_trips_2014` a
    LEFT JOIN
      Centroids p ON a.pickup_location_id = CAST(p.zone_id as INT64)
    LEFT JOIN
      Centroids d ON a.dropoff_location_id = CAST(d.zone_id as INT64);
    """
    df = pd.read_sql(query, engine)
    df["index"] = df.index
    return df


def process_data(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(task_ids='extract_data_task')

    df["daypart"] = df.apply(calculate_daypart, axis=1)
    df[['pickup_hexagon', 'dropoff_hexagon']] = df.apply(calculate_hexagon, axis=1, result_type="expand")
    # There are some null values for ['pickup_latitude', 'pickup_longitude', 'dropoff_latitude', 'dropoff_longitude']
    df = df.dropna(subset=['pickup_latitude', 'dropoff_latitude']).reset_index(drop=True)

    popular_pickup = get_popular_pickup_hexagons(df)
    popular_dropoff = get_popular_dropoff_hexagons(df)
    popular_routes = get_popular_routes(df)
    df.drop(columns=["index"], inplace=True)

    popular_hexagons = pd.concat([popular_pickup, popular_dropoff, popular_routes], ignore_index=True)

    ti.xcom_push(key='popular_hexagons', value=popular_hexagons)


def load_data(**kwargs):
    ti = kwargs['ti']
    popular_hexagons = ti.xcom_pull(task_ids='process_data_task', key='popular_hexagons')
    df = ti.xcom_pull(task_ids='extract_data_task')

    logging.info("Starting to load data to BigQuery...")

    to_gbq(popular_hexagons, 'dbo.popular_hexagons', project_id='peppy-ward-411412', if_exists='replace',
           credentials=credentials)
    to_gbq(df, 'dbo.tlc_green_trips_2014_latest', project_id='peppy-ward-411412', if_exists='replace',
           credentials=credentials)

    logging.info("Data loaded to BigQuery successfully!")


default_args = {
    'owner': 'orcun.birgul',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'Taxi_Data_Management',
    default_args=default_args,
    description='Data Management for NYC Taxi Trips',
    schedule_interval=None,  # Manually trigger,
    max_active_runs=1
)

extract_data_task = PythonOperator(
    task_id='extract_data_task',
    python_callable=extract_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data_task',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_task',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

extract_data_task >> process_data_task >> load_data_task
