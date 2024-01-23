import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

project_id = 'peppy-ward-411412'
dataset_id = 'dbo'
table_id = 'tlc_green_trips_2014'
gcs_bucket = 'new_york_taxi_trips'

default_args = {
    'owner': 'orcun.birgul',
    'depends_on_past': False,
    'start_date': datetime(2022, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'load_gcs_to_bigquery',
    default_args=default_args,
    description='Load GCS to BigQuery',
    schedule_interval=None,  # Manually trigger,
    max_active_runs=1
)


def load_gcs_to_bigquery(ds_nodash, **kwargs):
    task_instance = kwargs['task_instance']
    date_to_load = Variable.get('date_to_load', default_var=datetime(2022, 1, 1))
    logging.info(f'Loading data for {date_to_load}')
    replaced_date_to_load = date_to_load.replace('-', '/')

    gcs_object_path = f'tlc_green_trips_2014/{replaced_date_to_load}/*.csv'

    gcs_to_bigquery_task = GCSToBigQueryOperator(
        task_id=f'load_to_bigquery_{date_to_load}',
        bucket=gcs_bucket,
        source_objects=[gcs_object_path],
        destination_project_dataset_table=f'{project_id}.{dataset_id}.{table_id}',
        source_format='CSV',
        skip_leading_rows=1,
        write_disposition='WRITE_TRUNCATE',
        create_disposition='CREATE_IF_NEEDED',
        gcp_conn_id='bigquery_default',  # GCP bağlantı adı
        dag=dag,
    )

    gcs_to_bigquery_task.execute(task_instance.get_template_context())


load_task = PythonOperator(
    task_id='load_task',
    python_callable=load_gcs_to_bigquery,
    provide_context=True,
    dag=dag,
)

load_task

if __name__ == "__main__":
    dag.cli()
