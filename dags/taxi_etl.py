from datetime import datetime, timedelta
from airflow import DAG
# from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator


# Default arguments for DAGs
default_args = {
    'owner': 'orcun.birgul',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Taxi_Trips_ETL_Pipeline',
          default_args=default_args,
          description='ETL Pipeline for NYC Taxi Trips',
          schedule_interval="0 8 * * *",
          catchup=False,
          max_active_runs=1)


trigger_load_data_dag_task = TriggerDagRunOperator(
    task_id='trigger_load_data_dag',
    trigger_dag_id='load_gcs_to_bigquery',
    dag=dag,
)

trigger_taxi_data_dag_task = TriggerDagRunOperator(
    task_id='trigger_taxi_data_dag',
    trigger_dag_id='Taxi_Data_Management',
    dag=dag,
)


trigger_load_data_dag_task >> trigger_taxi_data_dag_task


if __name__ == "__main__":
    dag.cli()
