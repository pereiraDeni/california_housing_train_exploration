from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from python import functions_projects as fp

from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
datestring = "{{ ds }}"

default_args = {
    "owner": "DenilsonFreitas",
    "start_date": datetime(2022, 12, 15),
    "depends_on_past": False,
}

config = {
    "bucket_raw": "s3-raw-dadosmaistodos",
    "bucket_processed": "s3-processed-dadosmaistodos",
    "bucket_curated": "s3-curated-dadosmaistodos",
    "key_raw": "laptop_data_source/california_housing_train_raw_",
    "key_processed": "laptop_data_source/california_housing_train_processed_",
    "key_curated": "laptop_data_source/california_housing_train_curated_",
    "filename_raw": "/usr/local/airflow/dags/file/california_housing_train_raw",
    "filename_processed": "/usr/local/airflow/dags/file/california_housing_train_processed",
    "filename_curated": "/usr/local/airflow/dags/file/california_housing_train_curated"
}

filename ="/usr/local/airflow/dags/file/california_housing_train"

dag = DAG(
    "challenge-data-engineering",
    description="Carrega dados raw para o datalake, transformar alguns dados e transformar em parquet",
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    concurrency=5,
    max_active_runs=2,
)

load_csv_raw_s3 = PythonOperator(
    task_id=f"load_csv_raw_s3",
    dag=dag,
    python_callable=fp.upload_to_s3,
    op_kwargs={"istrue": False, "date": datestring, "filename": config["filename_raw"], "key": config["key_raw"], "bucket":config["bucket_raw"]}
    )

load_csv_processed_s3 = PythonOperator(
    task_id=f"load_csv_processed_s3",
    dag=dag,
    python_callable=fp.upload_to_s3,
    op_kwargs={"istrue": True, "date": datestring, "filename": config["filename_processed"], "key": config["key_processed"], "bucket":config["bucket_processed"]}
    )

load_csv_curated_s3 = PythonOperator(
    task_id=f"load_csv_curated_s3",
    dag=dag,
    python_callable=fp.upload_to_s3,
    op_kwargs={"istrue": True, "date": datestring, "filename": config["filename_curated"], "key": config["key_curated"], "bucket":config["bucket_curated"]}
    )

transform_columns = PythonOperator(
        task_id="transform_columns",
        dag=dag,
        python_callable=fp.transform_columns,
        op_kwargs={"date": datestring, "filename_raw": config["filename_raw"], "filename_processed": config["filename_processed"]}
        )

validate_dtypes = PythonOperator(
        task_id="validate_dtypes",
        dag=dag,
        python_callable=fp.validate_dtypes,
        op_kwargs={"date": datestring, "filename": config["filename_processed"]}
        )

agg_file_parquet = PythonOperator(
        task_id="agg_file_parquet",
        dag=dag,
        python_callable=fp.agg_file_parquet,
        op_kwargs={"date": datestring, "filename_processed": config["filename_processed"], "filename_curated": config["filename_curated"] }
        )

load_csv_raw_s3 >> transform_columns >> validate_dtypes >> load_csv_processed_s3 >>agg_file_parquet >> load_csv_curated_s3
