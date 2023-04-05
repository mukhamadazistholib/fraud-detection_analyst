import os
from airflow import DAG
from airflow import models
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryDeleteDatasetOperator,
)
from airflow.providers.google.cloud.operators.gcs import(GCSCreateBucketOperator,
                                                         GCSCreateBucketOperator)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.dates import days_ago
from datetime import datetime

DATASET_NAME = os.environ.get("GCP_DATASET_NAME", 'iykra_project_new')
TABLE_NAME = os.environ.get("GCP_TABLE_NAME", 'fraud_online')


dag = models.DAG(
    dag_id='gcs_to_bq_operator',
    start_date=datetime(2023, 4, 1),
    schedule_interval=None,
)

create_bucket=GCSCreateBucketOperator(
    task_id='creating_bucket',
    bucket_name = 'iykra_project',
    storage_class='MULTI_REGIONAL',
    location='US',
    project_id='skilful-scarab-339408',
    dag=dag
)
upload_data_from_another_source=GCSToGCSOperator(
    task_id="upload_another_source_data_to_gcs",
    source_bucket='skilful-scarab-339408',
    source_object='data/onlinefraud.csv',
    destination_bucket='iykra_project',
    destination_object='data/onlinefraud.csv',
    gcp_conn_id='google_cloud_default',
    dag=dag,
)

create_test_dataset = BigQueryCreateEmptyDatasetOperator(
    task_id='create_airflow_test_dataset', 
    dataset_id=DATASET_NAME, 
    dag=dag
)

# [START howto_operator_gcs_to_bigquery]
load_csv = GCSToBigQueryOperator(
    task_id='gcs_to_bigquery',
    bucket='iykra_project',
    source_objects=['data/onlinefraud.csv'],
    destination_project_dataset_table=f"{DATASET_NAME}.{TABLE_NAME}",
    schema_fields=[
        {'name': 'step', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'type', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'nameOrig', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'oldbalanceOrg', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'newbalanceOrig', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'nameDest', 'type': 'STRING', 'mode': 'NULLABLE'},
        {'name': 'oldbalanceDest', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'newbalanceDest', 'type': 'FLOAT', 'mode': 'NULLABLE'},
        {'name': 'isFraud', 'type': 'INTEGER', 'mode': 'NULLABLE'},
        {'name': 'isFlaggedFraud', 'type': 'INTEGER', 'mode': 'NULLABLE'}
    ],
    write_disposition='WRITE_TRUNCATE',
    dag=dag,
)
[create_test_dataset,create_bucket] >> upload_data_from_another_source >> load_csv