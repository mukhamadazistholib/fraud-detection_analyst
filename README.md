# Data Fellowship Final Project
### Data Ingestion from Local Storage to Bquery
```py
from airflow import DAG
from airflow.contrib.operators.bigquery_operator import BigQueryOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 3, 21),
}

dag = DAG(
    'ingest_local_data_to_bigquery',
    default_args=default_args,
    schedule_interval=None,
)

#Reads data from local storage and returns as a pandas dataframe.
def read_local_data():
    data = pd.read_csv('"C:\PS_20174392719_1491204439457_log.csv"')
    return data

#Loads data to BigQuery.
def load_data_to_bigquery():

    # read data from XCom variable
    data = "{{ task_instance.xcom_pull(task_ids='read_data') }}"
    
    # create BigQuery hook
    bq_hook = BigQueryHook(bigquery_conn_id='<connection_id>')
    
    # load data to BigQuery
    bq_hook.insert_rows_from_dataframe(
        '<project_id>.<dataset_id>.<table_id>', data, 
        {'source': 'Airflow DAG'}
    )

read_data_task = PythonOperator(
    task_id='read_data',
    python_callable=read_local_data,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data_to_bigquery,
    dag=dag,
)

read_data_task >> load_data_task

```

#### PySpark Dataflow
![img](/assets/Drawing%20pyspark%20to%20GCS%20and%20GCP.drawio.png)