# Data Fellowship Final Project - Fraud Detection analyst End to End Data Pipeline
## Background
Online payment fraud has been a growing concern for businesses and consumers. 
According to a report by Juniper Research, global online payment fraud losses are expected to reach $48 billion by 2023.
- Online payments have become increasingly popular
- The risk of online payment fraud also increase
- Impact the business financially and credibility
- Impact on customers trust 

## Problem Statement
How to detect fraudulent transactions and provide insights to businesses to help them prevent fraudulent activities?

## Goals
1. Develop an end-to-end data pipeline that allows data analysts and business intelligence professionals to choose between batched data and real-time streamed data.
2. Build an analytics dashboard that utilizes data from the analytics database to generate meaningful insights for the organization.

## PySpark Dataflow
![img](/assets/Drawing%20pyspark%20to%20GCS%20and%20GCP.drawio.png)

### Installation and Usage Guidelines

====================
- Prerequisite:

    A. Python 3: [Install Here](https://www.python.org/downloads/)
    
    B. Docker and Docker Compose: [Install Here](https://docs.docker.com/engine/install/ubuntu/)
    
    C. Google's Credential Service Account used for GCS and Bigquery access: [Get Here](https://developers.google.com/workspace/guides/create-credentials)
    
    D. Kafka: [Install Here](https://kafka.apache.org/quickstart)
    
    E. Airflow : [Install Here](https://airflow.apache.org/docs/apache-airflow/stable/installation/index.html)
    
### Usage Guideline 
![Data Pipeline Graph-](https://user-images.githubusercontent.com/98518827/229811511-20bb9efa-b022-4680-81d9-63ff0a09d707.png)

End-to-end data pipeline is the process of collecting data from multiple sources, cleaning, processing, and entering it into a system that can be used for analysis.

Cloud Storage: Initial data will be stored in cloud storage. Here, data can be stored in various formats such as CSV, JSON, or parquet.

Apache Kafka: After the data is stored in cloud storage, Apache Kafka will be used as a third party in the data pipeline process. Kafka is an open source data streaming platform that enables real-time or streaming data delivery. Data can be moved from cloud storage to Kafka using Kafka Connect.

BigQuery: Data that is in Kafka will be entered into BigQuery. BigQuery is a cloud data warehouse that enables large-scale data processing and data analysis across an organization. In BigQuery, data can be managed and stored in structured tables and then used for analysis.

Looker Studio: Once data is entered into BigQuery, Looker Studio will be used as a platform to visualize and analyze data. Looker Studio is a Business Intelligence (BI) platform that enables users to create reports and dashboards, perform data analysis, and share information with teams in real-time.

## Data Visualization Dashboard

Access the dashboard [here](https://lookerstudio.google.com/u/0/reporting/fd89ad24-b3a1-433e-8008-30bb3fd8ac46/page/zGfKD)


![Fraud Monitoring Dashboard](https://user-images.githubusercontent.com/98518827/230102256-57069407-0b4d-4743-8a5d-719eea536087.png)
   
## Kelompok 4 DF 9 
- Chandra Parashian Napitupulu
- Hadi Nur Salam
- Mukhamad Azis Tholib
- Reza Septian Kamajaya

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
