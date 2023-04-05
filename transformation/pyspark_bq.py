from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import os
##Configure SPARK

spark = SparkSession.builder\
    .master('local[1]')\
    .appName('transform_bq')\
    .getOrCreate()


##Input the Credential
os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="skilful-scarab-339408-9477d1b628fd.json"
spark.conf.set('temporaryGcsBucket', 'temp_bucket')

##Creating DataFrames
df_fraud=spark.read \
        .format('bigquery')\
        .load('skilful-scarab-339408.iykra_project_new.fraud_online')
df_fraud.createOrReplaceTempView('fraudonline') 

table_fraud=spark.sql("""
          WITH temp AS(SELECT DISTINCT *
          FROM fraudonline)
          
          SELECT TIMESTAMPADD(HOUR,step,TIMESTAMP'2023-03-01 00:00:00') AS Tanggal,
          type,
          amount,
          nameOrig,
          oldbalanceOrg,
          newbalanceOrig,
          nameDest,
          oldbalanceDest,
          newbalanceDest,
          CASE WHEN isFraud = 0 THEN 'Not Fraud'
          ELSE 'Fraud'
          END AS isFraud,
          CASE WHEN isFlaggedFraud = 0 THEN 'Not Fraud'
          ELSE 'Fraud'
          END AS isFlaggedFraud
          FROM temp
                    """)

table_fraud.write \
  .format("bigquery") \
  .option("writeMethod", "direct") \
  .save("iykra_project_new.data_fraud_transform")


