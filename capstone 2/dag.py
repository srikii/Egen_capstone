
import datetime
import airflow
from airflow import DAG
from airflow import models 
from airflow.operators.python import BranchPythonOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils import trigger_rule
from google.cloud import bigquery
from google.cloud import storage
import pandas as pd


DAG_NAME= "bitcoin_dag"
default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=1),
    'email_on_failure' : False,
    'email_on_retry': False,
    'retry_delay':datetime.timedelta(minutes=2)
    }

dag= DAG(
    dag_id="bitcoin_dag",
    default_args=default_args,
    schedule_interval='0 0 * * *',#daily at midnight
    catchup=False,#does not run multiple times from when it missed 
    description="bitcoin_dag",
    max_active_runs=5,
)

def get_combined_data():
    
    storage_client=storage.Client()
    bucket = storage_client.bucket("proj_data_bucket")
    

    blobs=bucket.list_blobs()
    combined_df=pd.DataFrame()

    #download all files from bucket to combine
    for blob in blobs:
        filename = blob.name.replace('-','_')
        print(f"downloading {filename}")


        #downnload to data
        blob.download_to_filename(f'/home/airfow/gcs/data/{filename}')
        print(f"done getting{filename}")

        #concat files
        file_df=pd.read_csv(f'/home/airfow/gcs/data/{filename}')
        combined_df=combined_df.append(file_df)
        print("success concat")
        
        #delete original files from source bucket
        blob.delete()
    

    if len(combined_df)>0:
        combined_file="combined_file.csv"
        combined_df.to_csv(f"/home/airflow/gcs/data/{combined_file}",index=False)
        return 'clean_data'
    else:
        print("no records found")
        return("end")


def clean_data():
    
    df=pd.read_csv('/home/airflow/gcs/data/combined_file.csv')
    df=df.fillna("").astype(str)
    
    #get bitcoin price and date for timeseries analysis
    df=df[df["id"]=='BTC']
    df = df.loc[:, df.columns.intersection(['id','price', 'price_timestamp'])]
    
    df.to_csv("/home/airflow/gcs/data/cleaned_records.csv")



def upload_bigquery():
    Client=bigquery.Client()

    table_id="proj-data:cryptodata.crypto_table"
    destination_table=Client.get_table(table_id)

    current_rows= destination_table.num_rows
    print(current_rows," number of rows in table before")

    job_config=bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True
    )

    #upload location
    uri=f"gs://us-central1-cryptodata-comp-3c4a187b-bucket/data/combined_file.csv"

    #request to upload
    load_job=client.load_table_from_uri(url,table_id,job_config=job_config)
    load_job.result()

    destination_table=Client.get_table(table_id)
    rows_after_insert=destination_table.num_rows
    print(f"rows after inserting= {rows_after_insert} ")
    

start=DummyOperator(task_id="start",dag=dag)
end=DummyOperator(task_id="end",dag=dag)

get_combined_data=BranchPythonOperator(
    task_id="get_combined_data_task",
    python_callable=get_combined_data ,
    dag=dag
)

clean_data = PythonOperator(
    task_id="clean_data",
    python_callable=clean_data,
    dag=dag
)

upload_bigquery= PythonOperator(
    task_id="upload_bigquerty",
    python_callable=upload_bigquery,
    dag=dag
)

start >> get_combined_data >> clean_data >> upload_bigquery >> end 
get_combined_data >> end
